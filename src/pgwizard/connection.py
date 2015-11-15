import psycopg2
import random
import time

class PostgreSQLConnection:
    """Connection to a PostgreSQL database."""

    def __init__(self, database, host, port, user, password, accept_writes, accept_reads, autocommit,
                 max_connection_age_in_seconds):
        """Set connection attributes and connect to the database."""
        self._database = database
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._accept_writes = accept_writes
        self._accept_reads = accept_reads
        self._autocommit = autocommit
        self._max_connection_age_in_seconds = max_connection_age_in_seconds
        self.connect()

    def connect(self):
        """Set a connection to the database."""
        try:
            self._connection = psycopg2.connect(database=self._database, host=self._host, port=self._port,
                                                user=self._user, password=self._password)
            self._connection.autocommit = self._autocommit
            self._close_at = None if self._max_connection_age_in_seconds is None \
                             else time.time() + self._max_connection_age_in_seconds
        except:
            self._connection = None
            self._close_at = None
        self._cursor = None

    def is_usable(self):
        """Return True if the connection is usable. False, otherwise."""
        try:
            self._connection.cursor().execute("SELECT 1")
        except:
            return False
        else:
            return True

    def has_expired(self):
        """Return True if the connection has expired. False, otherwise."""
        return time.time() > self._close_at if self._close_at is not None else False

    def open_cursor(self):
        """Open a new cursor."""
        self._cursor = self._connection.cursor()

    def close_cursor(self):
        """Close the cursor now."""
        self._cursor.close()
        self._cursor = None

    def is_connected(self):
        """Return True if there is a connection to the database. False, otherwise."""
        return self._connection is not None

    def has_cursor(self):
        """Return True if the connection has an opened cursor. False, otherwise."""
        return self._cursor is not None

    def is_autocommit(self):
        """Return True if autocommit mode is on. False, otherwise."""
        return self._autocommit

    def execute(self, operation, parameters=None):
        """Execute an operation on the database."""
        self._cursor.execute(operation, parameters)

    def fetch_one(self):
        """Fetch the next row of a query result, returning a single tuple, or None when no more data is available."""
        return self._cursor.fetchone()

    def fetch_many(self, n):
        """Fetch the next rows of a query result, returning a list of tuples."""
        return self._cursor.fetchmany(n)

    def fetch_all(self):
        """Fetch all (remaining) rows of a query result, returning a list of tuples."""
        return self._cursor.fetchall()

    def commit(self):
        """Commit any pending transaction to the database."""
        self._connection.commit()

    def rollback(self):
        """Roll back to the start of any pending transaction."""
        self._connection.rollback()

class PostgreSQLConnectionPool:
    """Pool of connections to PostgreSQL databases."""

    def __init__(self):
        """Set the dictionary that maps names to a list of connections."""
        self._connections = {}

    def add_connection(self, name, connection):
        """Add a connection to the pool."""
        if name not in self._connections:
            self._connections[name] = []
        self._connections[name].append(connection)

    def refresh_connections(self):
        """Refresh connections of the pool."""
        for connections in self._connections.values():
            for connection in connections:
                if not connection.is_usable() or connection.has_expired():
                    connection.connect()

    def open_cursors(self):
        """Open cursors for connections of the pool."""
        for connections in self._connections.values():
            for connection in connections:
                if connection.is_connected():
                    connection.open_cursor()

    def close_cursors(self):
        """Close opened cursors for connections of the pool."""
        for connections in self._connections.values():
            for connection in connections:
                if connection.has_cursor():
                    connection.close_cursor()

    def get_connection_for_writing_to(self, name):
        """Return a connection identified by name for writing."""
        return random.choice([connection for connection in self._connections[name] if connection._accept_writes and
                              connection.has_cursor()])

    def get_connection_for_reading_from(self, name):
        """Return a connection identified by name for reading."""
        return random.choice([connection for connection in self._connections[name] if connection._accept_reads and
                              connection.has_cursor()])
