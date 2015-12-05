import psycopg2
import random
import datetime

class PGWizardCursor:
    """Cursor for a PostgreSQL database connection."""

    def __init__(self, connection):
        """Open the cursor."""
        self._cursor = connection.cursor()

    def __del__(self):
        """Close the cursor when destructed."""
        self._cursor.close()

    def __enter__(self):
        """Return itself when called from 'with' statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the cursor when called from 'with' statement."""
        self._cursor.close()

    def close(self):
        """Close the cursor."""
        self._cursor.close()

    def execute(self, operation, parameters=None):
        """Execute an operation."""
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

class PGWizardConnection:
    """Connection to a PostgreSQL database."""

    def __init__(self, connection):
        """Set connection attribute."""
        self._connection = connection

    def open_cursor(self):
        """Return a cursor for the connection."""
        return PGWizardCursor(self._connection)

class PGWizardTransactionalConnection(PGWizardConnection):
    """Transactional connection to a PostgreSQL database."""

    def commit(self):
        """Commit any pending transaction to the database."""
        self._connection.commit()

    def rollback(self):
        """Roll back to the start of any pending transaction."""
        self._connection.rollback()

class PGWizardConnectionPool:
    """Pool of connections to PostgreSQL databases."""

    def __init__(self, connection_test_interval_in_seconds):
        """Initialize the containers for connections and set attributes."""
        self._connection_test_interval = datetime.timedelta(seconds=connection_test_interval_in_seconds)
        self._master = {}
        self._slave = {}

    def __del__(self):
        """Close the connections when destructed."""
        for master in self._master.values():
            if master['connection']:
                master['connection'].close()
            if master['transactional_connection']:
                master['transactional_connection'].close()
        for slaves in self._slave.values():
            for slave in slaves:
                if slave['connection']:
                    slave['connection'].close()
                if slave['transactional_connection']:
                    slave['transactional_connection'].close()

    def set_master_database_server(self, name, database, host, port, user, password):
        """Set a master database server connection."""
        self._master[name] = {
                'database': database,
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'connection': None,
                'transactional_connection': None,
                'connection_test_expiration_time': None,
                'transactional_connection_test_expiration_time': None
        }

    def add_slave_database_server(self, name, database, host, port, user, password):
        """Add a slave database server connection."""
        if name not in self._slave:
            self._slave[name] = []
        self._slave[name].append({
            'database': database,
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'connection': None,
            'transactional_connection': None,
            'connection_test_expiration_time': None,
            'transactional_connection_test_expiration_time': None
        })

    def get_master_connection(self, name):
        """Return the master database server connection."""
        master = self._master[name]
        now = datetime.datetime.now()
        expiration_time = master['connection_test_expiration_time']
        if expiration_time is None or expiration_time < now:
            master['connection_test_expiration_time'] = now + self._connection_test_interval
            try:
                connection = master['connection']
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            except:
                connection = psycopg2.connect(database=master['database'], host=master['host'], port=master['port'],
                                              user=master['user'], password=master['password'])
                connection.autocommit = True
                master['connection'] = connection
        return PGWizardConnection(master['connection'])

    def get_slave_connection(self, name):
        """Return a random slave database server connection."""
        slave = random.choice(self._slave[name])
        now = datetime.datetime.now()
        expiration_time = slave['connection_test_expiration_time']
        if expiration_time is None or expiration_time < now:
            slave['connection_test_expiration_time'] = now + self._connection_test_interval
            try:
                connection = slave['connection']
                cursor = connection.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            except:
                connection = psycopg2.connect(database=slave['database'], host=slave['host'], port=slave['port'],
                                              user=slave['user'], password=slave['password'])
                connection.autocommit = True
                slave['connection'] = connection
        return PGWizardConnection(slave['connection'])

    def get_master_transactional_connection(self, name):
        """Return the master database server transactional connection."""
        master = self._master[name]
        now = datetime.datetime.now()
        expiration_time = master['transactional_connection_test_expiration_time']
        if expiration_time is None or expiration_time < now:
            master['transactional_connection_test_expiration_time'] = now + self._connection_test_interval
            try:
                transactional_connection = master['transactional_connection']
                cursor = transactional_connection.cursor()
                cursor.execute("SELECT 1")
                transactional_connection.commit()
                cursor.close()
            except:
                transactional_connection = psycopg2.connect(database=master['database'], host=master['host'],
                                                            port=master['port'], user=master['user'],
                                                            password=master['password'])
                transactional_connection.autocommit = False
                master['transactional_connection'] = transactional_connection
        return PGWizardTransactionalConnection(master['transactional_connection'])

    def get_slave_transactional_connection(self, name):
        """Return a random slave database server transactional connection."""
        slave = random.choice(self._slave[name])
        now = datetime.datetime.now()
        expiration_time = slave['transactional_connection_test_expiration_time']
        if expiration_time is None or expiration_time < now:
            slave['transactional_connection_test_expiration_time'] = now + self._connection_test_interval
            try:
                transactional_connection = slave['transactional_connection']
                cursor = transactional_connection.cursor()
                cursor.execute("SELECT 1")
                transactional_connection.commit()
                cursor.close()
            except:
                transactional_connection = psycopg2.connect(database=slave['database'], host=slave['host'],
                                                            port=slave['port'], user=slave['user'],
                                                            password=slave['password'])
                transactional_connection.autocommit = False
                slave['transactional_connection'] = transactional_connection
        return PGWizardTransactionalConnection(slave['transactional_connection'])
