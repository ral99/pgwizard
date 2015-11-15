from setuptools import setup

setup(
    name='pgwizard',
    version='0.1',
    packages=['pgwizard'],
    package_dir={'': 'src'},
    install_requires=['psycopg2==2.6.1'],
    author='Rodrigo A. Lima',
    description='Manage connections to PostgreSQL databases.',
    license='BSD',
    keywords='postgre database',
)
