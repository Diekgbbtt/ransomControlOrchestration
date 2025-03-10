"""
agnostic database connector
"""

"""
connector = DBConnector.get_tech()
connession = connector.connect(cfg_data...)
connession.cursor.execute(query)
"""
"""
connector = DBConnector.get_tech()
with connector(cfg_data...) as connession:
    
    connession.execute(query)

"""

from oracledb import defaults as oracle_defaults, Error as oracle_error, connect as oracle_connect, Connection, Cursor, OperationalError, DatabaseError as OracleDatabaseError
from psycopg import Connection as psycopg_connection
from mysql import connector as mysqlconnector
from pymssql import Connection as pymssql_connection
from ibm_db import connect as ibmdb_connect
from ibm_db_dbi import Connection as ibmdb_dbi_connection
from abc import ABC, abstractmethod
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s - %(message)s")


NUM_LINES = 1000


class DBConnector(ABC):

    def __init__(self, hostname_or_ip: str, port: int, username: str, password: str, sid: str):
        self.hostname_or_ip = hostname_or_ip
        self.port = port
        self.username = username
        self.password = password
        self.sid = sid
        self.connection = None
        self.cursor = None
    
    
    def __enter__(self):
        try:
            self.connection = self.connect()
            self.cursor = self.connection.cursor()
        except Exception as e: # all connection implementation adhere to PEP249, which defines an error as base exception class, inheriting from Exception
            logging.error(e, exc_info=True)

    def __exit__(self):
        self.connection.close()
    
    @abstractmethod
    def connect(self):
        raise NotImplemented

    def execute_query(self, query: str, many: bool):
        """
        Lancia la query definita dalla variabile sql_string.
        :return: Risultato sottoforma di generator
        :param: many: se deve essere effettuata una fetchmany()
        :param: se si ha bisogno di una lista delle colonne ritornera' anche quella (to be deprecated)
        """
        try:
            columns = list()
            self.cursor.execute(query)
            columns = [col[0].lower() for col in self.cursor.description]
            if many:
                results = self.cursor.fetchmany(NUM_LINES)
                while results:
                    for r in results:
                        yield r, columns
                    results = self.cursor.fetchmany(NUM_LINES)
            else:
                r = self.cursor.fetchone()
                yield r
        except Exception as e:
            logging.error(e, exc_info=True)
        finally:
            self.cursor.close()

    def get_technology(tech: str):
        
        technologies = {
            'oracle': OracleConnector,
            'mysql': MySQLConnector,
            'postgresql': PostgreSQLConnector,
            'mssql': MSSQLConnector,
            'ibmdb2': IBMDB2Connector
        }
        return technologies[tech]

class OracleConnector(DBConnector):

    def connect(self):
            connection = oracle_connect(
                host=self.hostname_or_ip,
                port=self.port,
                user=self.username,
                password=self.password,
                sid=self.sid,
            )
            return connection


class MySQLConnector(DBConnector):

    def connect(self):
        connection = mysqlconnector.connect(
            user=self.username,        
            password=self.password, 
            host=self.host, 
            port=self.port, 
            database=self.sid
        )
        return connection



class PostgreSQLConnector(DBConnector):

    def connect(self):
        connection = psycopg_connection.connect(fr"postgresql://{self.username}:{self.password}@{self.hostname_or_ip}:{self.port}/{self.sid}"),
        return connection

class MSSQLConnector(DBConnector):

    def connect(self):
        connection = pymssql_connection.connect(server=self.host, user=self.username, password=self.password,
                                     database=self.database_name, port=self.port)
        return connection


class IBMDB2Connector(DBConnector):

    def connect(self):
        conn_string = f'DATABASE={self.sid};HOSTNAME={self.host};PORT={self.port};PROTOCOL=TCPIP;UID={self.username};PWD={self.password};'
        connection = ibmdb_connect(conn_string, '', '')
        connection = ibmdb_dbi_connection(connection)
        return connection
