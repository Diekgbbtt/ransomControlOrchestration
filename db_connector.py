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

from oracledb import connect as oracle_connect
from psycopg2 import connect as psycopg_connect
from mysql.connector.connection import MySQLConnection as mysql_connect
from pymssql import connect as pymssql_connect
from ibm_db import connect as ibmdb_connect
from ibm_db_dbi import Connection as ibmdb_dbi_connection
from abc import ABC, abstractmethod
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(asctime)s - %(message)s")


NUM_LINES = 1000

class DBConnector(ABC):

    def __init__(self, host: str, port: int, sid: str, tech: str, usr: str, pwd: str):
        self.hostname_or_ip = host
        self.port = port
        self.username = usr
        self.password = pwd
        self.sid = sid
        self.tech = tech
        self.connection = None
        self.cursor = None
    
    
    def __enter__(self):
        try:
            self.connection = self.connect()
            self.cursor = self.connection.cursor()
        except Exception as e: # all connection implementation adhere to PEP249, which defines an error as base exception class, inheriting from Exception
            logging.error(e, exc_info=True)
            raise Exception(f"Connection to the target database failed \n {e} \n {type(e)}") from e
    def __exit__(self, *kwargs):
        self.connection.close()
    
    @abstractmethod
    def connect(self):
        raise NotImplemented

    def execute_query(self, query: str, all: bool = False, fetchsize: int = None) -> object:
        """
        Lancia la query definita dalla variabile sql_string.
        :return: Risultato sottoforma di generator
        :param: many: se deve essere effettuata una fetchmany()
        :param: se si ha bisogno di una lista delle colonne ritornera' anche quella (to be deprecated)
        """
        try:
            self.cursor.execute(query)
            if all:
                return self.cursor.fetchall()
            elif fetchsize and fetchsize > 1:
                results = self.cursor.fetchmany(fetchsize)
                while results:
                    for r in results:
                        yield r
                    results = self.cursor.fetchmany(NUM_LINES)
            else:
                r = self.cursor.fetchone()
                yield r
        except Exception as e:
            logging.error(e, exc_info=True)
        finally:
            self.cursor.close()

    def get_technology(tech: str) :
        
        if isinstance(tech, str):
           tech = tech.lower().strip()
        else:
            raise TypeError(f"Invalid type for technology: {type(tech)}")

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
        connection = mysql_connect.connect(
            user=self.username,        
            password=self.password, 
            host=self.hostname_or_ip, 
            port=self.port, 
            database=self.sid
        )
        return connection



class PostgreSQLConnector(DBConnector):

    def connect(self):
        connection = psycopg_connect(user=self.username,password=self.password,host=self.hostname_or_ip,port=self.port,dbname=self.sid)
        return connection

class MSSQLConnector(DBConnector):

    def connect(self):
        connection = pymssql_connect(server=self.hostname_or_ip, user=self.username, password=self.password,
                                     database=self.sid, port=self.port)
        return connection


class IBMDB2Connector(DBConnector):

    def connect(self):
        conn_string = f'DATABASE={self.sid};HOSTNAME={self.hostname_or_ip};PORT={self.port};PROTOCOL=TCPIP;UID={self.username};PWD={self.password};'
        connection = ibmdb_connect(conn_string, '', '')
        connection = ibmdb_dbi_connection(connection)
        return connection
