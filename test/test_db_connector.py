import pytest
from db_connector import DBConnector, MySQLConnector, OracleConnector, PostgreSQLConnector, MSSQLConnector, IBMDB2Connector, oracle_connect
from unittest.mock import MagicMock, patch

class TestDBConnector:

    # @patch("db_connector.oracledb.connect")
    def test_oracle_connector_enter_success(self):
        with patch('db_connector.oracle_connect') as mock_connect:
            mock_connect.return_value = MagicMock()
            
            with OracleConnector(host='host', port=1521, usr='user', pwd='pass', sid='orcl', tech='oracle') as ctx:
                mock_connect.assert_called_once_with(
                    host='host',
                    port=1521,
                    user='user',
                    password='pass',
                    sid='orcl'
                )

    def test_mysql_connector_enter_success(self):
        with patch('db_connector.mysql_connect.connect') as mock_connect:
            mock_connect.return_value = MagicMock()
            
            with MySQLConnector(host='host', port=3306, usr='user', pwd='pass', sid='db', tech='mysql') as ctx:
                mock_connect.assert_called_once_with(
                    host='host',
                    port=3306,
                    user='user',
                    password='pass',
                    database='db'
                )

    def test_postgres_connector_enter_success(self):
        with patch('db_connector.psycopg_connect') as mock_connect:
            mock_connect.return_value = MagicMock()

            with PostgreSQLConnector(host='host', port=5432, usr='user', pwd='pass', sid='db', tech='postgresql') as ctx:
                mock_connect.assert_called_once_with(
                    user='user',
                    password='pass',
                    host='host',
                    port=5432,
                    dbname='db'
                )

    def test_mssql_connector_enter_success(self):
        with patch('db_connector.pymssql_connect') as mock_connect:
            mock_connect.return_value = MagicMock()
            
            with MSSQLConnector(host='host', port=1433, usr='user', pwd='pass', sid='db', tech='mssql') as ctx:
                mock_connect.assert_called_once_with(
                    server='host',
                    user='user',
                    password='pass',
                    database='db',
                    port=1433
                )

    def test_ibmdb2_connector_enter_success(self):
        with patch('db_connector.ibmdb_connect') as mock_connect, \
             patch('db_connector.ibmdb_dbi_connection') as mock_dbi:
            mock_connect.return_value = MagicMock()
            mock_dbi.return_value = MagicMock()
            
            with IBMDB2Connector(host='host', port=50000, usr='user', pwd='pass', sid='db', tech='ibmdb2') as ctx:
                expected_conn_string = 'DATABASE=db;HOSTNAME=host;PORT=50000;PROTOCOL=TCPIP;UID=user;PWD=pass;'
                mock_connect.assert_called_once_with(expected_conn_string, '', '')

    def test_connector_enter_connection_failure(self):
        with patch('oracledb.connect') as mock_connect:
            connector = OracleConnector(host='host', port=1521, usr='user', pwd='pass', sid='orcl', tech='oracle')
            mock_connect.side_effect = Exception("Connection failed")
            
            with pytest.raises(Exception) as exc_info:
                with connector:
                    pass
            assert "Connection to the target database failed" in str(exc_info.value)

    def test_get_technology_valid(self):
        tech = 'mysql'
        connector = DBConnector.get_technology(tech)
        assert connector == MySQLConnector

    def test_get_technology_invalid(self):
        tech = 'invalid_tech'
        with pytest.raises(KeyError) as exc_info:
            DBConnector.get_technology(tech)
        assert "invalid_tech" in str(exc_info.value)

    def test_get_technology_case_insensitivity(self):
        tech = 'PostgreSQL'
        connector = DBConnector.get_technology(tech.lower())
        assert connector == PostgreSQLConnector

    def test_get_technology_empty_string(self):
        tech = ''
        with pytest.raises(KeyError) as exc_info:
            DBConnector.get_technology(tech)
        assert "" in str(exc_info.value)

    def test_get_technology_none_input(self):
        tech = None
        with pytest.raises(TypeError) as exc_info:
            DBConnector.get_technology(tech)
        assert "NoneType" in str(exc_info.value)


if __name__ == "__main__":
    test_obj = TestDBConnector()
    test_obj.test_oracle_connector_enter_success()
    test_obj.test_postgres_connector_enter_success()