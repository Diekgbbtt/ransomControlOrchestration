""""
author : diego gobbetti
version : 0.0.1
email : d.gobbetti@nsr.it
"""

"""
TO DO:
    -   handle worst case scenarios
        -X valutare dimensione report e chuking se supera limite (dimensione email - x MB) -> invio di più mail, con ogni mail che indichi solo discrepanza rilevata alla tabella x colonna y
        -X creare livello persistente semplice con sqlLite che fornisca un log dei report e il loro stato di invio, in modo tale che se la mail non venga inviata, si può agire manualemente
        - timing with completion bars
        -X costrutti try-catch
        - testing
        - piccolo portale?
    
    -   
        
"""

"""
        Strutture Dati tabelle reports e discrepancies
    
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_name VARCHAR(255) NOT NULL,
        processing_status VARCHAR(255) NOT NULL,
        creation_timestamp DATE,
        data BLOB


        id INTEGER PRIMARY KEY AUTOINCREMENT,
        db VARCHAR(255)
        table VARCHAR(255),
        column VARCHAR(255),
        discrepanciy VARCHAR(255),
        FOREIGN KEY (report_id) REFERENCES reports (id)
"""



# third party modules
from requests import packages, Session, RequestException
from smtplib import SMTP, SMTPException
from oracledb import defaults as oracle_defaults, Error as oracle_error, connect as oracle_connect, Connection, Cursor, OperationalError, DatabaseError
from email.message import EmailMessage
from email.errors import *

# standard library modules
from abc import ABC, abstractmethod
from typing import Dict, Optional, Union, Any, List
from warnings import simplefilter
from urllib3 import disable_warnings, exceptions
from os import remove, chdir, makedirs, path, write, close, open, O_CREAT, O_WRONLY
from math import ceil
from shutil import copy
import time
from dateutil import parser # type: ignore
from datetime import datetime
from zipfile import ZipFile, error as zip_error
from sqlite3 import connect, Connection, Error
from utils import *


disable_warnings(exceptions.InsecureRequestWarning)  # needed to suppress warnings
simplefilter('ignore')
packages.urllib3.disable_warnings()
oracle_defaults.fetch_lobs = False


""""

*****************************************************************************************************************
*   astrazione del controllo con design pattern factory method, nell'ottica di effettuare controli              *
*   di diverso tipo utilizando sempre delphix come engine                                                      *
*****************************************************************************************************************

A PRIORI IPOTIZZO UNA QUERY A CATALOGO CON DB,TB, CLN, CONTROLLI E RELATIVI DATI NECESSARI PER IL CONTROLLO

Invece di file di configurazione harvest iniziale dei controlli con query e inizializzazione:

for(row : results)
    controlName = row.getcolumn(x)
    controlData = row.getmultiplecolumns.... + data elaborations needed
    factory = controlFactory(controlName)
    controlDatabase(factory.instance_control(controlData))

"""

class ControlClass(ABC):

    @abstractmethod
    def __init__(self, data: dict) -> None:
        return

    @abstractmethod
    def start(self) -> None:
        return

    @abstractmethod
    def finish(self) -> None:
        return



class controlFactory:

    def __init__(self, controlName : str):
        self.control = controlName
    
    def instance_control(self, control_data : dict, cfg_data : dict) -> None:
        try:
            match self.control:
                case 'ransomCheck':
                    return ransomCheck(control_data=control_data, cfg_data=cfg_data)
                case 'anotherCheck':
                    pass
                case _: # Default case to handle any unmatched control type
                    raise Exception(msg=f"Control {self.control} not supported")
        except:
            raise Exception(msg=f"Error creating control instance {self.control}. \n Error : {e.msg if e.hasattr('msg') else e}")
    
class ransomCheck(controlClass):

    def __init__(self, control_data: dict, cfg_data: dict) -> None:

        for key, val in control_data.items():
            setattr(self, key, val)
        try:
            self.initialize_objs(cfg_data=cfg_data)
        except Exception as e:
            raise Exception(msg=(e.msg if e.hasattr('msg') else f"Error initializing engines. \n Error : {e}"))

    def initialize_objs(self, cfg_data: dict) -> None:

        try:
            for key, val in cfg_data.get('dpx_engines').get('source_engines').items():
                    
                if key == self.sourceEngineRef:
                        self.source_engine = DelphixEngine(decrypt_value(val['host']))
                        self.source_engine.create_session(val['apiVersion'])
                        self.source_engine.login_data(decrypt_value(val['usr']), decrypt_value(val['pwd']))

            for key, val in cfg_data.get('dpx_engines').get('vault_engines').items():
                    
                if key == self.vaultEngineRef:
                        self.vault_engine = DelphixEngine(decrypt_value(val['host']))
                        self.vault_engine.create_session(val['apiVersion'])
                        self.vault_engine.login_data(decrypt_value(val['usr']), decrypt_value(val['pwd']))
            
            for key, val in cfg_data.get('dpx_engines').get('discovery_engines').items():
                if key == self.discEngineRef:
                        self.disc_engine = DelphixEngine(decrypt_value(val['host']))
                        self.disc_engine.login_compliance(decrypt_value(val['usr']), decrypt_value(val['pwd']))
        except Exception as e:
            raise Exception(msg=f"Error initializing engines. \n Error : {e.msg if e.hasattr('msg') else e}")

        self.vdb = object()    
        for key, val in cfg_data.get('vdbs_control').items():
            if key == self.vdbRef:
                    for _key, _val in val.items():
                        setattr(self.vdb, _key, _val)
        
        self.mail = object()
        for key, val in cfg_data.get('mail').items():
            setattr(self.mail, key, val)

    def start(self) -> None:

        try:
            """
            engine_1 = DelphixEngine(self.data.get('dpx_engines').get('source_engines').get(self.sourceEngineRef)['host'])
            engine_1.create_session(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['apiVersion'])
            engine_1.login_data(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['pwd'])
            engine_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['host'])
            engine_13.create_session(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['apiVersion'])
            engine_13.login_data(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['pwd'])
            engineCompl_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['host'])
            engineCompl_13.login_compliance(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['pwd'])
            """

            self.source_engine.replication(self.replicationSpec)
            self.vault_engine.refresh(self.vdbContainerRef, self.dSourceContainer_ref)

            self.disc_engine.mask(self.jobId)

            self.evaluate()

            if(self.discrepant_values):
                
                self.connect_db(file_name='reports.db')
                self.register_reports()
                self.create_report() # zip the new discrepancies file
                self.send_alert()
                self.backup_report()
        
        except Exception as e:
            raise Exception(msg=f"Error executing control. \n Error : {(e.msg if e.hasattr('msg') else e)}")

    def finish(self) -> None:

        return

    """
        Creates a report file and zips it, returning the path to the zipped report.
        
        Args:
            report_file_name (str): The name of the report file to create.
            db_conn (sqlite3.Connection): The database connection to use for updating the report status.
        
        Returns:
            str: The path to the zipped report file, or None if an error occurred.
    """


    def connect_db(self, file_name: str = 'reports.db') -> None:
        try:
            self.db_conn = connect(file_name)
        except OperationalError or oracle_error as e:
            raise Exception(msg=f"Errror connecting to vault database. \n Error : {e}")
  
    def create_report(self) -> Union[str, None]:

        try:
            if not path.exists("Evaluation"):
                makedirs("Evaluation")
            chdir("Evaluation") 
            self.report_file_path = f"{self.report_file_name}.txt"
            flags = O_CREAT | O_WRONLY  # Create file if it doesn't exist, open for writing
            mode = 0o666  # Permissions for the file
            fd = open(self.report_file_path, flags, mode)
            for i in range((1, self.discrepant_values.rowcount())):
                row = self.discrepant_values[i]
                write(fd, f"discrepancy revealed in database {row[0]} table {row[1]} column {row[2]}, expected value is {row[4]} while the actual value is {row[3]} \n".encode())
            close(fd)
            self.zip_report()
        except OSError as e:
            raise Exception(msg=f"Error creating report: \n {(e.strerror if e.hasattr('strerror') else e)} \n error number:  {e.errno}")
        except Exception as e:
            raise Exception(msg=f"Error creating report: \n {(e.msg if e.hasattr('msg') else e)}")

    """
    Creates a zip file from the specified report file and removes the original report file.

    Args:
        report_path (str): The path to the report file to be zipped.
        db_conn (sqlite3.Connection): The database connection to use for updating the report status.

    Returns:
        str or list: The path to the zipped report file, or a list of chunked file paths if the zipped file exceeds the maximum size.
    """
    def zip_report(self) -> Union[str, list, None]:

        try:
            self.reports_zip_path = self.report_file_path.replace(".txt", ".zip")
            with ZipFile(self.reports_zip_path, "x") as zip:
                zip.write(self.report_file_path, compresslevel=9)
            remove(self.report_file_path)
            del self.report_file_path
            self.assess_dimensions()
        except Exception as e:
            raise Exception(f"Error zipping report: {e.msg if e.hasattr('msg') else e}")


    """
    Assesses the size of the zipped report file and determines if it needs to be split into smaller chunks.

    Args:
        zip_path (str): The path to the zipped report file.
        connector: The database connection to use for updating the report status.

    Returns:
        list: A list of file paths for the zipped report or its chunks.
    """
    def assess_dimensions(self) -> Union[List[str], None]:

        try:
            # Constants
            CHUNK_SIZE = 150 * 1024 * 1024  # 150MB in bytes
            MAX_SIZE = 300 * 1024 * 1024  # 300MB in bytes
            
            # Check the size of the file
            file_size = path.getsize(self.reports_zip_path)
            
            # If file is smaller than or equal to 300MB, no need to chunk
            if file_size <= MAX_SIZE:
                self.update_report_status(self.reports_zip_path[:-4], "REPORT ZIP CREATED")
                self.reports_zip_path = [self.reports_zip_path]  # Return original file path
            
            # Split into chunks if file is larger than 300MB
            file_parts = []
            with open(self.reports_zip_path, 'rb') as f:
                total_parts = ceil(file_size / CHUNK_SIZE)
                base_name, ext = path.splitext(self.reports_zip_path)
                
                for i in range(total_parts):
                    part_file_path = f"{base_name}_part_{i + 1}{ext}"
                    with open(part_file_path, 'wb') as chunk_file:
                        # does it create the fiole if it isn't already there?
                        chunk_data = f.read(CHUNK_SIZE)
                        if chunk_data:
                            chunk_file.write(chunk_data)
                            file_parts.append(part_file_path)
            self.update_report_status(self.reports_zip_path[:-4], "MULTIPLE REPORT ZIP CREATED")
            self.reports_zip_path = file_parts
        except OSError as e:
            raise Exception(msg=f"error opening file, reading or writing in chunks: {e} \n {e.strerror if e.hasattr('strerror') else e}")
        except Exception as e:
            raise Exception(msg=f"Error assessing dimensions: \n {e.msg if e.hasattr('msg') else e}")

    """
    Updates the processing status of a report in the database.

    Args:
        report_name (str): The name of the report to update.
        status (str): The new status to set for the report.
        db_conn (sqlite3.Connection): The database connection to use for the update.

    Returns:
        None
    """
    def update_report_status(self, report_name: str, status: str) -> None:

        try:
            cursor = self.db_conn.cursor()

            cursor.execute('''
                SELECT name FROM sqlite_master WHERE type='table';
                        ''')
            
            cursor.execute("""
                UPDATE reports
                    SET processing_status = :status 
                        WHERE report_name = :report_name
                        """, {
                                'status': status,
                                'report_name': report_name
                                })
            self.db_conn.commit()
            print(f"Report {report_name} status updated to {status}")

        except oracle_error as e:
            raise Exception(msg=f"Error updating report status: \n {e}")

    """
        Registers a report and its associated discrepancies in the database.

        This method inserts a new report entry into the reports table and 
        records any discrepancies associated with that report in the discrepancies table. 
        It handles database connection errors and ensures that changes are committed or rolled back as necessary.

        Args:
            discrepant_values (list): A list of discrepancies to be recorded, where each discrepancy is expected to be a list or tuple containing relevant data.
            report_file_name (str): The name of the report being registered.
            db_conn (sqlite3.Connection): The database connection to use for executing SQL commands.

        Returns:
            None
    """
    def register_reports(self) -> None:
        
        self.report_file_name = f"discrepancies_{datetime.now().strftime('%d_%m_%Y-%H_%M_%S')}"
        try:
            cursor = self.db_conn.cursor()
        except DatabaseError or oracle_error as e:
            if self.db_conn:
                self.db_conn.rollback()
                # reverts any uncommitted changes made in the current transaction. 
                # This restores the database to its previous state before those changes were applied
            try:
                self.connect_db()
                self.register_reports()
            except oracle_error as e:
                raise Exception(msg=(e.msg if e.hasattr('msg') else f"Error getting cursor from db connection: {str(e)}"))

        try:
            processing_status = "REPORT TO BE GENERATED"
        
            # Create the reports table if it doesn't exist
            cursor.execute('''
            INSERT INTO reports (report_name, processing_status, created_at)
            VALUES (:report_name, :processing_status, :created_at)
            ''', {
                'report_name': self.report_file_name,
                'processing_status': processing_status,
                'created_at': datetime.now().strftime('%d_%m_%Y-%H_%M_%S'),
            })

            report_id = cursor.lastrowid

            for row in self.discrepant_values:
                cursor.execute('''
                INSERT INTO discrepancies (db, "table", "column", value, discrepancy, report_id)
                VALUES (:db, :table, :column, :value, :discrepancy, :report_id)
                ''', {
                    'db': row[0],
                    'table': row[1],
                    'column': row[2],
                    'value': row[3],
                    'discrepancy': f'effettivo {row[3]} != atteso {row[4]}',
                    'report_id': report_id
                })

            self.db_conn.commit()
        except oracle_error as e:
            if self.db_conn:
                self.db_conn.rollback()
            try:
                self.connect_db()
                self.register_reports()
            except oracle_error as e:
                raise Exception(msg=(e.msg if e.hasattr('msg') else f"Error registering reports: {str(e)}"))
            


    """
    Backs up the zipped report files to a designated backup directory.

    This method checks if the backup directory exists, creates it if it doesn't, 
    and then copies the provided report zip files to this directory. 
    It also updates the status of each report in the database to indicate that 
    it has been backed up.

    Args:
        reports_zip_path (list): A list of paths to the zipped report files to be backed up.
        db_conn (sqlite3.Connection): The database connection to use for updating the report status.

    Returns:
        None
    """
    def backup_report(self) -> None:
        try:
            # Create the reports backup directory if it doesn't exist
            if not path.exists("Evaluation/backup"):
                makedirs("Evaluation/backup")
            
            # Add the file to the backup directory
            for report_zip_path in self.reports_zip_path:
                copy(report_zip_path, "Evaluation/backup")
                if len(self.reports_zip_path) == 1:
                    self.update_report_status(report_zip_path[:-4], "REPORT ZIP BACKED UP")
                elif len(self.reports_zip_path) > 1:
                    self.update_report_status(report_zip_path[:-4], f"REPORT ZIP PART {report_zip_path.split('_')[2].strip()[:-4]} BACKED UP")
            
            self.db_conn.close()
        except OSError as e:
            raise Exception(msg=f"Error copying or creating directory, while backing up report: \n {e.strerror if e.hasattr('strerror') else e}")
        except Exception as e:
            raise Exception(msg=f"Error backing up report: {e.msg if e.hasattr('msg') else e}")

    """
        Sends an email alert with the specified report attachments.

        This method establishes an SMTP connection to the specified domain, 
        logs in using the provided credentials, and sends an email containing 
        the specified report files as attachments. It updates the status of 
        each report in the database after sending.

        Args:
            domain (str): The SMTP server domain.
            username (str): The username for SMTP authentication.
            pwd (str): The password for SMTP authentication.
            reports_path (list): A list of paths to the report files to be attached.
            sender (str): The email address of the sender.
            receivers (List[str]): A list of email addresses to send the alert to.
            connector: The database connection to use for updating the report status.

        Returns:
            None
    """

    def send_alert(self) -> None:
        try:
            with SMTP(decrypt_value(self.mail.smtpServer)) as mail_server:
                # Start TLS for security
                mail_server.starttls()
                mail_server.login(decrypt_value(self.mail.usr), decrypt_value(self.mail.pwd))
                
                for report in self.reports_zip_path: 
                    content = self.add_content(report)  # Prepare the email content with the report attachment
                    mail_server.send_message(content, decrypt_value(self.mail.usr), self.mailReceivers)  # Send the email
                    
                    # Update the report status in the database
                    if len(self.reports_zip_path) == 1:
                        self.update_report_status(report[:-4], f"REPORT SENT")
                    elif len(self.reports_zip_path) > 1:
                        self.update_report_status(report[:-4], f"PART {report.split('_')[2].strip()[:-4]} OF REPORT SENT")

        except SMTPException as e:
            raise Exception(msg=f"Error sending email: {(e.strerror if e.hasattr('strerror') else e)}") 
        except MessageError or MessageDefect as e:
            raise Exception(msg=f"Error sending email: {e.msg}")
        except Exception as e:
            raise Exception(msg=f"Error sending email: {e.msg if e.hasattr('msg') else e}")
    """
    Prepares the email content for the alert, including the report attachment.

    This method creates an email message indicating a potential ransomware attack, 
    includes the current date and time, and attaches the specified report file.

    Args:
        report_attach (str): The path to the report file to be attached.

    Returns:
        EmailMessage: The prepared email message with the report attachment.
    """
    def add_content(report_attach: str) -> EmailMessage:
        try:
            alertDate = datetime.now().strftime('%d_%m_%Y-%H_%M_%S')
            alert = EmailMessage()
            alert["Subject"] = f"Ransomware attack detected: Start Fast Recovery"
            alert["Content-Type"] = f"text/plain"  # Set content type to plain text
            
            # Set the content of the email
            alert.set_content(f"""
                Dear Administrator,

                Our system has detected a potential ransomware attack on the application.
                Immediate action is required to prevent data loss and further damage.
                
                Detected at: {alertDate}
                In the attached document you can find further information regarding data discrepancies detected.                      
                Please investigate the issue promptly and take necessary measures to mitigate the attack.
                If the revealed discrepancies are critical, start delphix Fast Recovery process of the affected Databases.

                Best regards,
                                    
                Ransomware Detection System
            """)
            
            # Attach the report file
            with open(report_attach, 'rb') as zip_file:
                try:
                    alert.add_attachment(zip_file.read(), maintype="application", subtype="zip", filename=report_attach[11:])
                except Exception as e:
                    print(f"error adding attachment to message \n Error : {e} ")
                    return alert
            return alert
        except MessageError or MessageDefect as e:
            raise Exception(msg=f"Error adding content to message \n Error : {e} ")

    """
    Evaluates discrepancies in the database by executing a SQL query.

    This method connects to the specified Oracle database using the provided credentials, 
    executes a query to retrieve discrepancies between expected and actual values, 
    and returns the result set.

    Args:
        db_hostname (str): The hostname of the database.
        db_port (str): The port of the database.
        username (str): The username to connect to the database.
        pwd (str): The password to connect to the database.
        sid (str): The SID (System Identifier) of the database.

    Returns:
        ResultSet: The result set containing discrepancies between expected and actual values.
    """
    def evaluate(self) -> None:

        start_time = datetime.now()
        vdb_conn = oracle_connect(host=decrypt_value(self.vdb.host), port=decrypt_value(self.vdb.port), user=decrypt_value(self.vdb.usr), password=decrypt_value(self.vdb.pwd), sid=decrypt_value(self.vdb.sid))
        curs = vdb_conn.cursor()
        try :
            rs = curs.execute("SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO FROM \
                            ( SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO, \
                CASE WHEN CAST(result AS VARCHAR(200)) = RES_ATTESO THEN 1 ELSE 0 END AS EVALUATION \
                    FROM CHECK_BASE CB LEFT JOIN ( \
                    WITH numbers AS( \
                        SELECT LEVEL AS n \
                        FROM DUAL \
                        CONNECT BY LEVEL <= 1000) \
                        SELECT ID, DATABASE_ID, TABLE_ID, COLUMN_ID, REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, '\"(\w+|\d+)\":', 1, n), '(\w+|\d+)') as chiavi, \
                            REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, ':\"(\w+|\d+)\"[,}]', 1, n), '(\w+|\d+)') as result \
                            FROM CHECK_2, numbers \
                            WHERE  n <= REGEXP_COUNT(RESULT, '\"(\w+|\d+)\":')  \
                            ORDER BY ID, n ) CR ON CB.VALUE = CAST(CR.chiavi AS VARCHAR(200)) \
                                                        LEFT JOIN CHECK_LINK CL ON CB.ID = CL.ID_BASE \
                                                            JOIN CHECK_VIEW_2 CV2 ON CL.ID_CHECK = CV2.ID) WHERE EVALUATION = 0")
            
            self.discrepant_values = rs.fetchall()
            end_time = datetime.now()
            print(f"Discrepancies evaluation finished in {end_time - start_time}")
        except oracle_error as e:
            raise Exception(msg=f"Error executing query to fetch control results \n Error : {e}")


class DelphixEngine:
    def __init__(self, ip: str):
        self.ip = ip
        self.base_uri = f"https://{ip}"
        self.session = Session()
        header = {
            "Content-Type": "application/json"
        }
        self.session.headers.update(header)

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        return self.ip

    """
        Sends a GET request to the specified API endpoint.

        This method constructs the full API URL and sends a GET request. 
        It handles authorization if a token is provided and checks the response for errors.

        Args:
            uri (str): The API endpoint to send the GET request to.
            key (str, optional): The key to extract from the JSON response.
            field (str, optional): The field to extract from the JSON response.
            auth_token (str, optional): The authorization token to include in the request headers.

        Returns:
            dict or any: The extracted result from the JSON response, or the full response if no key or field is specified.
        
        Raises:
            Exception: If the response indicates an error.
    """
    def _get(self, uri: str, key: Optional[str] = None, field: Optional[str] = None, auth_token: Optional[str] = None) -> Any:

        api = f"{self.base_uri}/{uri}"
        if auth_token:
            self.session.headers.update({'Authorization': f'{auth_token}'})
        
        try:
            response = self.session.get(api, verify=False)
        except RequestException as e:
            raise RequestException(response=e.response)
        
        # Check for errors in the response
        if auth_token:
            if not response.ok or "errorMessage" in response.json():
                raise Exception(msg=f"{uri}: {response.json()}")
        else:
            if not response.ok or response.json().get('status') == 'ERROR':
                raise Exception(msg=f"{uri}: {response.json()}")
        
        result = response.json()
        if key:
            result = result[key]
        if field:
            result = result[field]
        
        return result

    """
        Sends a POST request to the specified API endpoint.

        This method constructs the full API URL and sends a POST request with the provided data. 
        It checks the response for errors and handles authorization if necessary.

        Args:
            uri (str): The API endpoint to send the POST request to.
            data (dict, optional): The data to send in the POST request.
            key (str, optional): The key to extract from the JSON response.

        Returns:
            dict: The JSON response from the POST request.
        
        Raises:
            Exception: If the response indicates an error.
    """
    def _post(self, uri: str, data: Optional[Dict] = None, key: Optional[str] = None) -> Dict:

        api = f"{self.base_uri}/{uri}"
        try:
            response = self.session.post(api, json=data, verify=False)
        except RequestException as e:
            raise RequestException(response=e.response)
        # Check for errors in the response
        if "Authorization" in self.session.headers or uri == "masking/api/login":
            if not response.ok or "errorMessage" in response.json():
                raise Exception(msg=f"{uri}: {response.json()}\ndata: {data} \n code : {response.status_code}")
        else:
            if not response.ok or response.json().get('status') == 'ERROR':
                raise Exception(msg=f"{uri}: {response.json()}\ndata: {data}")
        
        return response

    """
        Filters a list of dictionaries based on a specified parameter.

        This method returns a list of dictionaries where the specified parameter matches the given string.

        Args:
            list (list): The list of dictionaries to filter.
            string (str): The string to match against the specified parameter.
            param (str): The parameter to check in each dictionary.

        Returns:
            list: A list of matching dictionaries, or None if no matches are found.
    """
    def filter_by_string(self, list_of_dicts: List[Dict], string: str, param: str) -> Optional[List[Dict]]:

        list = [s for s in list if s.get(param) == string]
        if len(list):
            return list
        else:
            print("No element found")
            return None
    
    
    """
        Retrieves the latest snapshot from a list based on a specified parameter.

        This method compares the specified parameter of each item in the list and returns the one with the latest value.

        Args:
            List (list): The list of items to evaluate.
            param (str): The parameter to compare for determining the latest item.

        Returns:
            dict: The item with the latest value for the specified parameter.
    """
    def get_latest_snap(self, List_of_dicts: List[Dict], param: str) -> Dict:

        if len(List) == 1:
            return List[0]
        
        latest = List[0]
        for x in List:
            if parser.isoparse(latest[param]) < parser.isoparse(x[param]):
                latest = x
        
        return latest

    """
        Creates a new session with the Delphix API.

        This method sends a POST request to the session endpoint with the specified API version.

        Args:
            api_version (str): The version of the API to use for the session.

        Returns:
            dict: The JSON response containing session information.
    """
    def create_session(self, api_version: str) -> Dict:

        uri = r"resources/json/delphix/session"
        major, minor, micro = api_version.split('.')
        data = {
            "type": "APISession",
            "version": {
                "type": "APIVersion",
                "major": int(major),
                "minor": int(minor),
                "micro": int(micro)
            }
        }
        try:
            response = self._post(uri, data)
            return response.json()
        except RequestException as e:
            raise Exception(msg=f"error creating session version {api_version} with engine {self.ip}, bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(msg=f"error creating session version {api_version} with engine {self.ip} \n Error : {e.msg if e.hasattr('msg') else e}")

    """
        Logs in to the Delphix API using the provided credentials.

        This method sends a POST request to the login endpoint with the username and password.

        Args:
            username (str): The username for authentication.
            password (str): The password for authentication.

        Returns:
            dict: The JSON response containing login information.
        
        Raises:
            Exception: If the login fails.
    """
    def login_data(self, username: str, password: str) -> Dict:

        uri = r"resources/json/delphix/login"
        data = {
            'type': 'LoginRequest',
            'username': username.strip(),
            'password': password.strip(),
            'target': 'DOMAIN'
        }
        try:
            response = self._post(uri, data)
            return response
        except RequestException as e:
            raise Exception(msg=f"error logging in data engine {self.ip}, bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(msg=f"error logging in data engine {self.ip} \n Error : {e.msg if e.hasattr('msg') else e}")

    """
    Logs in to the compliance API using the provided credentials.

    This method sends a POST request to the compliance login endpoint and updates the session headers with the authorization token.

    Args:
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        str: The authorization token received from the login response.
    """
    def login_compliance(self, username: str, password: str) -> str:

        uri = "masking/api/login"
        data = {
            'username': username.strip(),
            'password': password.strip()
        }
        try:
            response = self._post(uri, data)
            self.session.headers.update({'Authorization': response.json()["Authorization"]})
            return response.json()["Authorization"]
        except RequestException as e:
            raise Exception(msg=f"error logging in compliance engine {self.ip}, Bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(msg=f"error logging in compliance engine \n Error : {e.msg if e.hasattr('msg') else e}")

    """
    Executes a replication job based on the specified reference.

    This method sends a POST request to start the replication job and monitors its progress until completion.

    Args:
        reference (str): The reference ID for the replication job.

    Returns:
        None
    """
    def replication(self, reference: str) -> None:

        uri_repExe = rf"resources/json/delphix/replication/spec/{reference}/execute"
        uri_SourceState = r"resources/json/delphix/replication/sourcestate"
        try:
            job_id = (self._post(uri_repExe)).json()['job']
            uri_jobId = rf"resources/json/delphix/job/{job_id}"

            with IncrementalBar(message=rf"Preparing replication job {(self._get(uri_jobId, key='result'))['target']}", suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100) as bar:
                    while self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"] is None:
                        bar.next(1)
            
            with IncrementalBar(message=rf"Waiting for replication {(self._get(uri_jobId, key='result'))['target']} to finish", suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100) as bar:
                while self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"] is not None:
                    bar.next(1)
        
        except RequestException as e:
            raise Exception(msg=f"error executing replication job, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(msg=f"error executing replication job \n Error : {e.msg if e.hasattr('msg') else e}")

    """
    Refreshes a virtual database (VDB) using the specified data source reference.

    This method retrieves the latest snapshot for the specified data source and sends a request to refresh the VDB.

    Args:
        vdb_ref (str): The reference ID of the virtual database to refresh.
        dSource_ref (str): The reference ID of the data source to use for the refresh.

    Returns:
        None
    """
    def refresh(self, vdb_ref: str, dSource_ref: str) -> None:

        uri_snap = rf"resources/json/delphix/capacity/snapshot"
        uri_refresh = rf"resources/json/delphix/database/{vdb_ref}/refresh"
        try:
            snaps = self.filter_by_string(self._get(uri_snap, key="result"), dSource_ref, "container")

            data = {
                "type": "OracleRefreshParameters",
                "timeflowPointParameters": {
                    "type": "TimeflowPointSnapshot",
                    "snapshot": snaps[0]['snapshot']
                }
            }

            job_id = (self._post(uri_refresh, data)).json()['job']
            uri_jobId = rf"resources/json/delphix/job/{job_id}"
            
            with IncrementalBar(message=rf"Waiting for refresh of {vdb_ref} to finish", suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100) as bar:
                while self._get(uri_jobId, key="result")['jobState'] != "COMPLETED":
                    bar.next(1)
        except RequestException as e:
                raise Exception(msg=f"error executing vdb refershing job, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(msg=f"error executing vdb refershing job \n Error : {e.msg if e.hasattr('msg') else e}")
    
    """
        Executes a masking job based on the specified job ID.

        This method sends a request to start the masking job and monitors its progress until completion.

        Args:
            jobId (str): The ID of the masking job to execute.

        Returns:
            None
    """
    def mask(self, jobId: str) -> None:

        uri = "masking/api/executions"
        data = { "jobId": jobId }

        try:
            execId = self._post(uri, data).json()['executionId']
            uriExec = rf"masking/api/executions/{execId}"
            
            with IncrementalBar(message=rf"Waiting for control to finish", suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100) as bar:
                while self._get(uriExec, key="status") == "RUNNING":
                    bar.next(1)
            
            if self._get(uriExec, key="status") == "SUCCEEDED":
                print(rf"Control completed successfully")
            elif self._get(uriExec, key="status") == "FAILED": 
                print(rf"Error encountered during Control")
                raise Exception(msg=f"values control job with discovery engine failed. Check delphix dashboard")
        except RequestException as e:
                raise Exception(msg=f"error executing masking job to check values, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(msg=f"error executing masking job to check values \n Error : {e.msg if e.hasattr('msg') else e}")

if __name__ == "__main__":

    # con piu controlli, query database con controlli invece di config

    cfg_dict = load_config()
    for ctrl in cfg_dict.get('Controls'):
            try:
                ctrl_factory = controlFactory(controlClass=ctrl.get('name'))
                rcheck = ctrl_factory.instance_control(control_data=ctrl, cfg_data=cfg_dict)
                controlDatabase(rcheck)
            except Exception as e:
                print((e.msg if e.hasattr('msg') else f"Error executing control {ctrl.get('control')} : \n Error : {e}"))
                continue
    os.system('clear')
    print("Controls Terminated")





