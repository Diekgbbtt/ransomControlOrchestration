#! /usr/bin/env python3

""""
author : diego gobbetti
version : 0.0.1
email : d.gobbetti@nsr.it
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
from requests import packages
from smtplib import SMTP, SMTPException
# import cx_Oracle
from email.message import EmailMessage
from email.errors import *
from utils import decrypt_value, load_config, controlDatabase

# standard library modules
import os
from typing import  Union, List
from warnings import simplefilter
from urllib3 import disable_warnings, exceptions
from os import remove, chdir, makedirs, path, write, close, open, O_CREAT, O_WRONLY
from math import ceil
from shutil import copy
from datetime import datetime
from zipfile import ZipFile
from sqlite3 import connect, Error as sqlite_error, OperationalError, InterfaceError

# local modules
from .delphix_engine import DelphixEngine
from .control import ControlClass, ControlFactory
from .db_connector import DBConnector




disable_warnings(exceptions.InsecureRequestWarning)  # needed to suppress warnings
simplefilter('ignore')
packages.urllib3.disable_warnings()


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


    
class RansomCheck(ControlClass):

    def __init__(self, control_data: dict, cfg_data: dict) -> None:

        for key, val in control_data.items():
            setattr(self, key, val)
        try:
            self.initialize_objs(cfg_data=cfg_data)
        except Exception as e:
            raise Exception((str(e) if str(e) else f"Error initializing engines. \n Error : {e}"))

    def initialize_objs(self, cfg_data: dict) -> None:
        try:
            cfg_source_engines = cfg_data.get('dpx_engines').get('source_engines')
            cfg_vault_engines = cfg_data.get('dpx_engines').get('vault_engines')
            cfg_disc_engines = cfg_data.get('dpx_engines').get('discovery_engines')
            cfg_vdbs_control = cfg_data.get('vdbs_control')
            
            if not (isinstance(cfg_source_engines, dict) and isinstance(cfg_vault_engines, dict) and isinstance(cfg_disc_engines, dict) and isinstance(cfg_vdbs_control, dict)):
                raise Exception("Engines are not configured properly in config file")
            
            for key, val in cfg_source_engines.items():   
                if key == self.sourceEngine:
                        self.source_engine = DelphixEngine(decrypt_value(val['host']))
                        self.source_engine.create_session(val['apiVersion'])
                        self.source_engine.login_data(decrypt_value(val['usr']), decrypt_value(val['pwd']))

            for key, val in cfg_vault_engines.items():              
                if key == self.vaultEngine:
                        self.vault_engine = DelphixEngine(decrypt_value(val['host']))
                        self.vault_engine.create_session(val['apiVersion'])
                        self.vault_engine.login_data(decrypt_value(val['usr']), decrypt_value(val['pwd']))
            
            for key, val in cfg_disc_engines.items():
                if key == self.discEngine:
                        self.disc_engine = DelphixEngine(decrypt_value(val['host']))
                        self.disc_engine.login_compliance(decrypt_value(val['usr']), decrypt_value(val['pwd']))

            for key, val in cfg_vdbs_control.items():
                if key == self.vdb:
                    if isinstance(val, dict):
                        self.vdb_target = val.copy()
                        self.vdb_target.tech = DBConnector.get_technology(self.vdb_target.tech)
                    else:
                        raise Exception(f"Error in config data: missing or wrong formatted vdb control details")
            
            if isinstance(cfg_data.get('mail'), dict):
                self.mail = cfg_data.get('mail').copy()
            else:
                raise Exception(f"Error in config data: missing mail details")
            
        except AttributeError as e:
            raise Exception(f"Error initializing engines. Invalid config, missing parameter in control data")
        except Exception as e:
            raise Exception(f"Error initializing engines. \n Error : {str(e) if str(e) else e}")
        
    def start(self) -> None:
        try:
            self.source_engine.replication(self.replicationSpec)
            self.vault_engine.refresh_control_vdb(self.vdbContainerControl, self.dSourceContainer)
            self.update_catalogs()
            self.disc_engine.mask(self.jobId)
            self.evaluate()
            if len(self.discrepant_values) > self.max_discrepancies:
                self.register_report()
                self.create_report()
                self.send_alert()
                self.source_engine.delete_latest_snap(self.dSourceContainer)
                self.register_discrepancies()
                os.exit(1)
            else:
                self.source_engine.refresh_recovery_db(self.vdbContainerRecovery)
                self.update_expected_values()
                if not len(self.discrepant_values)==0:
                    self.register_report()
                    self.create_report()
                    self.register_discrepancies()
                    self.backup_report()
        except Exception as e:
            raise Exception(f"Error executing control. \n Error : {(str(e) if str(e) else e)}")

    def stop(self) -> None:

        return

    """
        Creates a report file and zips it, returning the path to the zipped report.
        
        Args:
            report_file_name (str): The name of the report file to create.
            db_conn (sqlite3.Connection): The database connection to use for updating the report status.
        
        Returns:
            str: The path to the zipped report file, or None if an error occurred.
    """


    def connect_local_db(self, file_name: str = 'reports.db') -> None:
        try:
            self.db_conn = connect(file_name)
        except OperationalError or sqlite_error as e:
            raise Exception(f"Errror connecting to vault database. \n Error : {e}")
  
    def create_report(self) -> Union[str, None]:

        try:
            if not path.exists("Evaluation"):
                makedirs("Evaluation")
            chdir("Evaluation") 
            self.report_file_path = f"{self.report_file_name}.txt"
            flags = O_CREAT | O_WRONLY  # Create file if it doesn't exist, open for writing
            mode = 0o666  # Permissions for the file
            fd = open(self.report_file_path, flags, mode)
            for row in self.discrepant_values:
                write(fd, f"discrepancy revealed in database {row[0]} table {row[1]} column {row[2]}, expected value is {row[4]} while the actual value is {row[3]} \n".encode())
            close(fd)
            self.zip_report()
        except OSError as e:
            raise Exception(f"Error creating report: \n {(e.strerror if e.hasattr('strerror') else e)} \n error number:  {e.errno}")
        except Exception as e:
            raise Exception(f"Error creating report: \n {(str(e) if str(e) else e)}")

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
            raise Exception(f"Error zipping report: \n {str(e) if str(e) else e}")


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
            raise Exception(f"error opening file, reading or writing in chunks: {e} \n {e.strerror if e.hasattr('strerror') else e}")
        except Exception as e:
            raise Exception(f"Error assessing dimensions: \n {str(e) if str(e) else e}")

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

        except sqlite_error as e:
            raise Exception(f"Error updating report status: \n {e}")

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
    def register_report(self) -> None:
        
        self.report_file_name = f"discrepancies_{datetime.now().strftime('%d_%m_%Y-%H_%M_%S')}"
        try:
            self.connect_local_db(file_name='reports.db')
            cursor = self.db_conn.cursor()
        except InterfaceError or sqlite_error as e:
            if self.db_conn:
                self.db_conn.rollback()
                # reverts any uncommitted changes made in the current transaction. 
                # This restores the database to its previous state before those changes were applied
            try:
                self.connect_local_db(file_name='reports.db')
                self.register_report()
            except sqlite_error as e:
                raise Exception((str(e) if str(e) else f"Error getting cursor from db connection: {e}"))

        try:
            processing_status = "REPORT TO BE GENERATED"
        
            # add discrepancies report to the list with timestamp and control processing status
            cursor.execute('''
            INSERT INTO reports (report_name, processing_status, created_at)
            VALUES (:report_name, :processing_status, :created_at)
            ''', {
                'report_name': self.report_file_name,
                'processing_status': processing_status,
                'created_at': datetime.now().strftime('%d_%m_%Y-%H_%M_%S'),
            })

            self.report_id = cursor.lastrowid
        
        except sqlite_error as e:
            if self.db_conn:
                self.db_conn.rollback()
            try:
                self.connect_local_db(file_name='reports.db')
                self.register_report()
            except sqlite_error as e:
                raise Exception((str(e) if str(e) else f"Error registering reports: {e}"))

    def register_discrepancies(self):
        try:
            # add single discrepancies revealed, each with reference to their report
            for row in self.discrepant_values:
                self.db_conn.cursor.execute('''
                INSERT INTO discrepancies (db, "table", "column", value, discrepancy, report_id)
                VALUES (:db, :table, :column, :value, :discrepancy, :report_id)
                ''', {
                    'db': row[0],
                    'table': row[1],
                    'column': row[2],
                    'value': row[3],
                    'discrepancy': f'effettivo {row[3]} != atteso {row[4]}',
                    'report_id': self.report_id
                })

            self.db_conn.commit()
        except sqlite_error as e:
            if self.db_conn:
                self.db_conn.rollback()
            try:
                self.connect_local_db(file_name='reports.db')
                self.register_report()
            except sqlite_error as e:
                raise Exception((str(e) if str(e) else f"Error registering reports: {e}"))
            


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
            raise Exception(f"Error copying or creating directory, while backing up report: \n {e.strerror if e.hasattr('strerror') else e}")
        except Exception as e:
            raise Exception(f"Error backing up report: {str(e) if str(e) else e}")

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
                mail_server.starttls()
                mail_server.login(decrypt_value(self.mail.usr), decrypt_value(self.mail.pwd))
                
                for report in self.reports_zip_path: 
                    content = self.add_content(report)  # Prepare the email content with the report attachment
                    mail_server.send_message(content, decrypt_value(self.mail.usr), self.mailReceivers)  # Send the email
                    
                    if len(self.reports_zip_path) == 1:
                        self.update_report_status(report[:-4], f"REPORT SENT")
                    elif len(self.reports_zip_path) > 1:
                        self.update_report_status(report[:-4], f"PART {report.split('_')[2].strip()[:-4]} OF REPORT SENT")

        except SMTPException as e:
            raise Exception(f"Error sending email: {(e.strerror if e.hasattr('strerror') else e)}") 
        except MessageError or MessageDefect as e:
            raise Exception(f"Error sending email: {str(e)}")
        except Exception as e:
            raise Exception(f"Error sending email: {str(e) if str(e) else e}")
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
            raise Exception(f"Error adding content to message \n Error : {e} ")
        
    def update_catalogs(self):
        try:
            connector = DBConnector.get_technology(self.vdb_target.tech)
            with connector(**self.vdb_target) as db_conn:
                for proc in self.procedures:
                    db_conn.execute_procedure(proc)
        except Exception as e:
            raise Exception(f"Error refreshing catalogs by triggering execution of stored procedures \n Error : {e} ")

    def update_expected_values(self):
        
        procedure_data = [
            (row[0], row_values[0], row_values[1])
            for row in self.retrieve_target_columns()
                for row_values in self.retrieve_values(row)
        ]
        types = [{"value" : "CheckBaseTableType", "collection": True, "type" : {"value" : "CheckBaseRecType", "collection" : False} }]
        with self.vdb_target.tech(**self.vdb_target) as db_conn:
                db_conn.execute_procedure(self.expected_proc, [procedure_data], types=types)

    def retrieve_target_columns(self, results_table: str = "CHECK_2"):
        try:
            with self.vdb_target.tech(**self.vdb_target) as db_conn:
                return db_conn.execute_query(f"SELECT ID, DATABASE_ID, TABLE_ID, COLUMN_ID FROM C##control_user.{results_table}", all=True)
        except Exception as e:
            raise Exception(f"Error retrieving target columns from results table \n Error : {e} ")

    def retrieve_values(self, row):
        try:
            with self.vdb_target.tech(**self.vdb_target) as db_conn:
                cv2_rs = db_conn.execute_query("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM :schema.CHECK_VIEW_2 WHERE DB_ID = ':database_id' AND TABLE_ID = ':table_id' AND COLUMN_ID = ':column_id'", True, None, "C##control_user", row[1], row[2], row[3])
                return db_conn.execute_query("SELECT :column_name AS VALUE, COUNT(*) AS COUNT, FROM :table_schema.:table_name GROUP BY :column_name", True, None, cv2_rs[2], cv2_rs[0], cv2_rs[1], cv2_rs[2])
        
        except Exception as e:
            raise Exception(f"Error retrieving values from database \n Error : {e} ")
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
        try:
            start_time = datetime.now()
            with self.vdb_target.tech(**self.vdb_target) as db_conn:

                self.discrepant_values =  db_conn.execute_query(
                            query="""
                                SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO FROM \
                                                ( SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO, \
                                    CASE WHEN CAST(result AS VARCHAR(200)) = RES_ATTESO THEN 1 ELSE 0 END AS EVALUATION \
                                        FROM C##control_user.CHECK_BASE CB LEFT JOIN ( \
                                        WITH numbers AS( \
                                            SELECT LEVEL AS n \
                                            FROM DUAL \
                                            CONNECT BY LEVEL <= 100000) \
                                            SELECT ID, DATABASE_ID, TABLE_ID, COLUMN_ID, REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, '\"(\w+|\d+)\":', 1, n), '(\w+|\d+)') as chiavi, \
                                                REGEXP_SUBSTR(REGEXP_SUBSTR(RESULT, ':\"(\w+|\d+)\"[,}]', 1, n), '(\w+|\d+)') as result \
                                                FROM C##control_user.CHECK_2, numbers \
                                                WHERE  n <= REGEXP_COUNT(RESULT, '\"(\w+|\d+)\":')  \
                                                ORDER BY ID, n ) CR ON CB.VALUE = CAST(CR.chiavi AS VARCHAR(200)) AND CB.ID_CHECK = CR.ID \
                                                                                JOIN CHECK_VIEW_2 CV2 ON CB.ID_CHECK = CV2.ID) WHERE EVALUATION = 0")""",
                            all=True)
                    
            end_time = datetime.now()
            print(f"Discrepancies evaluation finished in {end_time - start_time}")
        except Exception as e:
            raise Exception(f"Couldn't evaluate dicsrepancies in the target database \n : {e}")


if __name__ == "__main__":

    # con piu controlli, query database con controlli invece di config

    cfg_dict = load_config()
    for ctrl in cfg_dict.get('Controls'):
            try:
                ctrl_factory = ControlFactory(controlClass=ctrl.get('name'))
                rcheck = ctrl_factory.instance_control(control_data=ctrl, cfg_data=cfg_dict)
                controlDatabase(rcheck)
            except Exception as e:
                print((str(e) if str(e) else f"Error executing control {ctrl.get('control')} : \n Error : {e}"))
                continue
    os.system('clear')
    print("Controls Terminated")





