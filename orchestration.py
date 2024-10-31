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
from requests import packages, Session
from smtplib import SMTP
import oracledb
from email.message import EmailMessage
from progress.bar import IncrementalBar

# standard library modules
from abc import ABC, abstractmethod
from sys import stdout
from warnings import simplefilter
from urllib3 import disable_warnings, exceptions
from json import load as json_load
from os import remove, chdir, makedirs, path, write, close, open, O_CREAT, O_WRONLY
from math import ceil
from shutil import copy
import time
from dateutil import parser # type: ignore
from datetime import datetime
from zipfile import ZipFile
from sqlite3 import connect, Connection



disable_warnings(exceptions.InsecureRequestWarning)  # needed to suppress warnings
simplefilter('ignore')
packages.urllib3.disable_warnings()

oracledb.defaults.fetch_lobs = False

""""

*****************************************************************************************************************
*   possibile implmentazione di una maggirore estrazione del controllo con design pattern                       *
*   factory method, nell'ottica di effettuare controli di diverso tipo utilizando sempre delphix come engine    *
*****************************************************************************************************************

A PRIORI E' NECESSARIA UNA QUERY A CATALOGO CON DB,TB, CLN E RELATIVI CONTROLLI


for(row : results)
    controlName = row.getcolumn(x)
    controlData = row.getmultiplecolumns.... + data elaborations needed
    factory = controlFactory(controlName)
    controlDatabase(factory.instance_control(controlData))



"""

"""
if(len(sys.argv) < 3):
    print("Utilizzo : py dpx_integration.py <replication_spec> <vdbContainer_ref> <dSourceContainer_ref>")
    sys.exit(1)
"""

"""
Evaluates the discrepancies between the expected and actual values in the database.

This function connects to the database using the provided credentials, and then executes a SQL query to retrieve the discrepancies between the expected and actual values. The function returns the result set containing the discrepancies.

Args:
    db_hostname (str): The hostname of the database.
    db_port (str): The port of the database.
    username (str): The username to connect to the database.
    pwd (str): The password to connect to the database.
    sid (str): The SID (System Identifier) of the database.

Returns:
    A result set containing the discrepancies between the expected and actual values in the database.
"""


def controlDatabase(check: controlClass):

    check.start()
    while( not check.finish()):
        print("controllo in corso")


# display a bar that shows the progress of the process
def print_process_status():
        bar = IncrementalBar(suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100)
        for i in bar.iter(range(200)):
            time.sleep(0.01)



class controlClass(ABC):

    @abstractmethod
    def __init__(self, data):
        
        return

    @abstractmethod
    def start(self):

        return

    @abstractmethod
    def finish(self):

        return


class controlFactory:

    def __init__(self, controlName : controlClass):
        self.control = controlName
    
    def instance_control(self, data):
        Control = self.control(data)
        return Control

    
class ransomChek(controlClass):

    def __init__(self, data):
        
        return

    def start(self):
        try:
            with open('config.json', 'r') as cfg:
                cfg_dict = json_load(cfg)

        except Exception as e:
            print(f"Error opening config: {str(e)}")

        for rep in cfg_dict.get('Replications'):
            try:

                engine_1 = DelphixEngine(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['host'])
                engine_1.create_session(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['apiVersion'])
                engine_1.login_data(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['pwd'])
                engine_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['host'])
                engine_13.create_session(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['apiVersion'])
                engine_13.login_data(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['pwd'])
                engineCompl_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['host'])
                engineCompl_13.login_compliance(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['pwd'])

                engine_1.replication(rep['replicationSpec'])
                engine_13.refresh(rep['vdbContainerRef'], rep['dSourceContainer_ref'])

                engineCompl_13.mask(rep['jobId'])

                discrepant_values = self.evaluate(cfg_dict.get('vdbs_control').get(rep['vdbRef'])['host'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['port'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['usr'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['pwd'],  cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid'] if cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid']!=None else None)

                if(discrepant_values):
                    report_file_name = f"discrepancies_{datetime.now().strftime('%d_%m_%Y-%H_%M_%S')}"
                    db_conn = connect('reports.db')
                    self.register_reports(discrepant_values, report_file_name, db_conn)
                    reports_zip_path = self.create_report(report_file_name, db_conn) # zip the new discrepancies file
                    self.send_alert(domain=cfg_dict.get('mail')['smtpServer'], username=cfg_dict.get('mail')['usr'], pwd=cfg_dict.get('mail')['pwd'], reports_path=reports_zip_path, sender=cfg_dict.get('mail')['usr'], receivers=rep['mailReceivers'], connector=db_conn)
                    self.backup_report(reports_zip_path, db_conn)
            
            except Exception as e:
                print(f"Error processing replication {rep['replicationSpec']}: {str(e)}")

    def finish(self):

        return

    """
        Creates a report file and zips it, returning the path to the zipped report.
        
        Args:
            report_file_name (str): The name of the report file to create.
            db_conn (sqlite3.Connection): The database connection to use for updating the report status.
        
        Returns:
            str: The path to the zipped report file, or None if an error occurred.
    """
    def create_report(self, report_file_name, db_conn):

        try:
            if not path.exists("Evaluation"):
                makedirs("Evaluation")
            chdir("Evaluation") 
            test_file_path = f"{report_file_name}.txt"
            flags = O_CREAT | O_WRONLY  # Create file if it doesn't exist, open for writing
            mode = 0o666  # Permissions for the file
            fd = open(test_file_path, flags, mode)
            write(fd, b"TEST - discrepancies test")
            close(fd)
            return self.zip_report(test_file_path, db_conn)
        except Exception as e:
            print(f"Error creating report: {str(e)}")
            return None


    """
    Creates a zip file from the specified report file and removes the original report file.

    Args:
        report_path (str): The path to the report file to be zipped.
        db_conn (sqlite3.Connection): The database connection to use for updating the report status.

    Returns:
        str or list: The path to the zipped report file, or a list of chunked file paths if the zipped file exceeds the maximum size.
    """
    def zip_report(self, report_path, db_conn):

        try:
            zip_path = report_path.replace(".txt", ".zip")
            with ZipFile(zip_path, "x") as zip:
                zip.write(report_path, compresslevel=9)
            remove(report_path)
            return self.asses_dimensions(zip_path=zip_path, connector=db_conn)
        except Exception as e:
            print(f"Error zipping report: {str(e)}")
            return None


    """
    Assesses the size of the zipped report file and determines if it needs to be split into smaller chunks.

    Args:
        zip_path (str): The path to the zipped report file.
        connector: The database connection to use for updating the report status.

    Returns:
        list: A list of file paths for the zipped report or its chunks.
    """
    def asses_dimensions(self, zip_path: str, connector):

        try:
            # Constants
            CHUNK_SIZE = 150 * 1024 * 1024  # 150MB in bytes
            MAX_SIZE = 300 * 1024 * 1024  # 300MB in bytes
            
            # Check the size of the file
            file_size = path.getsize(zip_path)
            
            # If file is smaller than or equal to 300MB, no need to chunk
            if file_size <= MAX_SIZE:
                self.update_report_status(zip_path[:-4], "REPORT ZIP CREATED", connector)
                return [zip_path]  # Return original file path
            
            # Split into chunks if file is larger than 300MB
            file_parts = []
            with open(zip_path, 'rb') as f:
                total_parts = ceil(file_size / CHUNK_SIZE)
                base_name, ext = path.splitext(zip_path)
                
                for i in range(total_parts):
                    part_file_path = f"{base_name}_part_{i + 1}{ext}"
                    with open(part_file_path, 'wb') as chunk_file:
                        chunk_data = f.read(CHUNK_SIZE)
                        if chunk_data:
                            print(chunk_data)
                            chunk_file.write(chunk_data)
                            file_parts.append(part_file_path)
            self.update_report_status(zip_path[:-4], "MULTIPLE REPORT ZIP CREATED", connector)
            return file_parts
        except Exception as e:
            print(f"Error assessing dimensions: {str(e)}")
            return None

    """
    Updates the processing status of a report in the database.

    Args:
        report_name (str): The name of the report to update.
        status (str): The new status to set for the report.
        db_conn (sqlite3.Connection): The database connection to use for the update.

    Returns:
        None
    """
    def update_report_status(report_name: str, status: str, db_conn) -> None:

        try:
            cursor = db_conn.cursor()

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
            db_conn.commit()
            print(f"Report {report_name} status updated to {status}")

        except Exception as e:
            print(f"Error updating report status: {str(e)}")

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
    def register_reports(discrepant_values, report_file_name, db_conn):
        try:
            cursor = db_conn.cursor()
        except Exception as e:
            print(f"Error connecting to db: {str(e)}")
            if db_conn:
                db_conn.rollback()
            return

        try:
            processing_status = "REPORT TO BE GENERATED"
        
            # Create the reports table if it doesn't exist
            cursor.execute('''
            INSERT INTO reports (report_name, processing_status, created_at)
            VALUES (:report_name, :processing_status, :created_at)
            ''', {
                'report_name': report_file_name,
                'processing_status': processing_status,
                'created_at': datetime.now().strftime('%d_%m_%Y-%H_%M_%S'),
            })

            report_id = cursor.lastrowid

            for row in discrepant_values:
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

            db_conn.commit()
        except Exception as e:
            print(f"Error registering reports: {str(e)}")
            if db_conn:
                db_conn.rollback()


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
    def backup_report(self, reports_zip_path, db_conn):

        # Create the reports backup directory if it doesn't exist
        if not path.exists("Evaluation/backup"):
            makedirs("Evaluation/backup")
        
        # Add the file to the backup directory
        for report_zip_path in reports_zip_path:
            copy(report_zip_path, "Evaluation/backup")
            if len(reports_zip_path) == 1:
                self.update_report_status(report_zip_path[:-4], "REPORT ZIP BACKED UP", db_conn)
            elif len(reports_zip_path) > 1:
                self.update_report_status(report_zip_path[:-4], f"REPORT ZIP PART {report_zip_path.split('_')[2].strip()[:-4]} BACKED UP", db_conn)
        
        db_conn.close()
        return


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

    def send_alert(self, domain: str, username: str, pwd: str, reports_path: str, sender: str, receivers: list[str], connector) -> None:

        with SMTP(domain) as mail_server:
            # Start TLS for security
            mail_server.starttls()
            mail_server.login(username, pwd)
            
            for report in reports_path: 
                content = self.add_content(report)  # Prepare the email content with the report attachment
                mail_server.send_message(content, sender, receivers)  # Send the email
                
                # Update the report status in the database
                if len(reports_path) == 1:
                    self.update_report_status(report[:-4], f"REPORT SENT", connector)
                elif len(reports_path) > 1:
                    self.update_report_status(report[:-4], f"PART {report.split('_')[2].strip()[:-4]} OF REPORT SENT", connector)
        
        return

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
            alert.add_attachment(zip_file.read(), maintype="application", subtype="zip", filename=report_attach[11:])
        
        return alert


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
    def evaluate(db_hostname, db_port, username, pwd, sid):

        start_time = datetime.now()
        vdb_conn = oracledb.connect(host=db_hostname, port=db_port, user=username, password=pwd, sid=sid)
        curs = vdb_conn.cursor()

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
        
        end_time = datetime.now()
        print(f"Discrepancies evaluation finished in {end_time - start_time}")

        return rs


        """
        try:
            completedProcess = subprocess.run(["bash", "resultsControl.sh", db_hostname, db_port, username, pwd, sid], capture_output=True, check=True, text=True)
        except subprocess.CalledProcessError as e: # check option - non-zero returncode -> exc CalledProcessError
                print('Error occurred :' + e.stdout + '\n' + e.stderr)
        if(completedProcess.stdout): # se shell process ha scritto in stdout ci sono delle discrepanze
            # altro modo è fare ritornare un particolare return code e alzare eccezione solo se return-code = 1 o altri valori sbagliati noti
            return 0
        else:
            return 1
        """



class DelphixEngine:
    def __init__(self, ip, ):
        self.ip = ip
        self.base_uri = f"https://{ip}"
        self.session = Session()
        header = {
            "Content-Type": "application/json"
        }
        self.session.headers.update(header)

    def __repr__(self):
        return str(self)

    def __str__(self):
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
    def _get(self, uri, key=None, field=None, auth_token=None):

        api = f"{self.base_uri}/{uri}"
        if auth_token:
            self.session.headers.update({'Authorization': f'{auth_token}'})
        
        response = self.session.get(api, verify=False)
        
        # Check for errors in the response
        if auth_token:
            if not response.ok or "errorMessage" in response.json():
                raise Exception(f"{uri}: {response.json()}")
        else:
            if not response.ok or response.json().get('status') == 'ERROR':
                raise Exception(f"{uri}: {response.json()}")
        
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
    def _post(self, uri, data=None, key=None):

        api = f"{self.base_uri}/{uri}"
        response = self.session.post(api, json=data, verify=False)
        
        # Check for errors in the response
        if "Authorization" in self.session.headers or uri == "masking/api/login":
            if not response.ok or "errorMessage" in response.json():
                raise Exception(f"{uri}: {response.json()}\ndata: {data} \n code : {response.status_code}")
        else:
            if not response.ok or response.json().get('status') == 'ERROR':
                raise Exception(f"{uri}: {response.json()}\ndata: {data}")
        
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
    def filter_by_string(self, list, string, param):

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
    def getLatest_snap(self, List, param):

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
    def create_session(self, api_version):

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
        response = self._post(uri, data)
        return response.json()


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
    def login_data(self, username, password):

        uri = r"resources/json/delphix/login"
        data = {
            'type': 'LoginRequest',
            'username': username.strip(),
            'password': password.strip(),
            'target': 'DOMAIN'
        }
        response = self._post(uri, data)
        return response


    """
    Logs in to the compliance API using the provided credentials.

    This method sends a POST request to the compliance login endpoint and updates the session headers with the authorization token.

    Args:
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        str: The authorization token received from the login response.
    """
    def login_compliance(self, username, password):

        uri = "masking/api/login"
        data = {
            'username': username.strip(),
            'password': password.strip()
        }
        response = self._post(uri, data)
        self.session.headers.update({'Authorization': response.json()["Authorization"]})
        return response.json()["Authorization"]


    """
    Executes a replication job based on the specified reference.

    This method sends a POST request to start the replication job and monitors its progress until completion.

    Args:
        reference (str): The reference ID for the replication job.

    Returns:
        None
    """
    def replication(self, reference):

        start_time = datetime.now()
        uri_repExe = rf"resources/json/delphix/replication/spec/{reference}/execute"
        job_id = (self._post(uri_repExe)).json()['job']
        uri_jobId = rf"resources/json/delphix/job/{job_id}"

        uri_SourceState = r"resources/json/delphix/replication/sourcestate"
        source_state_list = self._get(uri_SourceState, key="result")

        print(rf"Preparing replication job {(self._get(uri_jobId, key='result'))['target']}...")
        while self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"] is None:
            print_process_status()
        
        print(rf"Waiting for replication {(self._get(uri_jobId, key='result'))['target']} to finish...")
        while self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"] is not None:
            print_process_status()
        
        end_time = datetime.now()
        print(rf"Replication job {(self._get(uri_jobId, key='result'))['target']} completed in {end_time - start_time}")

        return
    

    """
    Refreshes a virtual database (VDB) using the specified data source reference.

    This method retrieves the latest snapshot for the specified data source and sends a request to refresh the VDB.

    Args:
        vdb_ref (str): The reference ID of the virtual database to refresh.
        dSource_ref (str): The reference ID of the data source to use for the refresh.

    Returns:
        None
    """
    def refresh(self, vdb_ref, dSource_ref):

        start_time = datetime.now()
        uri_snap = rf"resources/json/delphix/capacity/snapshot"
        snaps = self.filter_by_string(self._get(uri_snap, key="result"), dSource_ref, "container")
        snap_ref = snaps[0]['snapshot']

        uri_refresh = rf"resources/json/delphix/database/{vdb_ref}/refresh"
        data = {
            "type": "OracleRefreshParameters",
            "timeflowPointParameters": {
                "type": "TimeflowPointSnapshot",
                "snapshot": snap_ref
            }
        }

        job_id = (self._post(uri_refresh, data)).json()['job']
        uri_jobId = rf"resources/json/delphix/job/{job_id}"
        print(rf"Waiting for refresh of {vdb_ref} to finish...")
        while self._get(uri_jobId, key="result")['jobState'] != "COMPLETED":
            print_process_status()
        
        end_time = datetime.now()
        print(f"Refresh of {vdb_ref} completed in {end_time - start_time}")

        return
    
    """
        Executes a masking job based on the specified job ID.

        This method sends a request to start the masking job and monitors its progress until completion.

        Args:
            jobId (str): The ID of the masking job to execute.

        Returns:
            None
    """
    def mask(self, jobId):

        start_time = datetime.now()
        uri = "masking/api/executions"

        data = { 
            "jobId": jobId 
        }

        execId = self._post(uri, data).json()['executionId']
        uriExec = rf"masking/api/executions/{execId}"
        
        print(rf"Waiting for control to finish...")
        while self._get(uriExec, key="status") == "RUNNING":
            print_process_status()
        
        if self._get(uriExec, key="status") == "SUCCEEDED":
            end_time = datetime.now()
            print(rf"Control completed successfully in {end_time - start_time}")
        elif self._get(uriExec, key="status") == "FAILED": 
            print(rf"Error encountered during Control")

        return


if __name__ == "__main__":

    # query database con controlli
    try:
        with open('config.json', 'r') as cfg:
                cfg_dict = json_load(cfg)

    except Exception as e:
            print(f"Error opening config: {str(e)}")
    
    rcheck = ransomChek(data=cfg_dict)
    controlDatabase()





