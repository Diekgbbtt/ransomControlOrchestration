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
        - add tech agnostic module
        - timing with completion bars
        -X costrutti try-catch
        - testing
        - piccolo portale?
    
    -   
        
"""

"""
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
import requests
import smtplib
import oracledb
from email.message import EmailMessage
# from db_techagnostic_connector import DBConnector
# import subprocess - for future parallel support

# standard library modules
import sys
import warnings
import urllib3
import json
import os
import math
import shutil
# import threading
import time
from dateutil import parser
from datetime import datetime
import zipfile
from typing import List
import sqlite3



urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # needed to suppress warnings
warnings.simplefilter('ignore')
requests.packages.urllib3.disable_warnings()

oracledb.defaults.fetch_lobs = False
"""
CLOBs and BLOBs smaller than 1 GB can queried from the database directly as strings and bytes. 
This can be much faster than streaming a LOB Object. Support is enabled by setting the Defaults Object.
"""

""""

******* QUERY CATALOGO CON DB,TB, CLN E CONTROLLI *********


for(row : results)
    controlName = row.getcolumn(x)
    controlData = row.getmultiplecolumns....
    factory = controlFactory(controlName)
    controlDatabase(factory.istance_control(data))



def controlDatabase(controlClass):

    check.start()
    while( not check.finish()):
        print("controllo in corso")


class controlFactory:

    def __init__(self, controlName):
        self.control = controlName
    
    def istance_control(self, data):
        Control = self.control(data)
        return Control



class controlClass:

    @abstractmethod
    def __init__(self, data):
        
        return

    @abstractmethod
    def start(self):

        return

    @abstractmethod
    def finish(self):

        return
    
class ransomChek(controlClass):

    def __init__(self, data):
        
        return

    def start(self):

        return

    def finish(self):

        return



class schemasChek(controlClass):

    def __init__(self, data):
        
        return

    def start(self):

        return

    def finish(self):

        return
"""

class ApiObject:
    def __init__(self, dictionary):
        for key, value in dictionary.items():
            if isinstance(value, dict): # controlla se value associato alla chiave corrente è un dizionario
                try:
                    klass = globals()[key.capitalize()]
                    setattr(self, key, klass(value))
                except:
                    setattr(self, key, value)
            else:
                setattr(self, key, value)
# la risposta json presenta dei dati in formato key:value, quindi può essere modellata come un dizionario
# si può dire che questa classe crea dinamicamente degli oggetti con atrtibuti e relativi valori corrispondenti
# alle coppi chiave valore nella risposta json. In particolare controlla se una chiave corrisponde ad una classs
# globals()[key.capitalize()], quindi ad un altro dizionario(coppie attributi:valore) in caso positivo prova ad 
# istanziare un oggetto della classe setattr(self, key, klass(value))


class Database(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)

    @property
    def masked_status(self):
        if self.masked:
            return 'MASKED DB'
        return 'CLEAR DB'


class Capacity(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)

    @property
    def actual_space(self):
        actual_space = self.breakdown.actualSpace
        return actual_space

    @property
    def ingested_size(self):
        ingested_size = self.breakdown.ingestedSize
        return ingested_size
"""
It inherits from the ApiObject class, which likely handles the conversion of API response data into Python objects.
b. The __init__ method is called when creating a new instance of the Capacity class. It takes a dictionary as input and passes it to the ApiObject class's __init__ method using super().__init__(dictionary).
c. The @property decorator is used to define two methods (actual_space and ingested_size) as properties, allowing them to be accessed like attributes.
d. The actual_space property retrieves the value of actualSpace from the breakdown attribute, which is likely another object representing the breakdown of capacity.
e. The ingested_size property retrieves the value of ingestedSize from the breakdown attribute. #
"""

class Source(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)

    @property
    def actual_size(self):
        actual_size = self.runtime.databaseSize
        return actual_size

    @property
    def db_type(self):
        db_type = "Source"
        if self.virtual:
            db_type = "VDB"
        return db_type



class Repository(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)
 
    @property
    def technology(self):
        technology = self.type
        if self.version:
            technology += f" - v{self.version}"
        else:
            technology = self.name
        return technology

        """
        if(len(sys.argv) < 3):
            print("Utilizzo : py dpx_integration.py <replication_spec> <vdbContainer_ref> <dSourceContainer_ref>")
            sys.exit(1)
        """

def main():
    try:
        with open('config.json', 'r') as cfg:
            cfg_dict = json.load(cfg)

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

                discrepant_values = evaluate(cfg_dict.get('vdbs_control').get(rep['vdbRef'])['host'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['port'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['usr'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['pwd'],  cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid'] if cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid']!=None else None)

                if(discrepant_values):
                    report_file_name = f"discrepancies_{datetime.now().strftime('%d_%m_%Y-%H_%M_%S')}"
                    db_conn = sqlite3.connect('reports.db')
                    register_reports(discrepant_values, report_file_name, db_conn)
                    reports_zip_path = create_report(report_file_name, db_conn) # zip the new discrepancies file
                    send_alert(domain=cfg_dict.get('mail')['smtpServer'], username=cfg_dict.get('mail')['usr'], pwd=cfg_dict.get('mail')['pwd'], reports_path=reports_zip_path, sender=cfg_dict.get('mail')['usr'], receivers=rep['mailReceivers'], connector=db_conn)
                    backup_report(reports_zip_path, db_conn)
            
            except Exception as e:
                print(f"Error processing replication {rep['replicationSpec']}: {str(e)}")
    except Exception as e:
        print(f"Error in main function: {str(e)}")

def print_process_status():
    sys.stdout.write('.')
    sys.stdout.flush()
    time.sleep(2)
    sys.stdout.write('\b \b' * 3)  # Erase the last three dots
    return

def create_report(report_file_name, db_conn):
    try:
        if not os.path.exists("Evaluation"):
            os.makedirs("Evaluation")
        os.chdir("Evaluation") 
        test_file_path = f"{report_file_name}.txt"
        flags = os.O_CREAT | os.O_WRONLY  # Create file if it doesn't exist, open for writing
        mode = 0o666  # Permissions for the file
        fd = os.open(test_file_path, flags, mode)
        os.write(fd, b"TEST - discrepancies test")
        os.close(fd)
        return zip_report(test_file_path, db_conn)
    except Exception as e:
        print(f"Error creating report: {str(e)}")
        return None

def zip_report(report_path, db_conn):
    try:
        zip_path = report_path.replace(".txt", ".zip")
        with zipfile.ZipFile(zip_path, "x") as zip:
            zip.write(report_path, compresslevel=9)
        os.remove(report_path)
        return asses_dimensions(zip_path=zip_path, connector=db_conn)
    except Exception as e:
        print(f"Error zipping report: {str(e)}")
        return None

def asses_dimensions(zip_path: str, connector):
    try:
        # Constants
        CHUNK_SIZE = 150 * 1024 * 1024  # 150MB in bytes
        MAX_SIZE = 300 * 1024 * 1024  # 300MB in bytes
        
        # Check the size of the file
        file_size = os.path.getsize(zip_path)
        
        # If file is smaller than or equal to 300MB, no need to chunk
        if file_size <= MAX_SIZE:
            update_report_status(zip_path[:-4], "REPORT ZIP CREATED", connector)
            return [zip_path]  # Return original file path
        
        # Split into chunks if file is larger than 300MB
        file_parts = []
        with open(zip_path, 'rb') as f:
            total_parts = math.ceil(file_size / CHUNK_SIZE)
            base_name, ext = os.path.splitext(zip_path)
            
            for i in range(total_parts):
                part_file_path = f"{base_name}_part_{i + 1}{ext}"
                with open(part_file_path, 'wb') as chunk_file:
                    chunk_data = f.read(CHUNK_SIZE)
                    if chunk_data:
                        print(chunk_data)
                        chunk_file.write(chunk_data)
                        file_parts.append(part_file_path)
        update_report_status(zip_path[:-4], "MULTIPLE REPORT ZIP CREATED", connector)
        return file_parts
    except Exception as e:
        print(f"Error assessing dimensions: {str(e)}")
        return None

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

def backup_report(reports_zip_path, db_conn):
        # Create the reports backup directory if it doesn't exist
        if not os.path.exists("Evaluation/backup"):
            os.makedirs("Evaluation/backup")
        # add the file to the backup directory
        for report_zip_path in reports_zip_path:
            shutil.copy(report_zip_path, "Evaluation/backup")
            if(len(reports_zip_path) == 1):
                update_report_status(report_zip_path[:-4], "REPORT ZIP BACKED UP", db_conn)
            elif(len(reports_zip_path) > 1):
                update_report_status(report_zip_path[:-4], f"REPORT ZIP PART {report_zip_path.split('_')[2].strip()[:-4]} BACKED UP", db_conn)
        db_conn.close()
        return


def send_alert(domain: str, username: str, pwd: str, reports_path: str, sender: str, receivers: List[str], connector) -> None:
        """
        An SMTP instance encapsulates an SMTP connection. It has methods that support a full repertoire of SMTP and ESMTP operations. 
        If the optional host and port parameters are given, the SMTP connect() method is called with those parameters during initialization.
        """
        with smtplib.SMTP(domain) as mail_server:
        # https://docs.python.org/3/library/smtplib.html#smtplib.SMTP
            mail_server.starttls()
            mail_server.login(username, pwd)
            for report in reports_path: 
                content = add_content(report)
                mail_server.send_message(content, sender, receivers)
                if(len(reports_path) == 1):
                    update_report_status(report[:-4], f"REPORT SENT", connector)
                elif(len(reports_path) > 1):
                    update_report_status(report[:-4], f"PART {report.split('_')[2].strip()[:-4]} OF REPORT SENT", connector)
        # mail_server.close()
        return

def add_content(report_attach: str) -> None:
        alertDate = datetime.now().strftime('%d_%m_%Y-%H_%M_%S')
        alert = EmailMessage()
        alert[""]=f"Ransomware attack detected : Start Fast Recovery"
        alert["Content-Type"]=f"text/plain" # multipart/mixed         
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
        
        """
            'rb' stands for read bytes, data is read as raw bytes without applying encoding, hence special chars like newline are interpreted as they are, 
            suitable for binary files like .jpg, .mp4, .exe
            with 'r' Data is read as strings (decoded from the file's binary content into text using the default or specified encoding, typically UTF-8).
            i.e. Line endings (\n in UNIX or \r\n in Windows) are translated to Python's universal newline character (\n).
        """
        # remove reports older than 7 days
        # loop on files in Evaluation directory

        with open(report_attach, 'rb' ) as zip_file: # encoding=get_encoding_type(file_path)
            alert.add_attachment(zip_file.read(), maintype="application", subtype="zip", filename=report_attach[11:])
        
        """
        If the message is a non-multipart, multipart/related, or multipart/alternative, call make_mixed() and then
        create a new message object, pass all of the arguments to its set_content() method, and attach() it to the multipart
        """
        # https://docs.python.org/3/library/email.message.html#email.message.EmailMessage.add_attachment

        return alert


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
    print(f"discrepancies evaluation finished in {end_time - start_time}")

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
        self.session = requests.Session()
        header = {
            "Content-Type": "application/json"
        }
        self.session.headers.update(header)

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.ip

    def _get(self, uri, key=None, field=None, auth_token=None):
        api = f"{self.base_uri}/{uri}"
        if (auth_token):
            self.session.headers.update({'Authorization'  : '{auth_token}'})
        response = self.session.get(api, verify=False)
        if(auth_token):
            if not response.ok or json.dumps(response.json()).__contains__("errorMessage") :
                raise Exception(f"{uri}: {response.json()}")
        else:
            if not response.ok or response.json()['status'] == 'ERROR':
                raise Exception(f"{uri}: {response.json()}")
        result = response.json()
        if key:
            result = result[key]
        if field:
            result = result[field]
        return result

    def _post(self, uri, data=None, key=None):
        api = f"{self.base_uri}/{uri}"
        response = self.session.post(api, json=data, verify=False)
        if("Authorization" in self.session.headers.keys() or uri=="masking/api/login"):
            if not response.ok or "errorMessage" in response.json():
                raise Exception(f"{uri}: {response.json()}\ndata: {data} \n code : {response.status_code}")
        else:
            if not response.ok or response.json()['status'] == 'ERROR':
                raise Exception(f"{uri}: {response.json()}\ndata: {data}")
        return response
    
    def filter_by_string(self, list, string, param):
        """
        for x in list:
            if(x[param]!=string):
                list
        """
        list = [s for s in list if s.get(param) == string]
        if(len(list)):
            return list
        else:
            print("No element found")
            return None

    def getLatest_snap(self, List, param):
            if(len(List)==1):
                return List
            latest = List[0]
            for x in List:
                if(parser.isoparse(latest[param]) < (parser.isoparse(x[param]))):
                    latest = List[x]
                
            return latest


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

    def login_data(self, username, password):
        uri = r"resources/json/delphix/login"
        data = {
            'type': 'LoginRequest',
            'username': username.strip(),
            'password': password.strip(),
            'target' : 'DOMAIN'
        }
        response = self._post(uri, data)
        return response
    """
    L'engin 1 è collegato ad un server LDAP, che gestisce l'autenticazione. Se viene passato semplicemente admin 
    come username, l'engine non capisce quale admin da LDAP prendere, è quindi necessario indicare un dominio di autenticazione.
    """

    def login_compliance(self, username, password):
        uri="masking/api/login"
        data = {
            'username' : username.strip(),
            'password' : password.strip()
        }
        response = self._post(uri, data)
        self.session.headers.update({'Authorization' : response.json()["Authorization"]})
        return response.json()["Authorization"]


    def capacity(self, ):
        uri = r"resources/json/delphix/capacity/consumer"
        response = self._get(uri, key="result")
        for res in response:
            yield Capacity(res)
    """
    The output of the capacity method is a generator that yields instances of the Capacity class
    The Capacity instance is then yielded to the caller, allowing the caller to iterate over the capacity information.
    """

    def database(self, reference ):
        uri = rf"resources/json/delphix/database/{reference}"
        response = self._get(uri, key="result")
        database = Database(response)
        return database

    def source(self, database_reference):
        uri = rf"resources/json/delphix/source?database={database_reference}"
        response = self._get(uri, key="result")
        for res in response:
            yield Source(res)
    """
    Sources represent external database instances outside the Delphix system. These can be linked sources 
    (which pull data into Delphix from pre-existing databases) 
    or virtual sources, which export data from Delphix to arbitrary targets.
    """

    def source_config(self, reference):
        uri = rf"resources/json/delphix/sourceconfig/{reference}"
        response = self._get(uri, key="result", field="repository")
        return response
    """
    The source config represents the dynamically discovered attributes of a source.
    per consultare le informazioni del config, Deve essere passata la referenza al config 
    retrivabile dall'api source, filtrato eventualmente per dataabse o env..
    """

    def repository(self, source_config_reference):
        uri = rf"resources/json/delphix/repository/{source_config_reference}"
        response = self._get(uri, key="result")
        repository = Repository(response)
        return repository
    """
    Source repositories are containers for SourceConfig objects. 
    Each Environment can contain any number of repositories, and repositories can contain any number of source configurations. 
    A repository typically corresponds to a database installation.
    """

    def replication(self, reference):

        start_time = datetime.now()
        uri_repExe = rf"resources/json/delphix/replication/spec/{reference}/execute"
        job_id = (self._post(uri_repExe)).json()['job']
        uri_jobId = rf"resources/json/delphix/job/{job_id}"

        uri_JobRef = r"resources/json/delphix/replication/serializationpoint/"
        # listRepJobs = self._get(uri_JobRef, key="result")
        # RepJob = self.filter_by_string(listRepJobs, rep_tag, "tag")
        # RepJob = list(filter(lambda x: x['tag']==rep_tag, listRepJobs))


        uri_SourceState = r"resources/json/delphix/replication/sourcestate"
        source_state_list = self._get(uri_SourceState, key="result")
        sourceState_rep = list(filter(lambda x: x['spec']==reference, source_state_list)) # non filtra correttamente
        sourceState_rep = [x for x in source_state_list if x['spec']==reference]
        sourceState_rep = self.filter_by_string(source_state_list, reference, "spec")


        # print(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec"))
        # print(sourceState_rep)
        # print(sourceState_rep[0]['activePoint'])
        print(rf"Preparing replication job {(self._get(uri_jobId, key="result"))['target']}...")
        while(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"]==None):
            print_process_status()
        print(rf"Waiting for replication {(self._get(uri_jobId, key="result"))['target']} to finish...")
        while(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"]!=None):
            print_process_status()
        end_time = datetime.now()
        print(rf"replication job {(self._get(uri_jobId, key="result"))['target']} completed in {end_time-start_time}")

        return
    
    def refresh(self, vdb_ref, dSource_ref):
        """
        l'api refresh necessita di una referenza allo snapshot da cui prednere i dati del refresh.
        L'unica soluzione al momento è prendere una lista di snapshot(passando come param una referenza al container contenent il dsource creato dalla replica) e filtrare per reference al container del vdb provisionato con lo snapshot.
        Confrontare i timestamp e prendere quello con timestamp più recente.
        """
        """
        Altro modo, tramite api /resources/json/delphix/database/{ref} si risale ai dati del database contenente il dSource, tra cui il current timeflow da cui si può risalire al parent snapshot e la sua referenza
        Problema --> lo snapshot effettuato alla creazione della replica, non è referenziato nel timeflow -->provare a fare uno snapshot per 
        """
        start_time = datetime.now()
        uri_snap = rf"resources/json/delphix/capacity/snapshot"

        # uri_vdb = rf"/resources/json/delphix/database/{vdb_ref}"
        # uri_dSource = rf"/resources/json/delphix/database/{dSource_ref}"


        snaps = self.filter_by_string(self._get(uri_snap, key="result"), dSource_ref, "container")
        # l'api ...capacity/snapshot ritorna le snapshot ordinate decrescentmente per grandezza snapshot, 
        # quindi l'ultima è quella più recente, filtrare con met sotto più sicuro
        # latestSnap_vdb = (self.getLatest_snap(snaps, "snapshotTimestamp"))['snapshot']

        # print(self._get(uri_snap, key="result"))

        snap_ref = snaps[0]['snapshot']

        uri_refresh = rf"resources/json/delphix/database/{vdb_ref}/refresh"
        data = {
       "type": "OracleRefreshParameters",
       "timeflowPointParameters": {
                "type": "TimeflowPointSnapshot",
                "snapshot": snap_ref
        }
	    # "username": "admin",
	    # "credential": {
		# "type": "PasswordCredential",
		# "password": "delphix"
        
	    }
        job_id = (self._post(uri_refresh, data)).json()['job']
        uri_jobId = rf"resources/json/delphix/job/{job_id}"
        print(rf"Waiting for refresh of {vdb_ref} to finish...")
        while(self._get(uri_jobId, key="result")['jobState']!="COMPLETED"):
            print_process_status()
        end_time = datetime.now()
        print(f"refresh of {vdb_ref} completed in {end_time-start_time}")

        return

    def mask(self, jobId):

        start_time = datetime.now()
        uri="masking/api/executions"

        data={ 
            "jobId": jobId 
        }

        execId = self._post(uri, data).json()['executionId']
        uriExec = rf"masking/api/executions/{execId}"
        
        print(rf"Waiting for control to finish...")
        while(self._get(uriExec, key="status")=="RUNNING"):
            print_process_status()
        if(self._get(uriExec, key="status")=="SUCCEEDED"):
            end_time = datetime.now()
            print(rf"Control completed successfully in {end_time-start_time}")
        elif(self._get(uriExec, key="status")=="FAILED"): 
            print(rf"Error encountered during Control")

        return


if __name__ == "__main__":
    main()





