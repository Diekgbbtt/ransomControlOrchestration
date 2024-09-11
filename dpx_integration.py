# import time - possible with time to get current date_time and parse data
import requests
# import subprocess - for future parallel support
import smtplib
import urllib3
import json
import os

import threading

from chardet import detect
from dateutil import parser
from datetime import datetime
from email.message import EmailMessage
import oracledb
import zipfile
# It is the renamed, new major release of the popular cx_Oracle driver.



urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # needed to suppress warnings
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

class Breakdown(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Runtime(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Sourcingpolicy(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class SourceConfig(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


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


class Operations(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Ingestionstrategy(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Configparams(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Runtimemountinformation(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Parameters(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Preprovisioningstatus(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Sourceappspassword(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Sourcewlspassword(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Services(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Appspassword(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


class Wlspassword(ApiObject):
    def __init__(self, dictionary):
        super().__init__(dictionary)


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


    with open('config.json', 'r') as cfg:
        cfg_dict = json.load(cfg)
    
    

    for rep in cfg_dict.get('Replications'):

       
    
        """
        engine_1 = DelphixEngine(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['host'])
        engine_1.create_session(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['apiVersion'])
        engine_1.loginData(cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('source_engines').get(rep['sourceEngineRef'])['pwd'])
        engine_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['host'])
        engine_13.create_session(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['apiVersion'])
        engine_13.loginData(cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('vault_engines').get(rep['vaultEngineRef'])['pwd'])
        engineCompl_13 = DelphixEngine(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['host'])
        engineCompl_13.loginCompliance(cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['usr'], cfg_dict.get('dpx_engines').get('discovery_engines').get(rep['discEngineRef'])['pwd'])

        engine_1.replication(rep['replicationSpec'])
        engine_13.refresh(rep['vdbContainerRef'], rep['dSourceContainer_ref'])
    

        engineCompl_13.mask(rep['jobId'])

        # discrepantValues = evaluate(cfg_dict.get('vdbs_control').get(rep['vdbRef'])['host'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['port'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['usr'], cfg_dict.get('vdbs_control').get(rep['vdbRef'])['pwd'],  cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid'] if cfg_dict.get('vdbs_control').get(rep['vdbRef'])['sid']!=None else None)

        """
        # if(discrepantValues):
        
        reportPath = createReport() # zip the new discrepancies file
        sendAlert(cfg_dict.get('mail')['smtpServer'], cfg_dict.get('mail')['usr'], cfg_dict.get('mail')['pwd'], reportPath, cfg_dict.get('mail')['usr'], rep['mailReceivers'])


def createReport():
    # reportPath=(f"Evaluation/discrepancies{(datetime.now()).strftime("%d_%m_%Y-%H_%M_%S")}.txt") #.replace(" ", "_").replace(":", "_")
    
    test_file_path = "Evaluation\discrepancies_email_test" + (datetime.now()).strftime('%d_%m_%Y-%H_%M_%S') + ".txt"
    flags = os.O_CREAT | os.O_WRONLY  # Create file if it doesn't exist, open for writing
    mode = 0o666  # Permissions for the file
    fd = os.open(test_file_path, flags, mode)
    os.write(fd, b"TEST - discrepancies test")
    os.close(fd)
    # reportPath = "Evaluation\discrepancies_email_test.txt"
    """
    for row in discrepantValues:
        with open(reportPath, "w") as report:
            report.write(rf"descrepancy revealed in database {row[0]} table {row[1]} column {row[2]}. Value {row[3]} has {row[4] if row[4] is not None else 0} occurrences while the expected occurrences are {row[5]}")
    """
    return zipReport(test_file_path)

def zipReport(reportPath):
    zipPath = reportPath.replace(".txt", ".zip")
    with zipfile.ZipFile(zipPath, "x") as zip:
        zip.write(reportPath, compresslevel=9)
    os.remove(reportPath)
    return zipPath

def sendAlert(domain, username, pwd, filePath, sender, receivers):
        """
        An SMTP instance encapsulates an SMTP connection. It has methods that support a full repertoire of SMTP and ESMTP operations. 
        If the optional host and port parameters are given, the SMTP connect() method is called with those parameters during initialization.
        """
        with smtplib.SMTP(domain) as mailServer:
        # https://docs.python.org/3/library/smtplib.html#smtplib.SMTP
            mailServer.starttls()
            mailServer.login(username, pwd)
            content = addContent(filePath)
            print(receivers)
            mailServer.send_message(content, sender, receivers)
        # mailServer.close()
        
        return

def addContent(filePath):
        alert = EmailMessage()
        alert[""]=f"Ransomware attack detected : Start Fast Recovery"
        alert["Content-Type"]=f"text/plain" # multipart/mixed         
        alert.set_content("""
            Dear Administrator,

            Our system has detected a potential ransomware attack on the application.
            Immediate action is required to prevent data loss and further damage.
            
            Detected at: -- aggiungi data in qualche modo
            In the attacche document you can find further information regarding data discrepancies detected.                      
            Please investigate the issue promptly and take necessary measures to mitigate the attack.
            Start Fast delphix Recovery process of the affected Databases.

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

        with open(filePath, 'rb' ) as zip_file: # encoding=get_encoding_type(filePath)
            """
            devo zipare tutta la directory evaluation. Nel momento della rilevazione di un attacco ransomware dovrei backuppare in un altra cartella zippata tutti i 
            discrepancies zip file più vecchi di 7 giorni. In evaluation rimangono quindi solo i file con data < di 7 giorni, ciascun file tranne l'ultimo creato all'attuale
            esecuzione sono zippati, quindi si zippa quest'ultimo e si zippa tutta Evaluation, si aggiunge alla mail con add_attachment e si cancella lo zip di Evaluation.
            """

            # os.chdir(path) - change dir to evaluation when adding zip files to the attachment
            alert.add_attachment(zip_file.read(), maintype="application", subtype="zip", filename=filePath[11:])
        """
        If the message is a non-multipart, multipart/related, or multipart/alternative, call make_mixed() and then
        create a new message object, pass all of the arguments to its set_content() method, and attach() it to the multipart
        """
        # https://docs.python.org/3/library/email.message.html#email.message.EmailMessage.add_attachment

        print(alert.is_multipart())
        for part in alert.walk():
            print(part.get_content_type())
            print(part.get_body())

        attachs = alert.iter_attachments()
        for a in attachs:
            print(' \n \n attachments: \n')
            print(a.get_content_type())
            print(a.get_content_disposition())
            print(a.get_filename())
            print(a.get_content())

        return alert

def get_encoding_type(file):
    with open(file, 'rb') as f:
        rawdata = f.read()
    return detect(rawdata)['encoding']



def evaluate(db_hostname, db_port, username, pwd, sid):
    
    conn = oracledb.connect(host=db_hostname, port=db_port, user=username, password=pwd, sid=sid)
    curs = conn.cursor()

    rs = curs.execute("SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO FROM ( \
                    SELECT DB_NAME, TABLE_NAME, COLUMN_NAME, VALUE, result, RES_ATTESO, \
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

    def loginData(self, username, password):
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

    def loginCompliance(self, username, password):
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

        uri_repExe = rf"resources/json/delphix/replication/spec/{reference}/execute"
        job_id = (self._post(uri_repExe)).json()['job']
        uri_jobId = rf"resources/json/delphix/job/{job_id}"

        uri_JobRef = r"resources/json/delphix/replication/serializationpoint/"
        # listRepJobs = self._get(uri_JobRef, key="result")
        # RepJob = self.filter_by_string(listRepJobs, rep_tag, "tag")
        # RepJob = list(filter(lambda x: x['tag']==rep_tag, listRepJobs))


        uri_SourceState = r"resources/json/delphix/replication/sourcestate"
        sourceStateList = self._get(uri_SourceState, key="result")
        sourceState_rep = list(filter(lambda x: x['spec']==reference, sourceStateList)) # non filtra correttamente
        sourceState_rep = [x for x in sourceStateList if x['spec']==reference]
        sourceState_rep = self.filter_by_string(sourceStateList, reference, "spec")


        # print(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec"))
        # print(sourceState_rep)
        # print(sourceState_rep[0]['activePoint'])
        while(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"]==None):
            print(rf"Preparing replication job {(self._get(uri_jobId, key="result"))['target']}...")
        while(self.filter_by_string(self._get(uri_SourceState, key="result"), reference, "spec")[0]["activePoint"]!=None):
            print(rf"Waiting for replication {(self._get(uri_jobId, key="result"))['target']} to finish...")
        print(rf"replication job {(self._get(uri_jobId, key="result"))['target']} completed")
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
        uri_snap = rf"resources/json/delphix/capacity/snapshot"

        # uri_vdb = rf"/resources/json/delphix/database/{vdb_ref}"
        # uri_dSource = rf"/resources/json/delphix/database/{dSource_ref}"


        snaps = self.filter_by_string(self._get(uri_snap, key="result"), dSource_ref, "container")
        # l'api ...capacity/snapshot ritorna le snapshot ordinate decrescentmente per grandezza snapshot, 
        # quindi l'ultima è quella più recente, filtrare con met sotto più sicuro
        # latestSnap_vdb = (self.getLatest_snap(snaps, "snapshotTimestamp"))['snapshot']

        # print(self._get(uri_snap, key="result"))

        snap_ref = snaps[0]['snapshot']

        print(snap_ref)

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

        while(self._get(uri_jobId, key="result")['jobState']!="COMPLETED"):
            print(rf"Waiting for refresh of {vdb_ref} to finish...")
            # print(self._get(uri_jobId, key="result"))['events'][len(self._get(uri_jobId, key="result")['events'])-1]['messageDetails']
        print(rf"refresh of "+ vdb_ref +" completed")

        return

    def mask(self, jobId):

        uri="masking/api/executions"

        data={ 
            "jobId": jobId 
        }

        execId = self._post(uri, data).json()['executionId']
        uriExec = rf"masking/api/executions/{execId}"

        while(self._get(uriExec, key="status")=="RUNNING"):
            print(rf"Waiting for control to finish...")
        if(self._get(uriExec, key="status")=="SUCCEEDED"):
            print(rf"Control completed successfully")
        elif(self._get(uriExec, key="status")=="FAILED"): 
            print(rf"Error encountered during Control")

        return
            

if __name__ == "__main__":
    main()





