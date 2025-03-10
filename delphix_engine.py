

from requests import packages, Session, RequestException
from typing import Dict, Optional, Union, Any, List
from datetime import datetime
from progress.bar import IncrementalBar


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
    def _post(self, uri: str, data: Optional[Dict] = None, key: Optional[str] = None) -> Dict:

        api = f"{self.base_uri}/{uri}"
        try:
            response = self.session.post(api, json=data, verify=False)
        except RequestException as e:
            raise RequestException(response=e.response)
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
    def get_latest_snap(self, param: str) -> Dict:

        if len(List) == 1:
            return List[0]
        
        latest = List[0]
        for x in List:
            if datetime.strptime(latest[param], '%Y-%m-%dT%H:%M:%S.%fZ') < datetime.strptime(x[param], '%Y-%m-%dT%H:%M:%S.%fZ'):
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
            raise Exception(f"error creating session version {api_version} with engine {self.ip}, bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(f"error creating session version {api_version} with engine {self.ip} \n Error : {str(e) if str(e) else e}")

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
            raise Exception(f"error logging in data engine {self.ip}, bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(f"error logging in data engine {self.ip} \n Error : {str(e) if str(e) else e}")

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
            raise Exception(f"error logging in compliance engine {self.ip}, Bad request likely. Response received {e.response}")
        except Exception as e:
            raise Exception(f"error logging in compliance engine \n Error : {str(e) if str(e) else e}")

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
            raise Exception(f"error executing replication job, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(f"error executing replication job \n Error : {str(e) if str(e) else e}")

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
                raise Exception(f"error executing vdb refershing job, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(f"error executing vdb refershing job \n Error : {str(e) if str(e) else e}")
    
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
                raise Exception(f"values control job with discovery engine failed. Check delphix dashboard")
        except RequestException as e:
                raise Exception(f"error executing masking job to check values, Bad request likely. Response received {e.response}")
        except Exception as e:
                raise Exception(f"error executing masking job to check values \n Error : {str(e) if str(e) else e}")
