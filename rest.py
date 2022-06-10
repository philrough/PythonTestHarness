import json
import requests
from requests.auth import HTTPBasicAuth
from requests_ntlm import HttpNtlmAuth

from test_harness import log

AUTH = 'HttpNtlmAuth' # or 'HTTPBasicAuth'

class RestClient:
    def __init__(self, username, password, logLevel):
        self.logger = log.get_logger(self.__class__.__name__)
        self.logger.setLevel(logLevel)
        self.username = username
        self.password = password
        self.auth = HTTPBasicAuth(self.username, self.password)

    def requests_get(self, url, params=None, headers=None):
        self.logger.info("Requesting {} | params: {} | headers: {} | auth {} : {}".format(url, params, headers, self.username, self.password))
        if AUTH == 'HttpNtlmAuth':
            return requests.get(url, params=params, headers=headers, auth=HttpNtlmAuth(self.username, self.password))
        elif AUTH == 'HTTPBasicAuth':
            return requests.get(url, params=params, headers=headers)
        else :
            self.logger.info("Error - Authorisation method incorrect : {}".format(AUTH))

    def secure_requests_get(self, url, params=None, headers=None):
        self.logger.info("Requesting {} | params: {} | headers: {}".format(url, params, headers))
        return requests.get(url, params=params, headers=headers, verify=False)
        
    def requests_post(self, url, data, headers, params=None):
        self.logger.info("Posting {} to {} | params: {} | headers: {} | auth {} : {}".format(data, url, params, headers, self.username, self.password))
        if AUTH == 'HttpNtlmAuth':
            return requests.post(url, data, headers=headers, auth=HttpNtlmAuth(self.username, self.password))
        elif AUTH == 'HTTPBasicAuth':
            return requests.post(url, data, headers=headers)
        else :
            self.logger.info("Error - Authorisation method incorrect : {}".format(AUTH))

    def secure_requests_post(self, url, data, headers, params=None):
        self.logger.info("Secure Posting {} to {} | params: {} | headers: {}".format(data, url, params, headers))
        return requests.post(url, data, headers=headers, verify=False)

    def requests_put(self, url, data, headers, params=None):
        self.logger.info("Putting {} to {} | params: {} | headers: {} | auth {} : {}".format(data, url, params, headers, self.username, self.password))
        if AUTH == 'HttpNtlmAuth':
            return requests.put(url, data, headers=headers, auth=HttpNtlmAuth(self.username, self.password))
        elif AUTH == 'HTTPBasicAuth':
            return requests.put(url, data, headers=headers)
        else :
            self.logger.info("Error - Authorisation method incorrect : {}".format(AUTH))
            
    def requests_delete(self, url, data, headers, params=None):
        self.logger.info("Deleting {} to {} | params: {} | headers: {} | auth {} : {}".format(data, url, params, headers, self.username, self.password))
        if AUTH == 'HttpNtlmAuth':
            return requests.delete(url, data=data, headers=headers, auth=HttpNtlmAuth(self.username, self.password))
        elif AUTH == 'HTTPBasicAuth':
            return requests.delete(url, data=data, headers=headers)
        else :
            self.logger.info("Error - Authorisation method incorrect : {}".format(AUTH))
            

class REST(RestClient):
    """https://gist.github.com/yashpatil/de7437522bfccfeee4cb"""

    def __init__(self, username, password, url, logLevel):
        RestClient.__init__(self, username, password, logLevel)
        self.logger = log.get_logger(REST.__name__)
        if AUTH == 'HttpNtlmAuth':
            self.base_url = "http://{}".format(url)
        elif AUTH == 'HTTPBasicAuth':
            self.base_url = "http://{}:{}@{}".format(username, password, url)
        else :
            self.logger.info("Error - Authorisation method incorrect : {}".format(AUTH))

    def consume(self, queue_name):
        return self.requests_get(self.__get_message_url(queue_name),
                                 headers={'Content-Type': 'application/json'})
                                 
    def produce(self, queue_name, message, headers):
        return self.requests_post(self.__get_message_url(queue_name), message,
                                  headers={'Content-Type': 'application/json'})
                                  
    def update(self, queue_name, message, headers):
        return self.requests_put(self.__get_message_url(queue_name), message,
                                  headers={'Content-Type': 'application/json'})
    def delete(self, queue_name, message, headers):
        return self.requests_delete(self.__get_message_url(queue_name), message,
                                  headers={'Content-Type': 'application/json'})

    def __get_message_url(self, queue_name):
        return "{}/{}".format(self.base_url, queue_name)
		
class REST_VPMREPORTING(RestClient):

    def __init__(self, username, password, url, epcode, logLevel):
        RestClient.__init__(self, username, password, logLevel)
        self.logger = log.get_logger(REST.__name__)
        self.base_url = "https://{}".format(url)
        self.epcode = epcode
        self.username = username
        self.password = password

    def consume(self, queue_name):
        return self.requests_get(self.__get_message_url(queue_name),
                                 headers={'Content-Type': 'application/json'})

    def produce(self, queue_name, message, headers):
        return self.secure_requests_post(self.__get_message_url(queue_name), self.__get_lookup_data(message),
                                  headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'bearer ' + self.__get_token()})
                                  
    def __get_message_url(self, queue_name):
        return "{}/{}".format(self.base_url, queue_name)
    
    def __get_token(self):     
        tokenbody = "grant_type=password&login_type=Windows&ep_code={}&username={}&password={}".format(self.epcode, self.username, self.password)
        token_response = self.secure_requests_post(self.__get_message_url("VPMReporting/api/token"), tokenbody,
                                  headers={'Accept': 'application/json'})
        return (json.loads(token_response.text))["access_token"]
    
    def __get_lookup_data(self, message):
        json_body = json.loads(message)
        #self.logger.debug("json_body -  {}".format(json_body))
        strategyid = self.secure_requests_get(self.__get_message_url('VPMReporting/api/referencedata/search?operator=contains&q=' + json_body['SnId'] + '&type=strategy'),
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'bearer ' + self.__get_token()})
        symbolid = self.secure_requests_get(self.__get_message_url('VPMReporting/api/referencedata/search?operator=contains&q=' + json_body['SyId'] + '&type=sylookup'),
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json', 'Authorization': 'bearer ' + self.__get_token()})
        json_strategyid = json.loads(strategyid.text)
        json_symbolid = json.loads(symbolid.text)
        json_body['SnId'] = json_strategyid[0]['id']
        json_body['SyId'] = json_symbolid[0]['id']
        #self.logger.debug("json_body -  {}".format(json_body))
        return json.dumps(json_body)
