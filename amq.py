import json
import requests
from requests.auth import HTTPBasicAuth

from test_harness import log


class RestClient:
    def __init__(self, username, password, logLevel):
        self.logger = log.get_logger(self.__class__.__name__)
        self.logger.setLevel(logLevel)
        self.auth = HTTPBasicAuth(username, password)

    def requests_get(self, url, params=None, headers=None):
        self.logger.debug("Requesting {} | params: {} | headers: {}".format(url, params, headers))
        return requests.get(url, auth=self.auth, params=params, headers=headers)

    def requests_post(self, url, data, params=None, headers=None):
        self.logger.debug("Posting [{}] to {} | params: {} | headers: {}".format(data, url, params, headers))
        return requests.post(url, data=data, auth=self.auth)


class AMQ(RestClient):
    """https://gist.github.com/yashpatil/de7437522bfccfeee4cb"""

    def __init__(self, username, password, url, logLevel):
        RestClient.__init__(self, username, password, logLevel)
        self.logger = log.get_logger(AMQ.__name__)
        self.logger.setLevel(logLevel)
        self.base_url = "http://{}:{}@{}/api".format(username, password, url)

    def get_queue_attributes(self, queue_name, broker_name='localhost', properties=''):
        url_base = "{}/jolokia/read/".format(self.base_url)
        jmx_params = "org.apache.activemq:type=Broker,brokerName={}," \
                     "destinationType=Queue," \
                     "destinationName={}/{}".format(broker_name, queue_name, properties)
        url = "{}{}".format(url_base, jmx_params)
        resp = self.requests_get(url)
        json_resp = json.loads(resp.text)
        return json_resp['value']

    def consume(self, queue_name):
        return self.requests_get(self.__get_message_url(),
                                 params={'oneShot': 'true',
                                         'destination': 'queue://{}'.format(queue_name)})

    def produce(self, queue_name, message):
        return self.requests_post(self.__get_message_url(),
                                  {"body": message,
                                   "destination": "queue://{}".format(queue_name)})

    def __get_message_url(self):
        return "{}/message".format(self.base_url)
