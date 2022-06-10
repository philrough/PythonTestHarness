import os
import re
import configparser
import lxml.etree as et
import datetime
import json


from behave import *
from hamcrest import assert_that, is_not, equal_to, less_than, greater_than, greater_than_or_equal_to, less_than_or_equal_to, contains_string
from test_harness.log import get_logger, configure_stdout_logger
from random import seed
from random import randint
from datetime import timezone

IgnorePlaceValues = False

SCRIPT_LOCATION = os.path.dirname(__file__)
CONFIG_LOCATION = os.path.join(SCRIPT_LOCATION, "../../", "test_harness.cfg")
SCENARIO_FILES_RELATIVE_LOCATION = os.path.join(SCRIPT_LOCATION, "../")

conf = configparser.ConfigParser()
conf.read(CONFIG_LOCATION)

configure_stdout_logger(conf['logging']['level'])
LOGGER = get_logger(os.path.basename(__file__))

#LOGGER.debug('Debug Enabled')
#LOGGER.info('Info Enabled')
#LOGGER.warning('Warning Enabled')
#LOGGER.error('Error Enabled')
#LOGGER.critical('Critical Enabled')

MESSAGES = {}

DATE = None
DATETIME = None
DATETIME2 = None
DATETIME3 = None


def get_static_data():
    global DATE
    if DATE is None:
        DATE = datetime.datetime.now().strftime("%Y-%m-%d")
    global DATETIME
    if DATETIME is None:
        DATETIME = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
    global DATETIME2
    if DATETIME2 is None:
        DATETIME2 = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f"+'2')
    global DATETIME3
    if DATETIME3 is None:
        DATETIME3 = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

get_static_data()

def symbol_replace(value):
    if ('#date#' in value):
        #LOGGER.info("Symbol #date# replaced with {}".format(DATE))
        return value.replace('#date#', DATE)
    elif ('#datetime#' in value):
        #LOGGER.info("Symbol #datetime# replaced with {}".format(DATETIME))
        return value.replace('#datetime#', DATETIME)
    elif ('#datetime3#' in value):
        #LOGGER.info("Symbol #datetime3# replaced with {}".format(DATETIME3))
        return value.replace('#datetime3#', DATETIME3)   
    elif ('#uniqueid#' in value):
        #LOGGER.info("Symbol #uniqueid# replaced with {}".format(UNIQUEID))
        return value.replace('#uniqueid#', UNIQUEID)
    elif ('#uniquever#' in value):
        #LOGGER.info("Symbol #uniquever# replaced with {}".format(UNIQUEVER))
        return value.replace('#uniquever#', UNIQUEVER)
    elif ('#tradeid#' in value):
        #LOGGER.info("Symbol #tradeid# replaced with {}".format(TRADEID))
        return value.replace('#tradeid#', TRADEID)
    elif ('#dealid#' in value):
        #LOGGER.info("Symbol #dealid# replaced with {}".format(DEALID))
        return value.replace('#dealid#', DEALID)
    elif ('#symbolid#' in value):
        #LOGGER.info("Symbol #symbolid# replaced with {}".format(SYMBOLID))
        return value.replace('#symbolid#', SYMBOLID)
    elif ('#strategyid#' in value):
        #LOGGER.info("Strategy #strategyid# replaced with {}".format(STRATEGYID))
        return value.replace('#strategyid#', STRATEGYID)
    elif ('#brokerid#' in value):
        #LOGGER.info("Broker #brokerid# replaced with {}".format(BROKERID))
        return value.replace('#brokerid#', BROKERID)
    elif ('#executingbrokerid#' in value):
        #LOGGER.info("Broker #executingbrokerid# replaced with {}".format(EBROKERID))
        return value.replace('#executingbrokerid#', EBROKERID)
    elif ('#securitytypeid#' in value):
        #LOGGER.info("SecurityType #securitytypeid# replaced with {}".format(SECURITYTYPEID))
        return value.replace('#securitytypeid#', SECURITYTYPEID)
    else:
        return (value)
        
def set_tradeid(data, test_name):
    global TRADEID
    TRADEID = ''
    if test_name == 'batch_tradeids':
        TRADEID = data
        return (TRADEID)
    for row in data:
        col = row.split(",")
        if len(row) == 0:
            continue
        if (col[0] == test_name):
            TRADEID = col[1]
            break
    return (TRADEID)

def set_uniqueid():
    global UNIQUEID
    UNIQUEID = ''
    UNIQUEID = datetime.datetime.now().strftime("%m%d%M%S%f")  
    seed() 
    UNIQUEID += str(randint(1,99))
    return (UNIQUEID)
    
def set_uniquever():
    global UNIQUEVER
    UNIQUEVER = ''
    UNIQUEVER = datetime.datetime.now().strftime("%m%d%H%M%S")  

    return (UNIQUEVER)
    
def set_parameter(parameter_name, parameter_value):
    if parameter_name == "dealid":
        global DEALID
        DEALID = ''
        DEALID = parameter_value
    elif parameter_name == "symbolid":
        global SYMBOLID
        SYMBOLID = ''
        SYMBOLID = parameter_value
    elif parameter_name == "strategyid":
        global STRATEGYID
        STRATEGYID = ''
        STRATEGYID = parameter_value
    elif parameter_name == "brokerid":
        global BROKERID
        BROKERID = ''
        BROKERID = parameter_value
    elif parameter_name == "executingbrokerid":
        global EBROKERID
        EBROKERID = ''
        EBROKERID = parameter_value
    elif parameter_name == "securitytypeid":
        global SECURITYTYPEID
        SECURITYTYPE = ''
        SECURITYTYPEID = parameter_value
    
def parse_xml_string(xml_str):
    return et.fromstring(xml_str)

def string_to_xml(xml_tree):
    return et.tostring(xml_tree).decode()

def parse_json_string(json_str):
    json_str = json_str.replace('true', 'True').replace('false', 'False')
    return (json_str)

def parse_text_string(txt_str):
    txt_str = txt_str.replace('\\r', '\r').replace('\\n', '\n')
    return(txt_str)

def xml_element_find(xml_tree, xpath_expr):
    #LOGGER.debug("xml_tree {}".format(xml_tree))
    #LOGGER.debug("xpath_expr {}".format(xpath_expr))   
    m = re.match(r"(.*) \[(\d+)\]", xpath_expr)
    if m:
        expr, occurance = m.groups()
        occurance = int(occurance)
        elems = xml_tree.findall(expr, xml_tree.nsmap)
        assert_that(occurance, less_than(len(elems)),
                    "Occurrence of {} was set to {} but there were {} elements found: {}".format(xpath_expr,
                                                                                                 occurance,
                                                                                                 len(elems), elems))
        return elems[occurance]
    else:
        return xml_tree.find(xpath_expr, xml_tree.nsmap)

def find(key, dictionary):
    #LOGGER.info("key {}".format(key))
    #LOGGER.debug("dictionary {}".format(dictionary))

    if not isinstance(dictionary, (str, int)):
        for k, v in dictionary.items():
            if k == key:
                yield v
    else:
        yield dictionary
        
def find_item(key, dictionary, item):
    #LOGGER.info("key {}".format(key))
    #LOGGER.info("item {}".format(item))
    #LOGGER.debug("dictionary item {}".format(dictionary[item]))
    #LOGGER.debug("dictionary items {}".format(dictionary[item].items()))
   
    if isinstance(item, str):
        subitem = item.split("/")
        if len(subitem) == 1:
            try:
                dictdata = dictionary[subitem[0]].items()
            except KeyError:
                dictdata = ''
        elif len(subitem) == 2:
            try:
                dictdata = dictionary[subitem[0]][subitem[1]].items()
            except KeyError:
                dictdata = ''
        elif len(subitem) == 3:   
            try:
                dictdata = dictionary[subitem[0]][subitem[1]][subitem[2]].items()
            except KeyError:
                dictdata = ''
    else:
        dictdata = dictionary[item].items()
        
    for k, v in dictdata:
        if k == key:
            yield v
        elif isinstance(v, dict):
            for result in find(key, v):
                yield result
        elif isinstance(v, list):
            for d in v:
                for result in find(key, d):
                    yield result

def json_element_find(json_tree, json_expr):
    if (len(json_tree) == 0):
            return 'Error: Received response is empty' 
    m = re.match(r"(.*) \[(\d+)\]", json_expr)
    if m:
        expr, occurance = m.groups()
        occurance = int(occurance)

        elems = list(find(json_expr, eval(json_tree)))
        assert_that(occurance, less_than(len(elems)),
                    "Occurrence of {} was set to {} but there were {} elements found: {}".format(json_expr,
                                                                                                occurance,
                                                                                                 len(elems), elems))
        return elems[occurance]
    else:
        elems = list(find(json_expr, eval(parse_json_string(json_tree))))
        if (len(elems) == 0):
            return 'Expected json element (' + json_expr + ') not found'
        else:
            return str(elems[0])

def json_item_element_find(json_tree, json_expr, json_item):
    m = re.match(r"(.*) \[(\d+)\]", json_expr)
    if m:
        expr, occurance = m.groups()
        occurance = int(occurance)

        elems = list(find_item(json_expr, eval(json_tree), json_item))
        assert_that(occurance, less_than(len(elems)),
                    "Occurrence of {} was set to {} but there were {} elements found: {}".format(json_expr,
                                                                                                occurance,
                                                                                                 len(elems), elems))
        return elems[occurance]
    else:
        elems = list(find_item(json_expr, eval(parse_json_string(json_tree)), json_item))
        if (len(elems) == 0):
            return 'Expected json element (' + json_expr + ') not found'
        else:
            return str(elems[0])
            
def jsonSort(rest_response):
        response_sortedby_key = json.dumps(rest_response.json(), indent=4, sort_keys=True) # Additional sorting to support variable pnl data
        response_sortedby_col = sorted(json.loads(response_sortedby_key), key=lambda k: (k["nav"], k["qty"], k["brokerAccount"], k["investmentDescription"], k["extendedInvestmentDescription"], k["fxRate"]))
        return json.dumps(response_sortedby_col)

class XPathReplacer:
    def __init__(self, xml_str):
        self.xml_str = xml_str

    def replace_xpath_text_field(self, xpath_element, change_to):
        xml_t = parse_xml_string(self.xml_str)
        el = xml_element_find(xml_t, xpath_element)

        assert_that(el, is_not(None),
                    "Expected xml element {} not found for value replacement to {}".format(xpath_element, change_to))
        el.text = change_to
        return string_to_xml(xml_t)

    def replace_xpath_attr(self, xpath_element, xml_attribute, change_to):
        xml_t = parse_xml_string(self.xml_str)
        el = xml_element_find(xml_t, xpath_element)

        assert_that(el, is_not(None),
                    "Expected xml element {} not found for {} attribute replacement to {}".format(xpath_element,
                                                                                                  xml_attribute,
                                                                                                  change_to))
        el.attrib[xml_attribute] = change_to
        return string_to_xml(xml_t)


class XPathVerifier:
    def __init__(self, xml_str):
        self.xml_str = xml_str

    @staticmethod
    def strip_namespace(text):
        return [g for g in re.match(r"^({.+})?(.*)", text.strip()).groups() if g is not None][-1]

    def find_attrib(self, element, attrib):
        for attr in element.attrib:
            if self.strip_namespace(attr) == attrib:
                return attr

    def __find_element(self, xpath_element):
        xml_t = parse_xml_string(self.xml_str)
        return xml_element_find(xml_t, xpath_element)

    def verify_text_field(self, xpath_element, expected_value):
        el = self.__find_element(xpath_element)

        if IgnorePlaceValues:
            el = el.split('.')[0]
            expected_value = expected_value.split('.')[0]

        assert_that(el, is_not(None), "Expected xml element {} not found".format(xpath_element))
        assert_that(el.text, is_not(None), "Expected xml element {} had no text fields".format(xpath_element))
        assert_that(el.text.strip(), equal_to(expected_value), "Expected text field {} mismatch".format(expected_value))

    def verify_attribute(self, xpath_element, xpath_attrib, expected_value):
        el = self.__find_element(xpath_element)

        assert_that(el, is_not(None),
                    "Expected xml element {} with attribute {} not found".format(xpath_element, xpath_attrib))

        attr_with_namespace = self.find_attrib(el, xpath_attrib)

        assert_that(el.attrib[attr_with_namespace], equal_to(expected_value),
                    "Expected attribute {} mismatch".format(xpath_attrib))

    def verify_root_tag_name(self, expected_tag_name):
        el = parse_xml_string(self.xml_str)
        tag_without_namespace = self.strip_namespace(el.tag)
        assert_that(tag_without_namespace, equal_to(expected_tag_name),
                    "Expected root tag {} mismatch".format(tag_without_namespace))


class JSONReplacer:
    def __init__(self, json_str):
        self.json_str = json_str

    def replace_json_text_field(self, json_element, change_to):
        json_t = self.json_str
        json_tree = eval(self.json_str)
        el = json_element_find(json_t, json_element)
        assert_that(el, is_not(None),
                    "Expected json element {} not found for value replacement to {}".format(json_element, change_to))
   
        json_t = json_t.replace(el, change_to)
        json_t = json_t.replace("'", "\"")
        return (json_t)
        
    def replace_json_item_text_field(self, json_element, json_item, change_to):

        json_t = self.json_str
        json_tree = eval(self.json_str)
        el = json_item_element_find(json_t, json_element, json_item)
        assert_that(el, is_not(None),
                    "Expected json element {} not found for value replacement to {}".format(json_element, change_to))
        json_t = json_t.replace(el, change_to)
        return (json_t)


class JSONVerifier:
    def __init__(self, json_str):
        self.json_str = json_str
        
    @staticmethod
    def strip_namespace(text):
        return [g for g in re.match(r"^({.+})?(.*)", text.strip()).groups() if g is not None][-1]

    def __find_element(self, json_element):
        json_t = self.json_str
        return json_element_find(json_t, json_element)

    def verify_text(self, expected_value):
        json_t = self.json_str
        if re.search('>', expected_value):
            assert_that(json_t, greater_than_or_equal_to(expected_value[1:]), "Expected text field {} mismatch".format(expected_value))
        elif re.search('<', expected_value):
            assert_that(float(json_t), less_than_or_equal_to(float(expected_value[1:])), "Expected text field {} mismatch".format(expected_value))
        elif re.search('&', expected_value):
            expected_value = expected_value.split('&')[0]
            assert_that(json_t, contains_string(expected_value), "Expected text field {} not found".format(expected_value))
        else:
            assert_that(json_t, equal_to(expected_value), "Expected text field {} mismatch".format(expected_value))
        
    def verify_text_field(self, json_element, expected_value):
        json_t = self.json_str
        el = json_element_find(json_t, json_element)
        if IgnorePlaceValues:
            el = el.split('.')[0]
            expected_value = expected_value.split('.')[0]
        if re.search('#ignore#', expected_value):
            expected_value = expected_value.replace('#ignore#', '')
            assert_that(json_t, contains_string(expected_value), "Expected text field {} not found".format(expected_value))
        else:
            assert_that(el, is_not(None), "Expected json element {} had no text fields".format(json_element))
            assert_that(el, equal_to(expected_value), "Expected text field {} value {} mismatch".format(json_element, expected_value))
  
    def store_text_field(self, json_element, parameter_name):
        json_t = self.json_str
        el = json_element_find(json_t, json_element)
        #LOGGER.info("parameter_name {}".format(parameter_name))
        #LOGGER.info("parameter_value {}".format(el))
        assert_that(el, is_not(None), "Expected json element {} not found".format(json_element))
        
    def verify_item_text_field(self, json_element, json_item, expected_value):
        json_t = self.json_str
        el = json_item_element_find(json_t, json_element, json_item)
        
        if IgnorePlaceValues:
            el = el.split('.')[0]
            expected_value = expected_value.split('.')[0]

        if re.match('>', expected_value):
           assert_that(el, greater_than_or_equal_to(expected_value[1:]), "Expected text field {} value {} mismatch".format(json_element, expected_value))
        else:
           assert_that(el, is_not(None), "Expected json element {} had no text fields".format(json_element))
           assert_that(el, equal_to(expected_value), "Expected text field {} value {} mismatch".format(json_element, expected_value))
           
    def store_item_text_field(self, json_element, json_item, parameter_name):
        json_t = self.json_str
        el = json_item_element_find(json_t, json_element, json_item)
        #LOGGER.info("parameter_name {}".format(parameter_name))
        #LOGGER.info("parameter_value {}".format(el))
        set_parameter(parameter_name, el)
