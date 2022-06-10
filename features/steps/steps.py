import shutil
import glob
import time

from test_harness.amq import AMQ
from test_harness.msmq import MSMQ
from test_harness.rest import REST
from test_harness.sql import SQL
from test_harness.system import SYSTEM
from test_harness.rest import REST_VPMREPORTING
from test_harness.log import get_logger, configure_stdout_logger

from test_harness.features.steps.core import *

SCRIPT_LOCATION = os.path.dirname(__file__)
CONFIG_LOCATION = os.path.join(SCRIPT_LOCATION, "../../", "test_harness.cfg")
SCENARIO_FILES_RELATIVE_LOCATION = os.path.join(SCRIPT_LOCATION, "../")

conf = configparser.ConfigParser()
conf.read(CONFIG_LOCATION)

SCENARIO_AMQ = AMQ(conf['amq']['username'], conf['amq']['password'], conf['amq']['url'], conf['logging']['level'])
SCENARIO_MSMQ = MSMQ(conf['msmq']['username'], conf['msmq']['password'], conf['msmq']['host'], conf['msmq']['name'], conf['logging']['level'])
SCENARIO_REST = REST(conf['rest']['username'], conf['rest']['password'], conf['rest']['url'], conf['logging']['level'])
SCENARIO_SQL = SQL(conf['sql']['host'], conf['sql']['database'], conf['sql']['username'], conf['sql']['password'], conf['logging']['level'])
SCENARIO_REST_VPMREPORTING = REST_VPMREPORTING(conf['rest_vpmreporting']['username'], conf['rest_vpmreporting']['password'], conf['rest_vpmreporting']['url'], conf['rest_vpmreporting']['epcode'], conf['logging']['level'])
SCENARIO_SYSTEM = SYSTEM(conf['system']['cashflow'], conf['system']['dataload'], conf['logging']['level'])
MESSAGES = {}

class TableHeaders:
    MESSAGE_LABEL = 'message_label'
    TRADE_ID = 'trade_id'
    FILE_PATH = 'file_path'
    DESTINATION_PATH = 'destination_path'
    XML_ELEMENT = 'xml_element'
    XML_PATH = 'xml_path'
    XML_ATTRIBUTE = 'xml_attribute'
    JSON_ELEMENT = 'json_element'
    JSON_ITEM = 'json_item'
    ELEMENT_POSITION = 'element_position'
    VALUE = 'value'
    PARAMETER = 'parameter'
    TYPE = 'type'

 
@given('A set of fpml messages')
def step_impl(context):
    #set_uniqueid()
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        MESSAGES[message_type] = None
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                MESSAGES[message_type] = f.read()
        else:
            MESSAGES[message_type] = None

@given('A RestAPI message payload')
def step_impl(context):
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                MESSAGES[message_type] = f.read()
        else:
            MESSAGES[message_type] = None

@given('A batch input file')
def step_impl(context):
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                MESSAGES[message_type] = f.read().split("\n")
        else:
            MESSAGES[message_type] = None
            
@step('A tradeid read from the "{data_file}" data file')
def step_impl(context):
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                MESSAGES[message_type] = f.read().split("\n")
        else:
            MESSAGES[message_type] = None
            
@step('A tradeid read from the data file')
def step_impl(context):
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                batch_data = f.read().split("\n")
                tradeids = ''
                count = 0
                for row in batch_data:
                    count += 1
                    if len(row) > 0:
                        tradeid = row.split(",")
                        if count == 1:
                            tradeids = '[' + '\'' + tradeid[1] + '\'' + ", "
                        elif count == len(batch_data)-1:
                            tradeids = tradeids + '\'' + tradeid[1]  + '\'' + "]"
                        else:
                            tradeids = tradeids + '\'' + tradeid[1] + '\'' + ", " 
        MESSAGES[message_type] = tradeids
        set_tradeid(MESSAGES[message_type], "batch_tradeids")
            
@step('the following test fields will be read from the "{data_file}" data file')
def step_impl(context):
    for row in context.table:
        message_type = row[TableHeaders.MESSAGE_LABEL]
        file_path = row[TableHeaders.FILE_PATH]
        if file_path:
            with open(os.path.join(SCENARIO_FILES_RELATIVE_LOCATION, file_path), 'r') as f:
                MESSAGES[message_type] = f.read().split("\n")
        else:
            MESSAGES[message_type] = None
            
@step('the following XML text fields set on input messages')
def step_impl(context):
    for row in context.table:
        message_label = row[TableHeaders.MESSAGE_LABEL]
        MESSAGES[message_label] = XPathReplacer(MESSAGES[message_label]) \
            .replace_xpath_text_field(row[TableHeaders.XML_ELEMENT], symbol_replace(row[TableHeaders.VALUE]))

@step('the following XML attributes are set on input messages')
def step_impl(context):
    for row in context.table:
        message_label = row[TableHeaders.MESSAGE_LABEL]
        MESSAGES[message_label] = XPathReplacer(MESSAGES[message_label]) \
            .replace_xpath_attr(row[TableHeaders.XML_ELEMENT],
                                row[TableHeaders.XML_ATTRIBUTE],
                                row[TableHeaders.VALUE])

@step('the following JSON text fields set on input messages')
def step_impl(context):
    for row in context.table:
        message_label = row[TableHeaders.MESSAGE_LABEL]
        MESSAGES[message_label] = JSONReplacer(MESSAGES[message_label]) \
            .replace_json_text_field(row[TableHeaders.JSON_ELEMENT], symbol_replace(row[TableHeaders.VALUE]))
            
@step('the following JSON text fields per JSON item set on input messages')
def step_impl(context):
    for row in context.table:
        message_label = row[TableHeaders.MESSAGE_LABEL]
        MESSAGES[message_label] = JSONReplacer(MESSAGES[message_label]) \
            .replace_json_item_text_field(row[TableHeaders.JSON_ELEMENT], row[TableHeaders.JSON_ITEM], symbol_replace(row[TableHeaders.VALUE]))


@when('"{message_label}" is sent to "{amq_name}" AMQ')
def step_impl(context, message_label, amq_name):
    response = SCENARIO_AMQ.produce(amq_name, MESSAGES[message_label])
    #LOGGER.info("Message sent to {} with response code: {}".format(amq_name, response.status_code))
    #LOGGER.debug(response.text)

@step('"{message_label}" is received from "{amq_name}" AMQ')
def step_impl(context, message_label, amq_name):
    response = SCENARIO_AMQ.consume(amq_name)
    assert_that(response.text, is_not(None))
    assert_that(response.text, is_not(""))
    MESSAGES[message_label] = response.text
    #LOGGER.info("Message sent to {} with response code: {}".format(amq_name, response.status_code))
    #LOGGER.debug(response.text)

@when('"{message_label}" is sent to MSMQ')
def step_impl(context, message_label):
    response = SCENARIO_MSMQ.produce(MESSAGES[message_label])
    #LOGGER.info("Message sent with id: {}".format(response.id))
    #LOGGER.debug(response.body)	

@step('"{message_label}" is received from MSMQ')
def step_impl(context, message_label):
    response = SCENARIO_MSMQ.consume()
    assert_that(response.body, is_not(None))
    assert_that(response.body, is_not(""))
    MESSAGES[message_label] = response.body
    #LOGGER.info("Message received with response code: {}".format(response.id))
    #LOGGER.debug(response.body)

    
@step('"{message_label}" is sent as a DELETE request to "{rest_name}" REST')
def step_impl(context, message_label, rest_name, headers=None):
    timerStart = time.time()
    response = SCENARIO_REST.delete(rest_name, MESSAGES[message_label], headers=None)
    timerElapsed = time.time() - timerStart
    MESSAGES['RESULT_REQUEST'] = response.text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Message sent to {} with response code: {}".format(rest_name, response.status_code))
    #LOGGER.info("Message received from {} with body {}".format(rest_name, response.text))

@step('"{message_label}" is sent as a PUT request to "{rest_name}" REST')
def step_impl(context, message_label, rest_name, headers=None):
    timerStart = time.time()
    response = SCENARIO_REST.update(rest_name, MESSAGES[message_label], headers=None)
    timerElapsed = time.time() - timerStart
    MESSAGES['RESULT_REQUEST'] = response.text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Message sent to {} with response code: {}".format(rest_name, response.status_code))
    #LOGGER.info("Message received from {} with body {}".format(rest_name, response.text))

@step('"{message_label}" is sent as a POST request to "{rest_name}" REST')
def step_impl(context, message_label, rest_name, headers=None):
    timerStart = time.time()
    response = SCENARIO_REST.produce(rest_name, MESSAGES[message_label], headers=None)
    timerElapsed = time.time() - timerStart
    MESSAGES['RESULT_REQUEST'] = response.text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Message sent to {} with response code: {}".format(rest_name, response.status_code))
    #LOGGER.info("Message received from {} with body {}".format(rest_name, response.text))
	
@step('"{message_label}" is sent as a GET request to "{rest_url}" REST')
def step_impl(context, message_label, rest_url):
    timerStart = time.time()
    response = SCENARIO_REST.consume(symbol_replace(rest_url))
    timerElapsed = time.time() - timerStart
    MESSAGES[message_label] = response.text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Received response {} from {}".format(response.text, rest_url))

@step('"{message_label}" is returned from a GET request to "{rest_url}" REST')
def step_impl(context, message_label, rest_url):
    timerStart = time.time()
    response = SCENARIO_REST.consume(symbol_replace(rest_url))
    timerElapsed = time.time() - timerStart
    MESSAGES[message_label] = response.text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Received response {} from {}".format(response.text, rest_url))
    
@step('"{message_label}" is returned from a GET request to "{rest_url}" RESTPNL')
def step_impl(context, message_label, rest_url):
    timerStart = time.time()
    response = SCENARIO_REST.consume(symbol_replace(rest_url))
    timerElapsed = time.time() - timerStart
    response_text = jsonSort(response)
    MESSAGES[message_label] = response_text
    MESSAGES['STATUS_CODE'] = "{}".format(response.status_code)
    MESSAGES['STATUS_ELAPSED'] = "{}".format(timerElapsed)
    #LOGGER.info("Received response {} from {}".format(response.text, rest_url))

@step('TradeId from "{message_label}" for "{test_name}" is sent as a GET request to "{rest_url}" REST')
def step_impl(context, message_label, test_name, rest_url):
    set_tradeid(MESSAGES[message_label], test_name)
    response = SCENARIO_REST.consume(symbol_replace(rest_url))
    MESSAGES['RESULT_REQUEST'] = response.text
    #LOGGER.info("Received response {} from {} with status {}".format(response.text, symbol_replace(rest_url), response.status_code))
    #LOGGER.info("Rest Query: {}".format(symbol_replace(rest_url)))

@step('"{message_label}" is sent as a POST request to "{rest_name}" REST_VPMREPORTING')
def step_impl(context, message_label, rest_name, headers=None):
    response = SCENARIO_REST_VPMREPORTING.produce(rest_name, MESSAGES[message_label], headers=None)
    MESSAGES[message_label] = (response.text).replace('null', '0')
    #LOGGER.info("Message received to {} with response code: {}".format(rest_name, response.status_code))
    #LOGGER.info("Message received from {} with body {}".format(rest_name, response.text))
    #     
@step('TradeId from "{message_label}" for "{test_name}" is set')
def step_impl(context, message_label, test_name):
    set_tradeid(MESSAGES[message_label], test_name)
    
@then('the following XML text fields are expected in received messages')
def step_impl(context):
    for row in context.table:
        XPathVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_text_field(row[TableHeaders.XML_ELEMENT], row[TableHeaders.VALUE])

@then('the following XML attributes are expected in received messages')
def step_impl(context):
    for row in context.table:
        XPathVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_attribute(row[TableHeaders.XML_ELEMENT],
                              row[TableHeaders.XML_ATTRIBUTE],
                              row[TableHeaders.VALUE])

@then('the following XML root tag names are expected in received messages')
def step_impl(context):
    for row in context.table:
        XPathVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_root_tag_name(row[TableHeaders.VALUE])

@then('the following JSON text fields are expected in received messages')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_text_field(row[TableHeaders.JSON_ELEMENT], symbol_replace(parse_text_string(row[TableHeaders.VALUE])))

@then('the following JSON text fields are stored as parameters')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .store_text_field(row[TableHeaders.JSON_ELEMENT], parse_text_string(row[TableHeaders.PARAMETER]))
            
@then('the following JSON text fields per JSON item are expected in received messages')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_item_text_field(row[TableHeaders.JSON_ELEMENT], int(row[TableHeaders.JSON_ITEM]), symbol_replace(parse_text_string(row[TableHeaders.VALUE])))

@then('the following JSON text fields per JSON item name are expected in received messages')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_item_text_field(row[TableHeaders.JSON_ELEMENT], row[TableHeaders.JSON_ITEM], symbol_replace(parse_text_string(row[TableHeaders.VALUE])))

@then('the following JSON text fields per JSON item are stored as parameters')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .store_item_text_field(row[TableHeaders.JSON_ELEMENT], int(row[TableHeaders.JSON_ITEM]), parse_text_string(row[TableHeaders.PARAMETER]))

@then('the following JSON text fields per JSON item name are stored as parameters')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .store_item_text_field(row[TableHeaders.JSON_ELEMENT], row[TableHeaders.JSON_ITEM], parse_text_string(row[TableHeaders.PARAMETER]))            
@then('the following JSON data is expected in received messages')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_text(row[TableHeaders.VALUE])

@step('the following response data is expected in the response')
def step_impl(context):
    for row in context.table:
        JSONVerifier(MESSAGES[row[TableHeaders.MESSAGE_LABEL]]) \
            .verify_text(row[TableHeaders.VALUE])
            
@then('the following text fields will be written to the "{data_file}" data file')
def step_impl(context, data_file):
    for row in context.table:
        with open(data_file, mode='a', newline = '') as batch_data_file:
            batch_data_writer = csv.writer(batch_data_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            batch_data_writer.writerow([row[TableHeaders.MESSAGE_LABEL], symbol_replace(row[TableHeaders.TRADE_ID])])
            
@given('the following file will be transfered')
def step_impl(context):
    for row in context.table:
        newPath = shutil.copy(row[TableHeaders.FILE_PATH], row[TableHeaders.DESTINATION_PATH])

@step('the following file will be deleted')
def step_impl(context):
    for row in context.table:
        if os.path.exists(row[TableHeaders.FILE_PATH]):
            time.sleep(1)
            os.remove(row[TableHeaders.FILE_PATH])
            time.sleep(1)
        if os.path.exists(row[TableHeaders.FILE_PATH]):
            time.sleep(1)
            os.remove(row[TableHeaders.FILE_PATH])
        
@step('the following folder will be cleared')
def step_impl(context):
    for row in context.table:
        fileList = glob.glob(row[TableHeaders.FILE_PATH])
        for filePath in fileList:
            try:
               os.remove(filePath)
               print(filePath)
            except:
                print('Error deleting file -', filePath)
        
@step('the following sql will be executed')
def step_impl(context):
    for row in context.table:
        response = SCENARIO_SQL.execute(row[TableHeaders.TYPE], symbol_replace(row[TableHeaders.VALUE]))
        #LOGGER.info("Received response {} from database".format(response))
        MESSAGES[row[TableHeaders.MESSAGE_LABEL]] = "{}".format(response)

@step('the cashflow folder will be cleared')
def step_impl(context):
    response = SCENARIO_SYSTEM.clearCashFlowFolder()
    #LOGGER.info("Received response {} from system".format(response))

@step('the following cashflow file will be deleted')
def step_impl(context):
    for row in context.table:
        response = SCENARIO_SYSTEM.clearCashFlowFile(row[TableHeaders.FILE_PATH])
        
@given('the following cashflow file will be transfered')
def step_impl(context):
    for row in context.table:
        response = SCENARIO_SYSTEM.transferCashFlowFile(row[TableHeaders.FILE_PATH], row[TableHeaders.DESTINATION_PATH])
        
@step('the data load folder will be cleared')
def step_impl(context):
    response = SCENARIO_SYSTEM.clearDataLoadFolder()
    #LOGGER.info("Received response {} from system".format(response))

@step('the following data load file will be deleted')
def step_impl(context):
    for row in context.table:
        response = SCENARIO_SYSTEM.clearDataLoadFile(row[TableHeaders.FILE_PATH])
        
@given('the following data load file will be transfered')
def step_impl(context):
    for row in context.table:
        response = SCENARIO_SYSTEM.transferDataLoadFile(row[TableHeaders.FILE_PATH], row[TableHeaders.DESTINATION_PATH])

