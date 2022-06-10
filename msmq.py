import win32com.client
from test_harness import log

MQ_SINGLE_MESSAGE = 3

class MSMQ():
    def __init__(self, username, password, host, name, logLevel):
        self.logger = log.get_logger(MSMQ.__name__)
        self.logger.setLevel(logLevel)
        self.hostqueue = host
        self.queuename = name

    def produce(self, message):
        qinfo=win32com.client.Dispatch("MSMQ.MSMQQueueInfo")
        qinfo.FormatName="direct=os:"+self.hostqueue+"\\PRIVATE$\\"+self.queuename
        queue=qinfo.Open(2,0)   # Open a ref to queue to write(0)
        msg=win32com.client.Dispatch("MSMQ.MSMQMessage")
        print("MsgBody: {}".format(msg))
        msg.Body = message.encode(encoding='UTF-8',errors='strict')
        msg.Send(queue, MQ_SINGLE_MESSAGE) # remove constant for non transactional queues
        queue.Close()
        return msg

    def consume(self):
        qinfo=win32com.client.Dispatch("MSMQ.MSMQQueueInfo")
        qinfo.FormatName="direct=os:"+self.hostqueue+"\\PRIVATE$\\"+self.queuename
        queue=qinfo.Open(1,0)   # Open a ref to queue to read(1)
        msg=queue.Receive()
        queue.Close()
        return msg
