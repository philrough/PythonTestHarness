import os
import shutil
import glob
import time
from test_harness import log


class SYSTEM():
    def __init__(self, cashflow, dataload, logLevel):
        self.logger = log.get_logger(SYSTEM.__name__)
        self.logger.setLevel(logLevel)
        self.cashflow = cashflow
        self.dataload = dataload

    def clearCashFlowFolder(self):
        fileList = glob.glob(self.cashflow + '\\*.csv')
        for filePath in fileList:
            try:
               os.remove(filePath)
            except:
                self.logger.info("Error deleting file -  {}".format(filePath))
        return
        
    def clearCashFlowFile(self, fileName):
        fileList = glob.glob(self.cashflow + '\\' + fileName)
        for filePath in fileList:
            try:
               if os.path.exists(filePath):
                  os.remove(filePath)
                  time.sleep(1)
                  if os.path.exists(filePath):
                      os.remove(filePath)
            except:
                self.logger.info("Error deleting file -  {}".format(filePath))
        return

    def transferCashFlowFile(self, sourceFile, destFile):
        shutil.copy( 'scenario_files/Project/' + sourceFile, self.cashflow + '\\' + destFile)
        time.sleep(5)
        if not os.path.exists(self.dataload + '\\' + destFile):
            shutil.copy( 'scenario_files/Project/' + sourceFile, self.cashflow + '\\' + destFile)
            time.sleep(5)
        
    def clearDataLoadFolder(self):
        fileList = glob.glob(self.dataload + '\\*.csv')
        for filePath in fileList:
            try:
               if os.path.exists(filePath):
                  os.remove(filePath)
                  time.sleep(1)
                  if os.path.exists(filePath):
                      os.remove(filePath)
            except:
                self.logger.info("Error deleting file -  {}".format(filePath))
        return
        
    def clearDataLoadFile(self, fileName):
        fileList = glob.glob(self.dataload + '\\' + fileName)
        for filePath in fileList:
            try:
               os.remove(filePath)
               if os.path.exists(filePath):
                   os.remove(filePath)
            except:
                self.logger.info("Error deleting file -  {}".format(filePath))
        return

    def transferDataLoadFile(self, sourceFile, destFile):
        shutil.copy( 'scenario_files/Project/' + sourceFile, self.dataload + '\\' + destFile)
        time.sleep(2)
        if not os.path.exists(self.dataload + '\\' + destFile):
            shutil.copy( 'scenario_files/Project/' + sourceFile, self.dataload + '\\' + destFile)
            time.sleep(2)
