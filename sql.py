import pyodbc

from test_harness import log


class SQL():
    def __init__(self, host, database, username, password, logLevel):
        self.logger = log.get_logger(SQL.__name__)
        self.logger.setLevel(logLevel)
        self.host = host
        self.database = database
        self.username = username
        self.password = password

    def execute(self, type, query):
        cnxn_str = ("Driver={SQL Server};"
            "Server=" + self.host + ";"
            "Database=" + self.database + ";"
            "UID=" + self.username + ";"
            "PWD=" + self.password + ";"
            "Trusted_Connection=yes;")    
        cnxn = pyodbc.connect(cnxn_str)

        cursor = cnxn.cursor()
        if type == 'query':
            data = cursor.execute(query).fetchone()
            if data == None:
                data = ''
            else:
                data, = data
        elif type == 'update':
            data = cursor.execute(query).rowcount
        cnxn.commit()
        del cnxn

        return data
