import pyodbc


class ConnectDB():

    def __init__(self):
        print("connectDB ver 2")
        self.dsn = "OPDB64"
        self.user = "checkUser"
        self.PWD = "114514"
        self.connectionString = "DSN=" + self.dsn + ";UID=" + self.user + ";PWD=" + self.PWD

    def exSQL(self, sql, show=False):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        rows = cursor.execute(sql)
        if (show):
            for row in rows:
                print(row)
        connection.commit()
        connection.close()

    def getTables(self, info="name"):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        for row in cursor.tables():
            if info == "name":
                print(row.table_name)
            elif info == "cat":
                print(row.table_cat)
            elif info == "shcem":
                print(row.table_schem)
            elif info == "type":
                print(row.table_type)
            elif info == "remarks":
                print(row.remarks)
        connection.close()

    def getCollumnNames(self, table):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        for row in cursor.columns(table=table):
            print(row.column_name)
        connection.close()

    def AddNewRec(self, table, string):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        cursor.execut(string)
        connection.commit()
        connection.close()

    def getTableData(self, table):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        rows = cursor.execute("select * from " + table)
        for row in rows:
            print(row)
        connection.close()

    def __insertData(self, table, columns, values):
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        sql = "insert into " + table + " " + columns + " values " + values
        print("sql: " + sql)
        cursor.execute(sql)
        connection.commit()
        connection.close()

    def insertDataFromList(self, table, collist, vallist):
        ##collist and vallist is list of colname and values.
        if (len(collist) != len(vallist)):
            print("lenghts is not equal.")
        columns = self.list2sentence(collist, "col")
        values = self.list2sentence(vallist, "val")
        print("target columns:"+columns)
        print("inserting vals:"+values)
        if(self.isDuplData(table, collist, vallist)):
            print("insertion was canceled because of duplication.")
        else:
            self.__insertData(table, columns, values)
            print('unique vals were inserted.')

    def list2sentence(self, wordlist, mode="val"):
        if mode == "val":
            sep, quat, trim, sent = ", ", "'", -2, "("
        elif mode == "col":
            sep, quat, trim, sent = ", ", "", -2, "("
        for w in wordlist:
            if type(w) != str:
                w = str(w)
            else:
                w = quat + w + quat
            sent = sent + w + sep
        sent = sent[:trim] + ")"
        return sent

    def isDuplData(self, table, collist, vallist):
        if (len(collist) != len(vallist)):
            print("lengths is not equal.")
            return ""
        condition = self.makeAndSentence(collist, vallist)
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        sql = "select * from " + table + " where " + condition
        print("sql: " + sql)
        rows = cursor.execute(sql)
        count = sum(1 for _ in rows)
        print("the hits count in the condition: "+str(count))
        result = count != 0
        connection.close()
        return result

    def makeAndSentence(self, collist, vallist):
        condition = ""
        for (col, val) in zip(collist, vallist):
            if type(val) != str:
                condition = condition + col + " = " + str(val) + " and "
            else:
                condition = condition + col + " = '" + val + "' and "
        condition = condition[:-5]
        print(condition)
        return (condition)


#
#unko.getCollumnNames("PatientInfo")