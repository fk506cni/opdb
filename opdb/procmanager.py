
from opdb.connector import ConnectDB
from opdb.parser import Xml2DF
from opdb.parser import txt2DF

class PutRec2FB():

    def __init__(self, xpath):
        self.cd = ConnectDB()
        self.xd = Xml2DF(xpath)
        print("processing xml:"+xpath)
        self.df_report = self.xd.getReportDF()
        self.df_bm = self.xd.getBMmerge()
        self.df_trial = self.xd.getTrialsDF()
        self.df_tx = self.xd.getTxsDF()

    def putRec2FB(self, df, table):
        collist = list(df.columns)
        print("inserting target table:" + table)
        print("inserting target colmns:"+ str(collist))
        for i in range(len(df.index)):
            vallist = list(df.iloc[i])
            print("inserting val:"+ str(vallist))
            self.cd.insertDataFromList(table, collist, vallist)

    def putReport(self):
        table = "Reports"
        if(self.df_report is not None):
            self.putRec2FB(self.df_report, table)
        else:
            print("Insertion skip due to None data. target:"+table)

    def putBM(self):
        table = "BioMarkers"
        if(self.df_bm is not None):
            self.putRec2FB(self.df_bm, table)
        else:
            print("Insertion skip due to None data. target:"+table)

    def putTrials(self):
        table = "TagGene2TagTrialsWW"
        if(self.df_trial is not None):
            self.putRec2FB(self.df_trial, table)
        else:
            print("Insertion skip due to None data. target:"+table)

    def putTxs(self):
        table = "TagGene2TagDrug"
        if(self.df_tx is not None):
            self.putRec2FB(self.df_tx, table)
        else:
            print("Insertion skip due to None data. target:"+table)

    def putAllData(self):
        self.putReport()
        self.putBM()
        self.putTxs()
        self.putTrials()


class PutJPRec2FB():

    def __init__(self, tpath):
        self.t2 = txt2DF(tpath)
        self.cd = ConnectDB()
        print("processing data:"+tpath)

        self.df_report = self.t2.getInfo()
        # self.df_drug = self.t2.getSummary2()
        # self.df_trial = self.t2.getDetail()

    def _putRec2FB(self, df, table):
        collist = list(df.columns)
        print("inserting target table:" + table)
        print("inserting target colmns:"+ str(collist))
        for i in range(len(df.index)):
            vallist = list(df.iloc[i])
            print("inserting val:"+ str(vallist))
            self.cd.insertDataFromList(table, collist, vallist)

    def _modRec2FB(self, df, table, keycolInds):
        collist = list(df.columns)

        print("update target table:" + table)
        print("update target colmns:"+ str(collist))

        for i in range(len(df.index)):
            vallist = list(df.iloc[i])
            print("updating val:"+ str(vallist))
            self.cd.modTableData(collist, vallist, keycolInds, table)

    def _modReportJP(self):
        df = self.df_report
        table = "Reports"
        keycolInds = [2]
        self._modRec2FB(df, table, keycolInds)

    def _putDrugsJP(self):
        df = self.df_drug
        table = "TagGene2TagDrugJp"
        self._putRec2FB(df, table)

    def _putTrialsJP(self):
        df = self.df_trial
        table = "TagGene2TagTrialsJP"
        self._putRec2FB(df, table)

    def addData(self):
        self._modReportJP()
        # self._putDrugsJP()
        # self._putTrialsJP()






# xpath = "D:/Cloud/Dropbox/DBs/POproto/rep/xxx_COMPLETE.xml"
#pb = PutRec2FB(xpath)
#df = pb.getRecDF()
#pb.putBM()
#pb.putTrials()
#pb.putTxs()
#pb.putAllData()
#df = pb.df_bm
#df.columns
#pb.putReport()
#pb.getMenu()
#df.columns

# list(df.columns).index('TradeName')

#print(df['Phase_1_Data_2_x_4'][1])
# df.iat[5,list(df.columns).index('TradeName')] == ''
#pb.putSummary()

#pb.df_marker.columns
#pb.df_marker.iloc[1]
#list(pb.df_marker.iloc[1])
#list(pb.df_marker.columns)
#
# tpath = "D:/Cloud/Dropbox/DBs/POproto/rep/jrep/OP15040706KUH_KUH00019_S4876012_jrep.data"
# pj = PutJPRec2FB(tpath)
# pj.addData()