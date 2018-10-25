
from opdb.connector import ConnectDB
from opdb.parser import Xml2DF
from opdb.parser import txt2DF
import pandas as pd

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
        print("putJp ver 1")
        self.t2 = txt2DF(tpath)
        self.cd = ConnectDB()
        print("processing data:"+tpath)

        self.df_report = self.t2.getInfo()
        self.df_drug = self.t2.getSummary2()
        self.df_trial = self.t2.getDetail()

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
        if self.df_drug is not None:
            self._putDrugsJP()
        if self.df_trial is not None:
            self._putTrialsJP()




class xml2tsv():

    def __init__(self, xpath, outdir):
        #self.cd = ConnectDB()
        self.xd = Xml2DF(xpath)
        self.df_report = self.xd.getReportDF()
        self.df_bm = self.xd.getBMmerge()
        self.df_trial = self.xd.getTrialsDF()
        self.df_tx = self.xd.getTxsDF()
        self.outdir = outdir
        print("input_path: "+xpath)
        print("output_dir: "+outdir)

        self.metacol = ["FileName", "SourceType"]

        self.metadf = pd.DataFrame(index=[], columns=self.metacol)

    def _colsort(self, df):
        df1 = df.copy()
        cols = list(df.columns.values).copy()
        cols.sort()
        df1 = df1.loc[:, cols]
        return df1

    def saveTsv(self):
        #report
        file0 = self.outdir+self.xd.ids["AnnotatedReportID"]
        tag0 = self.xd.ids["AnnotatedReportID"]

        if self.df_report is not None:
            rep = "_report.tsv"
            repfile = file0 + rep
            reptag = tag0 + rep
            meta_rep = pd.DataFrame([[reptag, "reportWW"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_rep)

            self.df_report = self._colsort(self.df_report)
            self.df_report.to_csv(repfile, sep="\t", index=False)

        if self.df_bm is not None:
            bm = "_biomarkers.tsv"
            bmfile = file0+bm
            bmtag = tag0+bm
            meta_bm = pd.DataFrame([[bmtag, "BM"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_bm)

            self.df_bm = self._colsort(self.df_bm)
            self.df_bm.to_csv(bmfile, sep="\t", index=False)

        if self.df_trial is not None:
            trial = "_trialww.tsv"
            trialfile = file0+trial
            trialtag = tag0+trial
            meta_trial = pd.DataFrame([[trialtag, "trialWW"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_trial)

            self.df_trial = self._colsort(self.df_trial)
            self.df_trial.to_csv(trialfile, sep="\t", index=False)

        if self.df_tx is not None:
            drug = "_drugww.tsv"
            drugfile = file0+drug
            drugtag = tag0+drug
            meta_drug = pd.DataFrame([[drugtag, "drugWW"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_drug)

            self.df_tx = self._colsort(self.df_tx)
            self.df_tx.to_csv(drugfile, sep="\t", index=False)


    def getMetaDF(self):
        return self.metadf

class data2tsv():

    def __init__(self, tpath, outdir):
        self.tpath = tpath
        self.outdir = outdir
        self.t2 =txt2DF(tpath)
        self.df_report = self.t2.getInfo()
        self.df_drug = self.t2.getSummary2()
        self.df_trial = self.t2.getDetail()
        self.reportID = self.t2.tag0

        print("input_path: "+tpath)
        print("output_dir: "+outdir)
        self.metacol = ["FileName", "SourceType"]
        self.metadf = pd.DataFrame(index=[], columns=self.metacol)

    def _colsort(self, df):
        df1 = df.copy()
        cols = list(df.columns.values).copy()
        cols.sort()
        df1 = df1.loc[:, cols]
        return df1

    def saveData(self):
        file0 = self.outdir+self.reportID
        tag0 = self.reportID

        if self.df_report is not None:
            rep = "_reportJP.tsv"
            repfile = file0 + rep
            reptag = tag0 + rep
            meta_rep = pd.DataFrame([[reptag, "reportJP"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_rep)

            self.df_report = self._colsort(self.df_report)
            self.df_report.to_csv(repfile, sep="\t", index=False)

        if self.df_trial is not None:
            trial = "_trialJP.tsv"
            trialfile = file0+trial
            trialtag = tag0+trial
            meta_trial = pd.DataFrame([[trialtag, "trialJP"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_trial)

            self.df_trial = self._colsort(self.df_trial)
            self.df_trial.to_csv(trialfile, sep="\t", index=False)

        if self.df_drug is not None:
            drug = "_drugJP.tsv"
            drugfile = file0+drug
            drugtag = tag0+drug
            meta_drug = pd.DataFrame([[drugtag, "drugJP"]], columns=self.metacol)
            self.metadf = self.metadf.append(meta_drug)

            self.df_drug = self._colsort(self.df_drug)
            self.df_drug.to_csv(drugfile, sep="\t", index=False)

    def getMetaDF(self):
        return self.metadf


# xpath = "D:/xx/xx/DBs/xx/rep/xxx_COMPLETE.xml"
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
# tpath = "D:/xx/xx/DBs/xx/rep/jrep/xxx.data"
# pj = PutJPRec2FB(tpath)
# pj.addData()
#
# import pandas as pd
# un = pd.DataFrame(index=[], columns=["un","ko","dai", "suki"])
# ko = pd.DataFrame([["unko", "unkkko","ddai", "sukki"]], columns=["un","ko","dai", "suki"])
# un = un.append(ko)
# print(un)
# col = un.columns.values
# col = list(col)
# col.sort()
# print(col)
#
# un = un.loc[:, col]
# print(un)
