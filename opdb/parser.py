
import math
import pandas as pd
import re
import io

class Xml2DF():

    ReplaceList_report = [["disease", "TSR_disease"],
                          ["report-id", "ReportID"],
                          ["snomed-concept-id", "TSR_snomed_concept_id"],
                          ["snomed-disease-concept-id", "TSR_snomed_disease_concept_id"],
                          ["snomed-disease-name", "TSR_snomed_disease_name"]]

    ReplaceList_summary = [["AnnotatedReportID", "AnnotatedReportID"],
                           ["therapies-approved-in-disease", "TherapiesApprovedinThisDis_1_1"],
                           ["therapies-associated-resistance", "MayIndResist2Tx_1_1"],
                           ["approved-in-other-diseases", "TherapiesApprovedinOtherInd_1_1"],
                           ["available-trials", "Trials_1_1"],
                           ["biomarker", "GeneSymbol"],
                           ["pathway", "BiologicalAssociation_1_1"],
                           ["result-type", "Test_1_1"],
                           ["result-value", "Result_1_1"],
                           ["therapeutic-qualifier", "Therapeutic_qualifier_1_xx"],
                           ["user-alteration-name", "user-alteration-name"],
                           ["variant-allele-frequency", "VariantAlleleFrequency_1_3"]]

    def __init__(self, xpath):
        print("xml2df ver23")
        import xml.etree.ElementTree as et
#        import pandas as pd
        from collections import Counter as cc
        self.pd = pd
        self.et = et
        self.cc = cc
        self.tr = self.et.parse(xpath)
        self.el = self.tr.getroot()
        self.ids = {"AnnotatedReportID": self.el.attrib["report-id"]}
        # print(self.el.tag)
        print("xml2df initialized")

    def _bool2str(self, bl):
        if bl:
            return "True"
        else:
            return "False"

    def replaceDictKeys(self, dict_0, replaceList):
        for (pre, post) in replaceList:
            #if sentense
            if(pre in dict_0):
                dict_0[post] = dict_0.pop(pre)
            else:
                dict_0.update({post: "None"})
        return dict_0

    def replaceDFcolnames(self, df, replaceList):
        for (pre, post) in replaceList:
            if(pre in df.columns):
                df = df.rename(columns={pre: post})
            else:
                df[post] = self.pd.Series(["None" for i in range(len(df.index))]).values
        return df

    def empty2x(self, val, x):
        if(val == ''):
            print("empty found.")
            return x
        else:
            print("empty not found")
            return val

    def getReportDF(self):
        report_attrib = self.el.attrib
        # attrib.update(self.ids)
        # report_attrib = self.replaceDictKeys(report_attrib, ReplaceList_report)
        df = self.pd.DataFrame(report_attrib, index=[self.el.tag])
        df = self.replaceDFcolnames(df, self.ReplaceList_report)
        ## hear "No" in report.
        df.fillna("NA")
        for col in df.__iter__():
            print(col)
            df[col] = df[col].replace("'", "''", regex=True)
        return (df)

    def getSummaryDF(self):
        labelList = ["summary", "positives"]
        posRes = self.getProgenyTreeFromLabel(labelList)
        posResDF = self.getChildrenDF(posRes, "positive-result")
        posResDF = self.replaceDFcolnames(posResDF, self.ReplaceList_summary)

        ispos = []
        for col, row in posResDF.iterrows():
            bl = row["TherapiesApprovedinThisDis_1_1"] == "Yes" \
                 or row["MayIndResist2Tx_1_1"] == "Yes" \
                 or row["TherapiesApprovedinOtherInd_1_1"] == "Yes" \
                 or row["Trials_1_1"] == "Yes"
            ispos.append(self._bool2str(bl))

        posResDF["isPositive"] = self.pd.Series(ispos).values

        #remove "user-alteration-name"
        del posResDF['user-alteration-name']
        #change nan to "No"
        posResDF = posResDF.fillna("None")
        return (posResDF)

    def getProgenyTreeFromLabel(self, labelList):
        # parse partial tree as list of tree
        tr_i = self.el
        for i in range(len(labelList)):
            labels_i = [i.tag for i in tr_i]
            # print(labels_i)
            tr_i = tr_i[labels_i.index(labelList[i])]
        return (tr_i)

    def getChildrenDF(self, e_tree, label):
        children = [i for i in e_tree if i.tag == label]
        dfs = [self.parseETasDF(i) for i in children]
        if (len(dfs) > 1):
            df = self.pd.concat(dfs, sort=True)
            return (df)
        else:
            return dfs[0]

    def parseETasDF(self, e_tree):
        attrib = e_tree.attrib
        attrib.update(self.ids)
        df = self.pd.DataFrame(attrib, index=[e_tree.tag])
        return (df)

    def getTree2TreeByLabel(self, e_tree, labelList):
        # parse partial tree as list of tree
        tr_i = e_tree
        for i in range(len(labelList)):
            labels_i = [i.tag for i in tr_i]
            if (labelList[i] in labels_i):
                # print(labels_i)
                tr_i = tr_i[labels_i.index(labelList[i])]
            else:
                tr_i = self.et.Element("error_el")
                tr_i.text = ""
        return (tr_i)

    def getMarkersDF(self):
        labelList = ["actionable-biomarkers"]
        self.mks = self.getProgenyTreeFromLabel(labelList)
        dfs = [self.getMarkerDF(i) for i in self.mks]

        if (len(dfs) == 0):
            return None
            pass

        if (len(dfs) > 1):
            df = self.pd.concat(dfs, sort=True)
        elif (len(dfs) == 1):
            df = dfs[0]

        df = df.fillna("NA")
        return (df)

    def getMarkerDF(self, e_tree):
        bi = e_tree
        # print(bi.attrib)
        # print(bi.attrib["marker"])
        info = {}
        info.update({"GeneSymbol": bi.attrib["marker"]})

        bio_summary = self.getTree2TreeByLabel(bi, ["biomarker-summary"])
        # info.update(bio_summary.attrib)
        bio_function = self.getTree2TreeByLabel(bi, ["biomarker-summary", "content"])
        info.update({"AnnotationSummary_2_x_1": bio_function.text})

        bio_molfunc = self.getTree2TreeByLabel(bi, ["molecular-function", "content"])
        info.update({"MolecularFunction_2_x_2": bio_molfunc.text})

        bio_incidence = self.getTree2TreeByLabel(bi, ["incidence", "content"])
        info.update({"IncidenceInDis_2_x_2": bio_incidence.text})

        bio_roleInDis = self.getTree2TreeByLabel(bi, ["role-in-disease", "content"])
        info.update({"RoleInDis_2_x_3": bio_roleInDis.text})

        bio_drugSens = self.getTree2TreeByLabel(bi, ["drug-sensitivity", "content"])
        info.update({"EffectOnDrgSens_2_x_3": bio_drugSens.text})

        bio_fdaAp = self.getTree2TreeByLabel(bi, ["fda-approved", "content"])
        info.update({"FDA_Aprd_2_x_4": bio_fdaAp.text})

        bio_p3 = self.getTree2TreeByLabel(bi, ["phase-3", "content"])
        info.update({"Phase_3_Data_2_x_4": bio_p3.text})

        bio_p2 = self.getTree2TreeByLabel(bi, ["phase-2", "content"])
        info.update({"Phase_2_Data_2_x_4": bio_p2.text})

        bio_p1 = self.getTree2TreeByLabel(bi, ["phase-1", "content"])
        info.update({"Phase_1_Data_2_x_4": bio_p1.text})

        bio_preC = self.getTree2TreeByLabel(bi, ["preclinical", "content"])
        info.update({"Preclinical_2_x_4": bio_preC.text})

        # print(info)
        df_i = self.pd.DataFrame(info, index=[bi.tag])
        return (df_i)

    def getBMmerge(self):
        print("get BM df")
        df_summary = self.getSummaryDF()
        df_bim = self.getMarkersDF()
        df = self.pd.merge(df_summary, df_bim, on='GeneSymbol')

        for col in df.__iter__():
            #print(col)
            df[col] = df[col].replace("'", "''", regex=True)
        return df.fillna("NA")

    def getTxsDF(self):
        print("get Tx df")
        labelList = ["actionable-biomarkers"]
        self.mks = self.getProgenyTreeFromLabel(labelList)
        dfs = [self.getTxDF_i(i) for i in self.mks]
        dfs = [i for i in dfs if i is not None]

        if(len(dfs) == 0):
            return None
            pass

        if (len(dfs) > 1):
            df = self.pd.concat(dfs, sort=True)
        elif (len(dfs) == 1):
            df = dfs[0]

        df = df.fillna("NA")

        for col in df.__iter__():
            #print(col)
            df[col] = df[col].replace("'", "''", regex=True)
        return (df)


    def getTxDF_i(self, e_tree):
        e_symbol = {"LinkedGeneSymbol": e_tree.attrib["marker"]}
        print("processing marker is " + e_symbol["LinkedGeneSymbol"])
        # e_symbol = self.pd.DataFrame(e_symbol, index=[e_tree.tag])
        e = self.getTree2TreeByLabel(e_tree, ["therapies"])
        if ("therapy" in [i.tag for i in e]):
            dfs = [self.parseTx_i(i, e_symbol) for i in e]
            df = self.pd.concat(dfs, sort=True)
        else:
            df = None
        return (df)

    def parseTx_i(self, e_tree, e_symbol):
        ti = e_tree
        # print(ti)
        # print(e_symbol)
        tx = {}
        tx.update(self.ids)
        tx.update(e_symbol)
        tx_drgName = self.getTree2TreeByLabel(ti, ["drug-name"])
        tx.update({"DrugName": tx_drgName.text})

        tx_targets = self.getTree2TreeByLabel(ti, ["targets"])
        tx_tag_str = [i.text for i in tx_targets]
        tx_tag_str.sort()
        tx.update({"TargetGeneS": ",".join(tx_tag_str)})

        tx_tradename = self.getTree2TreeByLabel(ti, ["trade-name"])
        tx.update({"TradeName": tx_tradename.text})

        tx_tagRation = self.getTree2TreeByLabel(ti, ["rationale"])
        tx.update({"TargetRationale": tx_tagRation.text})

        tx_stsInThisST = self.getTree2TreeByLabel(ti, ["status-in-this-indication", "status"]).text
        tx_stsInThisDis = self.getTree2TreeByLabel(ti, ["status-in-this-indication", "disease-list"])
        tx_stsInThisDisList = [i.text for i in tx_stsInThisDis]
        tx_stsInThisDisList = ",".join(tx_stsInThisDisList)
        tx.update({"CurrentStatus4This": ": ".join([tx_stsInThisST, tx_stsInThisDisList])})
        tx_stsInThis = ": ".join(["ThisDis", tx_stsInThisST, tx_stsInThisDisList])


        tx_stsInOtherST = self.getTree2TreeByLabel(ti, ["status-in-other-indications", "status"]).text
        tx_stsInOtherDis = self.getTree2TreeByLabel(ti, ["status-in-other-indications", "disease-list"])
        tx_stsInOtherDisList = [i.text for i in tx_stsInOtherDis]
        tx_stsInOtherDisList = ",".join(tx_stsInOtherDisList)

        tx.update({"CurrentStatus4Other": ": ".join([tx_stsInOtherST, tx_stsInOtherDisList])})
        tx_stsInOther = ": ".join(["OtherDis", tx_stsInOtherST, tx_stsInOtherDisList])
        tx.update({"CurrentStatus": tx_stsInThis + "\n" + tx_stsInOther})

        dftx = self.pd.DataFrame(tx, index=[tx["DrugName"]])
        return (dftx)

    def getTrialsDF(self):
        labelList = ["actionable-biomarkers"]
        self.mks = self.getProgenyTreeFromLabel(labelList)
        dfs = [self.getTrialDF_i(i) for i in self.mks]
        dfs = [i for i in dfs if i is not None]

        if len(dfs) == 0:
            return None

        if (len(dfs) > 1):
            df = self.pd.concat(dfs, sort=True)
        elif (len(dfs) == 1):
            df = dfs[0]

        for col in df.__iter__():
            #print(col)
            df[col] = df[col].replace("'", "''", regex=True)
        return (df)

    def getTrialDF_i(self, e_tree):
        e_symbol = {"LinkedGeneSymbol": e_tree.attrib["marker"]}
        print("processing marker is " + e_symbol["LinkedGeneSymbol"])
        e = self.getTree2TreeByLabel(e_tree, ["clinical-trials"])
        if ("clinical-trial" in [i.tag for i in e]):
            dfs = [self.parseTrial_i(i, e_symbol) for i in e]
            df = self.pd.concat(dfs, sort=True)
        else:
            df = None
        return (df)

    def parseTrial_i(self, e_tree, e_symbol):
        trial = e_tree
        trial_dcit = {}
        trial_dcit.update(self.ids)
        trial_dcit.update(e_symbol)
        trial_dcit.update({"TrialID": self.getTree2TreeByLabel(trial, ["trial-id"]).text})
        trial_dcit.update({"TrialTitle": self.getTree2TreeByLabel(trial, ["title"]).text})
        trial_dcit.update({"TrialPhase": self.getTree2TreeByLabel(trial, ["phase"]).text})
        trial_dcit.update({"TrialLocation_Contact": self.getTree2TreeByLabel(trial, ["overall-contact"]).text})

        trial_cont = [self.getTree2TreeByLabel(i, ["country"]).text for i in
                      self.getTree2TreeByLabel(trial, ["trial-sites"])]
        trial_inJp = self._bool2str("Japan" in trial_cont)
        print(trial_cont)
        print(type(trial_cont))
        print(trial_inJp)


        trial_cont_dict = self.cc(trial_cont)
        print(type(trial_cont_dict))

        trial_cont_dict = sorted(trial_cont_dict.items())

        trial_dcit.update({"TrialInJp": trial_inJp})

        trial_cs = ""
        for (cont, num) in trial_cont_dict:
            trial_cs = trial_cs + cont + ": " + str(num) + ", "
        trial_dcit.update({"TrialSites": trial_cs})

        #target genes
        trial_targets = [i.text for i in
                         self.getTree2TreeByLabel(trial, ["targets"])]
        trial_targets = ", ".join(trial_targets)

        trial_dcit.update({"TrialTargets": trial_targets})

        trial_df = self.pd.DataFrame(trial_dcit, index=[trial.tag])
        return (trial_df)

    def getRefDF(self):
        labelList = ["references"]
        ref = self.getProgenyTreeFromLabel(labelList)
        refDF = self.parseNestETasDF(ref)
        return (refDF)

    def nan2NA(self, x):
        if(math.isnan(x)):
            return "NA"
        else:
            return x

    def nan2NA_DF(self, df):
        df.applymap(lambda x: self.nan2NA(x))
        return df



#xpath = "D:/Cloud/Dropbox/DBs/POproto/rep/xxx_COMPLETE.xml"
#op = Xml2DF(xpath)
# # # # string = "afwwerre'afweg'aef"
# # # # string2 = op.procStrQ(string)
# # # # print(string2)
# df = op.getSummaryDF()
# df2 = op.getMarkersDF()
#df3 = op.getTrialsDF()
# df = op.getTxsDF()
# df = op.getBMmerge()
# list(df.columns).index("Phase_1_Data_2_x_4")
# "PR's across" in df.iat[1,13]
# "PR''s across" in df.iat[1,13]
# # list(df.columns).index('TradeName')
#
# for index, row in df.iterrows():
#     print(index, row['TradeName'])
#     row['TradeName'] = op.empty2x(row['TradeName'], " ")
#
#
# df.iat[index, list(df.columns).index('TradeName')]
#
#
# self.empty2x(df.iat[index, list(df.columns).index('TradeName')], " ")
# #
# #op.nan2NA(NaN)
#
# dfm = op.getBMmerge()
#
# dfm.columns
# dfm.index
# dfm['TherapiesApprovedinOtherInd_1_1']
#
# #dfm2 = dfm.fillna("NA")
# #dfm2['TherapiesApprovedinOtherInd_1_1']
# #type(dfm)

class txt2DF():

    Replacelist_reportJP = [["testid", "jpTestID"],
                            ["patientid", "KUH_ReportID"],
                            ["reportid", "ReportID"],
                            ["datetime", "ReportDate"],
                            ["creator", "jpCreator"],
                            ["refver", "jpRefver"],
                            ["pgver", "jpPgver"],
                            ["num-drugs", "jpNum_drugs"],
                            ["TSRファイル名", "jpTSR_filename"],
                            ["desease", "jpDesease"]
                            ]

    Replacelist_header = [["レポート作成日","ReportedDate"],
                          ["MKI検査番号","MKI_ID"],
                          ["識別番号","MKI_sampleID"],
                          ["検体ID",""]]

    Replacelist_trials = [["試験ID", "TrialID"],
                          ["対象疾患", "TargetDis"],
                          ["試験名称", "TrialName"],
                          ["実施機関（連絡先）", "TrialInst_Contact"]]

    def __init__(self, tpath):
        print("txt2DF ver1.1")
        #tpath = "D:/Cloud/Dropbox/DBs/POproto/rep/jrep/unkotest.data"
        f = open(tpath,'r',encoding="utf-8")
        data = f.read()
        f.close()
        self.pd = pd
        strlist = re.split('\[[0-9A-Z]*\]\\n', data)
        self.strlist = strlist[1:len(strlist)]
        taglist = re.findall('\[[0-9A-Z]*\]\\n', data)
        taglist = ["".join(i.splitlines()) for i in taglist]
        taglist = [re.sub(r"[\[\]]", "", i) for i in taglist]
        self.taglist = taglist

        tag = "INFORMATION"
        s = self._Tag2Str(tag)
        strs = s.splitlines()
        while strs.count("") > 0:
            del strs[strs.index("")]
        strs = [str.split(" # ")[0] for str in strs]
        #print(strs)
        d = dict()
        for s in strs:
            key, atr = tuple(s.split("="))
            d.update({key: atr})

        self.reportID = {"ReportID":d["reportid"]}
        self.kuhID = {"KUH_ReportID": d["patientid"]}
        self.pgVer = d["pgver"]
        self.id_dict = dict()
        self.id_dict.update(self.reportID)
        self.id_dict.update(self.kuhID)

    def _ishigh(self, ver1, ver2):
        v1 = list(re.sub(r"\.", "", ver1))
        v2 = list(re.sub(r"\.", "",ver2))
        res = False
        vlen = max(len(v1), len(v2))
        for i in range(vlen):
            tI = int(v1[i])
            vI = int(v2[i])
            if tI > vI:
                res = True
                break
            elif tI < vI:
                res = False
                break
            elif tI == vI:
                #print("pass: "+str(i))
                continue
        return res

    def _isIn(self, lver= "2.3.9", hver="2.6.0"):
        thisV = self.pgVer
        l_bl = self._ishigh(lver, thisV)
        r_bl = self._ishigh(thisV, hver)
        return l_bl and r_bl

    def _delEmp(self, strs):
        while strs.count("") > 0:
            del strs[strs.index("")]
        return strs

    def _bool2str(self, bl):
        if bl:
            return "True"
        else:
            return "False"

    def _replaceDFcolnames(self, df, replaceList):
        for (pre, post) in replaceList:
            if(pre in df.columns):
                df = df.rename(columns={pre: post})
            else:
                df[post] = self.pd.Series(["No" for i in range(len(df.index))]).values
        return df

    def _Tag2Str(self, tag):
        if tag in self.taglist:
            tag_ind = self.taglist.index(tag)
            str = self.strlist[tag_ind]
         #   print(str)
            return(str)
        else:
            print(tag+" is not in taglist")
            return(None)

    def getInfo(self):
        print("get info")
        tag = "INFORMATION"
        str = self._Tag2Str(tag)
        strs = str.splitlines()
        strs = self._delEmp(str.splitlines())
        # while strs.count("") > 0:
        #     del strs[strs.index("")]
        strs = [str.split(" # ")[0] for str in strs]
        #print(strs)
        d = dict()
        for s in strs:
            key, atr = tuple(s.split("="))
            d.update({key: atr})
        #print(d)
        df = self.pd.DataFrame(d, index=[tag])
        df = self._replaceDFcolnames(df, self.Replacelist_reportJP)
        #print(df)
        return df

    def getSummary2(self):
        print("get summary2")
        print(self.pgVer)
        if(self._isIn()):
            tag = "SUMMARY2"
            strs = self._Tag2Str(tag)
            df = pd.read_table(io.StringIO(strs), sep='\t', header=None)
            df = df.rename(columns ={0:'NumberInReport', 1:'LinkedGeneSymbol', 2: 'DrugWithTradeName', 3:'DomesticApproval', 4:'FDAApproval', 5:'DomesticTrials'})

            for key, val in self.id_dict.items():
                df[key] = pd.Series([val for i in range(len(df))]).values
            #
            # id_df = pd.DataFrame(self.id_dict, index=[tag])
            # df = pd.concat([atr_df, id_df], axis=1)
            #df = pd.read_table(strs, sep="\t")
            return df
        else:
            print("pg ver is out of service.")
            return None

    def getHeader(self):
        print("get header")
        tag = "HEADER"
        strs = self._Tag2Str(tag)
        strs = self._delEmp(strs.splitlines())
        d_head = dict()

        for s in strs:
            (key, atr) = tuple(re.split(r": |： ", s))
            d_head.update({key: atr})
        print(d_head)

        df = pd.DataFrame(d_head, index=[tag])
        return df

    def getDetail(self):
        #detail1 is clinical trials from MKI Data base.
        #

        tag = "DETAIL1"
        strs = self._Tag2Str(tag)
        strs = [re.sub(r"\[BR\]", "", s) for s in strs]
        str_list = re.split(r"2-[0-9]\. [0-9a-zA-Zぁ-んァ-ン一-龥\t・, ' ]+\n", strs)
        while str_list.count("") > 0:
            del str_list[str_list.index("")]
        tag_list = re.findall(r"2-[0-9]\. [0-9a-zA-Zぁ-んァ-ン一-龥\t・, ' ]+\n", strs)
        # for s , tag in zip(str_list, tag_list):
        #     print("unko")
        #     print(tag)
        #     print(s)

        ##tsr_annotated
        s0 = str_list[0]
        tags0 = re.findall(r"[A-Z]+[A-Za-z\(\)]+ を用いた国内治験・臨床試験\([0-9]+\)\t\t\t\tDTITLE\n", s0)
        tags0 = [re.findall(r"[A-Z]+[A-Za-z\(\)]+", i)[0] for i in tags0]


        strs0 = re.split(r"[A-Z]+[A-Za-z\(\)]+ を用いた国内治験・臨床試験\([0-9]+\)\t\t\t\tDTITLE\n", s0)
        strs0 = self._delEmp(strs0)
        #print(strs0)
        df0 = self._parseTrialJP(strs0, tags0)
        df0["IsInTSR"] = pd.Series(["True" for i in range(len(df0))]).values
        df0["lookEffective"] = pd.Series(["NA" for i in range(len(df0))]).values
        df0["needTargetinGeneMutation"] = pd.Series(["NA" for i in range(len(df0))]).values

        ##jp report annotated
        s1 = str_list[1]
        #print(s1)
        df1 = self._parseTrialJP([s1], ["NA"])
        df_lkEf = [self._bool2str("★" in s) for s in df1.loc[:, "TrialName"]]
        df_ndTGM = [self._bool2str("◎" in s) for s in df1.loc[:, "TrialName"]]

        df1["IsInTSR"] = pd.Series(["False" for i in range(len(df1))]).values
        df1["lookEffective"] = pd.Series(df_lkEf).values
        df1["needTargetinGeneMutation"] = pd.Series(df_ndTGM).values

        df = pd.concat([df0, df1])
        return df

    def _parseTrialJP(self, strs, tags):
        dfs =[]
        # print(len(strs))
        # print(len(tags))
        for s, tag in zip(strs, tags):
            s = re.sub(r"試験ID\t対象疾患\t試験名称\t実施機関（連絡先）\tDHEAD\tclass\tf_curated\tf_ct",
                       "試験ID\t対象疾患\t試験名称\t実施機関（連絡先）\tDHEAD\tclass\tf_curated\tf_ct\tunmet0\tunmet1\tunmet2",
                       s)
            # print(s)
            df_i = pd.read_table(io.StringIO(s), sep="\t")
            df_i["DrugInTSR"] = pd.Series([tag for i in range(len(df_i))]).values
            dfs.append(df_i)
        if(len(dfs) > 1):
            df = pd.concat(dfs)
        else:
            df = dfs[0]
        df = self._replaceDFcolnames(df, self.Replacelist_trials)
        return df


#
#
# tpath = "D:/Cloud/Dropbox/DBs/POproto/rep/jrep/unkotest.data"
# t2 = txt2DF(tpath)
# t2._Tag2Str('SUMMARY1')
# t2._Tag2Str("HEADER")
# df_afw = t2.getInfo()
# for i in range(10):
#     print(df_afw.iat[0, i])
# df_afw.columns
# print(list(df_afw.iloc[0, :]))
# df = t2.getSummary2()
# #t2.getHeader()
# df = t2.getDetail()
# df.TrialName
# df.loc[:,"TrialName"]

#list(re.sub(r"\.", "","2.4.2"))

# t2.isHigh("2.5.8")

