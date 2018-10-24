import os
import re
import sys
import pandas as pd
import zipfile
import shutil
#from pathlib import Path

from opdb.procmanager import PutRec2FB
from opdb.procmanager import PutJPRec2FB
from opdb.procmanager import xml2tsv
from opdb.procmanager import data2tsv

class mainN1():
    def __init__(self, dir_path):
        #dir_path = "D:/Cloud/Dropbox/DBs/POproto/rep/xmls/"
        self.dir_path = dir_path
        self.dir = os.listdir(dir_path)

    def proc(self):
        for file in self.dir:
            xpath = self.dir_path+file
            print(xpath)
            bl = re.search('\.xml$', xpath)
            if bl is not None:
                pb = PutRec2FB(xpath)
                pb.putAllData()

#mn = main()

class mainJp():

    def __init__(self, dir_path):
        #dir_path="D:/Cloud/Dropbox/DBs/POproto/rep/jrep/"
        self.dir_path = dir_path
        self.dir = os.listdir(dir_path)

    def proc(self):

        for file in self.dir:
            tpath = self.dir_path+file
            print(tpath)
            bl = re.search('_jrep\.data$', tpath)
            if bl is not None:
                pj = PutJPRec2FB(tpath)
                pj.addData()

class saveN1AndData():

    def __init__(self, indir, outdir):
        self.indir_path = indir
        self.files = os.listdir(indir)
        self.outdir = outdir
        self.metacol = ["FileName", "SourceType"]
        self.metadf = pd.DataFrame(index=[], columns=self.metacol)

        if not os.path.isdir(self.outdir):
            os.makedirs(self.outdir)

    def saveXmlAndData(self):


        for file in self.files:
            file = self.indir_path+file
            print("proccessing file is "+file)
            extend = re.findall(r"_jrep\.data$|\.xml$", file)
            print(extend)

            meta_i = []

            if len(extend) ==0:
                print("extend is not data or xml")
            elif re.search(r'\.xml$', extend[0]):
                print("calling xmlsaver")
                x2t = xml2tsv(file, self.outdir)
                x2t.saveTsv()
                self.metadf = self.metadf.append(x2t.getMetaDF())
                print(x2t.getMetaDF())
            elif re.search(r'_jrep\.data$', extend[0]):
                print("calling datasaver")
                d2t = data2tsv(file, self.outdir)
                d2t.saveData()
                self.metadf = self.metadf.append(d2t.getMetaDF())
                print(d2t.getMetaDF())
            else:
                print("unko")

        self.saveDataAstSV()

    def saveDataAstSV(self):
        self.metadf.to_csv(self.outdir+"meta.tsv", sep="\t", index=False)

    def getMetaDF(self):
        return self.metadf


if __name__ == '__main__':
    args = sys.argv
    #print(args)

    ##args description
    #arg1 is mode
    #arg2 is input_dir(xdir is dir with xml and data)
    #arg3 is output dir
    #arg4 is db name
    #arg5 is user
    #arg6 is pw

    if(len(args) ==4):
        mode = args[1]
        xdir = args[2]
        outdir = args[3]

        if(mode == "data2tsv"):
            print("mode is data2tsv... db connection not required.")
            sv = saveN1AndData(xdir, outdir)
            sv.saveXmlAndData()
            dfx = sv.getMetaDF()

        elif(mode =="data2zip"):
            print("mode is data2zip... db connection not required.")
            sv = saveN1AndData(xdir, outdir)
            sv.saveXmlAndData()
            dfx = sv.getMetaDF()

            outzip = outdir[:-1]+".zip"
            print("output file: "+outzip)

            shutil.make_archive(outdir[:-1], "zip",outdir)

            # outlist = os.listdir(outdir)
            #
            # compFile = zipfile.ZipFile(outzip,'w', zipfile.ZIP_DEFLATED)
            # for file_i in outlist:
            #     print(file_i+": will be zipped")
            #     file_path = outdir+file_i
            #     compFile.write(file_path)
            # compFile.close()







    elif(len(args)==7):
        mode = args[1]
        xdir = args[2]
        outdir = args[3]
        db = args[4]
        user = args[5]
        pw = args[6]

        if(mode == "data2db"):
            print("mode is data2db... needs connection to DB")

            print(xdir)
            mn = mainN1(xdir)
            mn.proc()

            mj = mainJp(xdir)
            mj.proc()
    else:
        print("give me prop args.")

else:
    print("unko!")