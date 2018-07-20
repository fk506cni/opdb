import os
import re
import sys
from opdb.procmanager import PutRec2FB
from opdb.procmanager import PutJPRec2FB
from opdb.procmanager import xml2tsv

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

    def saveXmlAndData(self):

        for file in self.files:
            file = self.indir_path+file
            print("proccessing file is "+file)
            extend = re.findall(r"_jrep\.data$|\.xml$", file)
            print(extend)

            if len(extend) ==0:
                print("extend is not data or xml")
            elif re.search(r'\.xml$', extend[0]):
                print("calling xmlsaver")
                x2t = xml2tsv(file, self.outdir)
                x2t.saveTsv()
            elif re.search(r'_jrep\.data$', extend[0]):
                print("calling datasaver")
            else:
                print("unko")

#mj = mainJp()


if __name__ == '__main__':
    args = sys.argv
    #print(args)


    if(len(args)==7):
        db = args[1]
        user = args[2]
        pw = args[3]
        mode = args[4]
        xdir = args[5]
        outdir = args[6]

        if(mode == "data2db"):

            print(xdir)
            mn = mainN1(xdir)
            mn.proc()

            mj = mainJp(xdir)
            mj.proc()
        elif(mode == "data2tsv"):
            print("mode is data2tsv... underconstruction")
            sv = saveN1AndData(xdir, outdir)
            sv.saveXmlAndData()
    else:
        print("give me 6 args.")

else:
    print("unko!")