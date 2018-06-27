import os
import re
from opdb.procmanager import PutRec2FB
from opdb.procmanager import PutJPRec2FB

class main():
    def __init__(self):
        dir_path = "D:/Cloud/Dropbox/DBs/POproto/rep/xmls/"
        dir = os.listdir(dir_path)

        for file in dir:
            xpath = dir_path+file
            print(xpath)
            bl = re.search('\.xml$', xpath)
            if bl is not None:
                pb = PutRec2FB(xpath)
                pb.putAllData()



mn = main()

class mainJp():


    def __init__(self):
        dir_path="D:/Cloud/Dropbox/DBs/POproto/rep/jrep/"
        dir = os.listdir(dir_path)

        for file in dir:
            tpath = dir_path+file
            print(tpath)
            bl = re.search('_jrep\.data$', tpath)
            if bl is not None:
                pj = PutJPRec2FB(tpath)
                pj.addData()

mj = mainJp()


