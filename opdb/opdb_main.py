import os
import re
from opdb.procmanager import PutRec2FB

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