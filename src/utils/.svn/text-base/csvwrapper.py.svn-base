import csv

class CsvWrapper:
    def __init__(self,conf):
        self.conf=conf
    
    def reader(self,handle):
        return csv.reader(handle,delimiter=self.conf.csvDelimiter)
    
    def writerows(self,file,rows):
        handle=open(file,'wb')
        writer=csv.writer(handle,delimiter=self.conf.csvDelimiter)
        writer.writerows(rows)
        handle.close()
        
        
        