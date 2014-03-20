import random
import time
import StringIO
import xmlrpclib

class UserProcessor:
    def __init__(self,conf):
        self.conf=conf

        file=open(conf.namesPath,'r')
        self.names=file.read().split(',')
        self.namesCount=len(self.names)
        self.usersPool=[]
        
    def process(self,count):
        #reinitialize the pool's value
        self.usersPool=[]

        #first we pick "count" names randomly
        snames=[ name+str(random.randint(0,20000)) for name in random.sample(self.names,count)]
        
        #create the file
        fname='users_'+str(int(time.time()))+'.csv'
        
        #generate users
        users=[(name + self.conf.csvDelimiter + self.generateMail(name) + self.conf.csvDelimiter) for name in snames]
        fusers='name' + self.conf.csvDelimiter + 'mail' + self.conf.csvDelimiter + 'pass\n'    
        fusers+='\n'.join(users)
        
        #send the file
        return self.sendFile(fname,fusers)
        
    def generateMail(self,name):
        domain=''.join([random.choice('abcdefghijklmnopqrstuvwxyz') for i in range(random.randint(5,9))])
        domain=domain+''.join(random.sample(['.com','.com.ar','.com.cl','.net'],1))
        mail=name+'@'+domain
        return mail   
    
    def getUser(self,poolcount=10):
        if len(self.usersPool)==0:
            self.process(poolcount)
        #picks a user from the pool
        return ''.join(random.sample(self.usersPool,1))
            
    def clearUsers(self):
        self.usersPool=[]
        
    def sendFile(self,filename,data):
        #first we load the server
        server = xmlrpclib.ServerProxy(self.conf.importServerUrl)
        
        bdata = xmlrpclib.Binary(data)
                
        #now we insert the users in the server
        result = server.import_users(self.conf.sourceName,filename,bdata)
        
        #fill the users pool  
        self.usersPool=result['processed']
        
        return len(self.usersPool)