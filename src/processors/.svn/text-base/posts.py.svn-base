from SimpleXMLRPCServer import SimpleXMLRPCServer
from utils.config import Config
from processors.users import UserProcessor
from utils.csvwrapper import CsvWrapper 
import sys,time,re,xmlrpclib,threading,Queue


class PostsProcessorDaemon(threading.Thread):
    def __init__(self,ip,port,conf):
        threading.Thread.__init__(self)
        self.ip=ip
        self.port=port
        self.conf=conf
        
    def run(self):
        self.start_server()
            
    def start_server(self):
        #create an instance of rpc server
        daemon=SimpleXMLRPCServer((self.ip,self.port))

        service=PostsProcessorService(self.conf)
        
        daemon.register_instance(service)
        
        print 'Listening on port %s' % self.port
           
        daemon.serve_forever()

class PostsProcessorService:
    
    def __init__(self,conf):
        self.conf=conf
        
    def processFile(self,source,filename,data):
        #save the file to disk
        file=self.conf.queuePath + str(int(time.time()))+'_'+filename
        
        try:
            print 'Receiving file %s ...' % filename        
            fileHandle=open(file,'w')
            
            print 'Saving file %s ...' % file
            fileHandle.write(data)
        
            fileHandle.close()
            
        except:
            return sys.exc_info()
        else:
            return 'ok'  

class NoFilePoolError:pass

class PostProcessor(threading.Thread):

    def __init__(self,conf,fpool):
        threading.Thread.__init__(self)

        self.conf=conf
        #sets the reference to the file queue
        self._fpool=fpool

        regxep=''
        
        for term in self.conf.bannedTerms:
            regxep= regxep + term.strip() + '|'
            
        regxep=regxep.rstrip('|')
        
        self.pattern=re.compile(regxep, re.IGNORECASE)
        
        self.userProcessor=UserProcessor(conf)
        self.csvProcessor=CsvWrapper(conf)
    
    def run(self):
        if self._fpool!=None:
            while True:
                #get the file from the pool
                file=self._fpool.get()
                if file!=None:
                    print '%s: Star processing file: %s' % (self.getName(),file)
                    self.process((self.conf.queuePath+file))
        else:
            raise NoFilePoolError(),'You must provide a file pool if you intend to run the processor in a multithreading fashion.'
            
    def process(self,file):
        
        #first we open the file to parse
        fhandle=open(file,'r')
        
        #reader=csv.reader(fhandle,delimiter=self.conf.csvDelimiter)
        reader=self.csvProcessor.reader(fhandle)
        
        processedRows=0
        bannedRows=0
        postsCount=0
        posts=[]
        #we parse the rows
        for row in reader:
            if processedRows >0:
                #verify if the post doesn't contains banned terms
                if not self.isBanned(row[0]):
                    if not self.isBanned(row[1]):
                        #get the category
                        category=self.mapCategory(row[2])
                        
                        #get the user for the post
                        user=self.getUser()
                        
                        #create the post
                        posts.append(self.createPost(row, category, user))
                        
                        postsCount = postsCount + 1
                        
                        #we reach the size of the batch file
                        if len(posts)==self.conf.batchSize:
                            self._saveAndSendFile(posts)
                            posts=[]
                    else:
                        bannedRows = bannedRows + 1
                        print '%s: Post baneado' % self.getName()
                else:
                    bannedRows = bannedRows + 1
                    print '%s: Post baneado' % self.getName()
                    
            processedRows = processedRows + 1
            
        #close the source file    
        fhandle.close()
        
        #save the remaining posts
        if len(posts)>0:
            self._saveAndSendFile(posts)
                
    def _saveAndSendFile(self,posts):
        
        #write posts file
        fname='posts_'+str(int(time.time()))+'.csv'
        
        print '%s: Saving file: %s ...%s posts' %  (self.getName(),fname,len(posts))
        
        self.csvProcessor.writerows((self.conf.processedPath+fname),(self.getHeaders()+ posts))
        
        #clear the user's pool so in the next batch, we generate a new one
        self.userProcessor.clearUsers()
        
        #send the posts file.        
        self.sendFile(fname)
        
    def isBanned(self,string):
        result=self.pattern.search(string)
        if not result is None:
            return True    
        return False    
        
    
    def mapCategory(self,category):
        for key in self.conf.categoryMapping.keys():
            mapping=self.conf.categoryMapping[key]
            if category in mapping:
                break
        else:
            key=self.conf.defaultCategory
            
        return key    
            
    
    def getUser(self):
        return self.userProcessor.getUser(self.conf.usersPerBatch)
    
    def createPost(self,rawPost,category,user):
        return [rawPost[0],rawPost[1],user,category,rawPost[3],rawPost[4]]
    
    def getHeaders(self):
        return [['Title','Body','Name','Category','Tags','Created']]
        
   
    def sendFile(self,file):
        #first we load the server
        server = xmlrpclib.ServerProxy(self.conf.importServerUrl)
        #open the file
        handle=open((self.conf.processedPath+file),'r')
        
        bdata = xmlrpclib.Binary(handle.read())
        
        print "%s: Sending file: %s ..." % (self.getName(),file)        
        #now we insert the users in the server
        result = server.import_posts(self.conf.sourceName,file,bdata)
        
        #fill the users pool  
        print "%s: Imported %s posts ..." % (self.getName(),result['processed'])
   
                               