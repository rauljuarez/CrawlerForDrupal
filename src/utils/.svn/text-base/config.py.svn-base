import xmlrpclib
from configobj import ConfigObj

class Config:
    __abstract__  = 'Config is an abstract class!' 
    
    def __init__(self,sourceName,entityType,configfile):
        self.sourceName=sourceName
        self.entityType=entityType
        self.configFile=configfile
        
        fconfig=ConfigObj(configfile)
        
        self.importServerUrl=fconfig['server']['url']
        self.csvDelimiter=fconfig['file']['delimiter']
        self.csvEnclosure=fconfig['file']['enclosure']
        self.batchSize=int(fconfig['file']['batchsize'])
        self.defaultCategory=fconfig['file']['default_category']
        self.usersPerBatch=int(fconfig['file']['user_per_batch'])
        self.queuePath=fconfig['paths']['queue']
        self.processedPath=fconfig['paths']['processed']
        self.namesPath=fconfig['paths']['names']
       
        if fconfig.has_key(sourceName):
            self.bannedTerms=fconfig[sourceName]['banned']
            self.categoryMapping=fconfig[sourceName]['categories']
        else:
            self.bannedTerms=[]
            self.categoryMapping={}
    def load(self,source):
        raise Exception(self.__abstract)

    def save(self,filename):
        fconfig = ConfigObj()
        if len(filename)>0:
            fconfig.filename = filename
        else:
            fconfig.filename = self.configFile
        
        fconfig['server'] = {}
        fconfig['server']['url'] = self.importServerUrl
            
        fconfig['file'] = {}
        fconfig['file']['delimiter'] = self.csvDelimiter
        fconfig['file']['enclosure'] = self.csvEnclosure
        fconfig['file']['batchsize'] = self.batchSize
        fconfig['file']['default_category']=self.defaultCategory
        fconfig['file']['user_per_batch']=self.usersPerBatch
        
        fconfig['paths'] = {}
        fconfig['paths']['queue'] = self.queuePath
        fconfig['paths']['processed'] = self.processedPath
        fconfig['paths']['names'] = self.namesPath
        
        fconfig[self.sourceName] = {}
        fconfig[self.sourceName]['banned'] = self.bannedTerms
        fconfig[self.sourceName]['categories'] = self.categoryMapping
       
        fconfig.write()
    

class XmlRpcConfig(Config):
    
    def __init__(self,sourceName,entityType,configfile):
        Config.__init__(self,sourceName,entityType,configfile)
    
    def load(self):
        #first we load the server
        server = xmlrpclib.ServerProxy(self.importServerUrl)
        #now we load the configuration from the remote server.
        config = server.get.config(self.sourceName,self.entityType)
        
        if len(config)>0:
            self.csvDelimiter=config['delimiter']
            self.csvEnclosure=config['enclosure']
            self.batchSize=int(config['batch_size'])
            self.defaultCategory=config['default_category']
            self.usersPerBatch=int(config['users_per_batch'])
        
        if len(self.bannedTerms)==0:
            self.bannedTerms = server.get.banned.terms(self.sourceName)

        if len(self.categoryMapping)==0:
            self.categoryMapping = server.get.category.mapping(self.sourceName)
        
        Config.save(self, '')
        
        
                    