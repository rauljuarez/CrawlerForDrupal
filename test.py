import xmlrpclib
import os.path
from utils.config import XmlRpcConfig
from processors.posts import PostsProcessorDaemon,PostProcessor
from processors.users import UserProcessor 
from utils.filemonitor import DirectoryMonitor

#x=XmlRpcConfig('taringa','posts')
#x.load('http://192.168.1.45/psp/xmlrpc.php') 
#print x.csvDelimiter

conf=XmlRpcConfig('taringa','test','D:/DEV_PY/PSPLoader/config')
conf.load()
#p=PostProcessor(conf)
#p.process('D:/DEV_PY/PSPLoader/processed.csv')






#user=UserProcessor(conf)
#user.process(conf.usersPerBatch)
#print user.usersPool
#print user.getUser()
#conf.queuePath='D:/DEV_PY/PSPLoader/'
#conf.csvDelimiter=';'

#def myCallback(file):
#    print 'i received a file named %s' % file

daemon=PostsProcessorDaemon('192.168.1.44',8000,conf)
daemon.start()
print 'daemon started'
print 'thread ID:%s' % daemon.getName()
DirectoryMonitor('D:/DEV_PY/PSPLoader',myCallback).monitor()




 