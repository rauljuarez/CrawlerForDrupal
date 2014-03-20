from utils.filemonitor import DirectoryMonitor
from processors.posts import PostsProcessorDaemon,PostProcessor
from utils.config import XmlRpcConfig
import Queue,os.path

#create the file queue
fqueue=Queue.Queue(100)

def pool_builder(files):
    for file in files:
        fqueue.put(file, True)

#load the configuration        
print 'Loading configuration...'
conf=XmlRpcConfig('taringa','posts','D:/DEV_PY/PSPCrawler/config')
conf.load()

#create the processors threads
print 'Starting processor threads...'
for x in xrange(2):
    PostProcessor(conf, fqueue).start()

#starts the xmlrpc server
print 'Starting daemon...'
daemon=PostsProcessorDaemon('192.168.1.44',8000,conf)
daemon.start()

#start the directory monitor
print 'Starting monitor...'
DirectoryMonitor(os.path.dirname(conf.queuePath),pool_builder).monitor()



        
