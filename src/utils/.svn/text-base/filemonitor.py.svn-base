import os, time

class DirectoryMonitor:
    
    def __init__(self,path,callback):
        self.path_to_watch=path
        self._callback=callback
    
    def monitor(self): 
        print 'Monitoring directory: %s ...' % self.path_to_watch
        before = dict ([(f, None) for f in os.listdir (self.path_to_watch)])
        while 1:
            time.sleep(5)
            after = dict ([(f, None) for f in os.listdir (self.path_to_watch)])
            added = [f for f in after if not f in before]
            if added: 
                print "Added: ", ", ".join (added)
                #call the callback function
                self._callback(added)
            before = after