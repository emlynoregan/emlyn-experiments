from google.appengine.ext import ndb

class Dude(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add=True) 
    updated = ndb.DateTimeProperty(auto_now=True) 

    def to_dict(self):
        return {
            "key": self.key.urlsafe() if self.key else None,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None
        }
        
        
def CleanupAllDudes():
    keys = []
    for dudekeys in Dude.query().fetch(100, keys_only=True):
        keys.extend(dudekeys)
    ndb.delete_multi(keys)
    
def CreateDudes(aNum):
    dudes = [Dude() for _ in range(aNum)]
    ndb.put_multi(dudes)

