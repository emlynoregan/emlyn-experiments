from google.appengine.ext import ndb
from google.appengine.api import search

class Account(ndb.model.Model):
    stored = ndb.DateTimeProperty(auto_now_add=True) 
    updated = ndb.DateTimeProperty(auto_now=True) 
    balance = ndb.IntegerProperty()

    def _post_put_hook(self, future):
        accountkey = future.get_result()
        
        document = search.Document(
            doc_id = accountkey.urlsafe(),
            fields = [
                search.DateField(name = "stored", value = self.stored),
                search.DateField(name = "updated", value = self.updated),
                search.NumberField(name = "balance", value = self.balance)
            ]
        )
        
        index = search.Index("Account")
        
        index.put(document)

    @classmethod
    def _post_delete_hook(cls, key, future):
        index = search.Index("Account")

        index.delete([key.urlsafe()])

    def to_dict(self):
        index = search.Index("Account")

        searchdoc = index.get(self.key.urlsafe())
        
        return {
            "key": self.key.urlsafe() if self.key else None,
            "stored": str(self.stored) if self.stored else None,
            "updated": str(self.updated) if self.stored else None,
            "balance": self.balance,
            "rank": searchdoc.rank if searchdoc else None
        }
        