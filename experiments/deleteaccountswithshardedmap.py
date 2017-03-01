from model.account import Account
from taskutils import shardedmap, futureshardedmap
from taskutils.sharded import shardedpagemap
from google.appengine.ext import ndb

def DeleteAccountsWithShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        shardedmap(DeleteAccount, ndbquery = Account.query())            
    return "Delete Accounts With Sharded Map", Go

def DeleteAccountsWithFutureShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        return futureshardedmap(DeleteAccount, ndbquery = Account.query()).key
    return "Delete Accounts With Future Sharded Map", Go

def DeleteAccountsWithShardedPageMapExperiment():
    def Go():
        def DeleteAccounts(keys):
            ndb.delete_multi(keys)

        shardedpagemap(DeleteAccounts, ndbquery = Account.query())
    return "Delete Accounts With Sharded Page Map", Go
