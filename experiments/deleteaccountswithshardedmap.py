from model.account import Account
from taskutils.ndbsharded2 import ndbshardedmap, futurendbshardedmap, ndbshardedpagemap
from google.appengine.ext import ndb

def DeleteAccountsWithShardedMapExperiment():
    def Go():
        def DeleteAccount(account):
            account.key.delete()

        ndbshardedmap(DeleteAccount, ndbquery = Account.query())            
    return "Delete Accounts With Sharded Map", Go

def DeleteAccountsWithFutureShardedMapExperiment():
    def Go():
        def DeleteAccount(futurekey, account):
            account.key.delete()
            return 1

        return futurendbshardedmap(DeleteAccount, ndbquery = Account.query()).key
    return "Delete Accounts With Future Sharded Map", Go

def DeleteAccountsWithShardedPageMapExperiment():
    def Go():
        def DeleteAccounts(keys):
            ndb.delete_multi(keys)

        ndbshardedpagemap(DeleteAccounts, ndbquery = Account.query())
    return "Delete Accounts With Sharded Page Map", Go
