from model.account import Account
from taskutils import ndbshardedmap, futurendbshardedmapwithcount
import logging

def IncrementAccountsWithShardedMapExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            def IncrementBalance(account, headers):
#                 headers = kwargs.get("headers")
                logging.debug("headers: %s" % headers)
                account.balance += creditamount
                account.put()
                
            ndbshardedmap(IncrementBalance, Account.query(), includeheaders = True)
        AddFreeCredit(10)
    return "Increment Accounts With Sharded Map", Go


def IncrementAccountsWithFutureShardedMapExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            def IncrementBalance(account):
                account.balance += creditamount
                account.put()
                
            futureobj = futurendbshardedmapwithcount(IncrementBalance, Account.query(), queue="background")
            return futureobj.key
        return AddFreeCredit(10)
    return "Increment Accounts With Future Sharded Map", Go
