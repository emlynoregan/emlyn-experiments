from model.account import Account
from taskutils.ndbsharded2 import ndbshardedmap, futurendbshardedmapwithcount
import logging
from taskutils.future import twostagefuture
from taskutils.ndbsharded2 import futurendbshardedpagemap, futurendbshardedmap

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
            def IncrementBalance(futurekey, account):
                account.balance += creditamount
                account.put()
                return 1
                
            futureobj = futurendbshardedmapwithcount(IncrementBalance, Account.query(), queue="background")
            return futureobj.key
        return AddFreeCredit(10)
    return "Increment Accounts With Future Sharded Map", Go

def CountAndIncrementAccountsExperiment():
    def Go():
        def AddFreeCredit(creditamount):
            def IncrementBalance(account):
                account.balance += creditamount
                account.put()

            def GetIncrementAccountsFuture(countfuture, parentkey, onsuccessf, onfailuref, **taskkwargs):
                future = futurendbshardedmap(IncrementBalance, Account.query(), parentkey=parentkey, onsuccessf=onsuccessf, onfailuref = onfailuref, **taskkwargs)
                future.set_weight(countfuture.get_result())
                return future
            
            def GetCountAccountsFuture(parentkey, onsuccessf, onfailuref, **taskkwargs):
                return futurendbshardedpagemap(None, Account.query(), parentkey=parentkey, onsuccessf=onsuccessf, onfailuref = onfailuref, **taskkwargs)
            
            futureobj = twostagefuture(GetCountAccountsFuture, GetIncrementAccountsFuture, queue="background")
            #futureobj = futurendbshardedmapwithcount(IncrementBalance, Account.query(), queue="background")
            return futureobj.key
        
        raise Exception("To be fixed")
        return AddFreeCredit(10)
    return "Count & Increment", Go
