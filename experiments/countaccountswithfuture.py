from model.account import Account
from taskutils.future2 import future, FutureReadyForResult, get_children, setlocalprogress,\
    GenerateOnAllChildSuccess
import logging
from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata
from taskutils.ndbsharded2 import futurendbshardedpagemap, futurendbshardedmap

def CountAccountsWithFutureExperiment():
    def Go():
        def CountRemaining(futurekey, cursor):
            accounts, cursor, kontinue = Account.query().fetch_page(
                200, start_cursor = cursor
            )
            
            numaccounts = len(accounts)
            
            setlocalprogress(futurekey, numaccounts)
            
            if kontinue:
                lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, numaccounts, lambda x, y: x+y)
        
                future(CountRemaining, parentkey=futurekey, queue="background", onallchildsuccessf=lonallchildsuccessf)(cursor)
                                
                raise FutureReadyForResult("still calculating")
            else:
                return numaccounts
        
        countfuture = future(CountRemaining, queue="background")(None)
        return countfuture.key
        
    return "Count Accounts With Future", Go

def CountAccountsWithFutureShardedMapExperiment():
    def Go():
        futureobj = futurendbshardedpagemap(None, Account.query(), queue="background")
        return futureobj.key
    return "Count Accounts With Future Sharded Map", Go


def SummarizeAccountsWithFutureShardedMapExperiment():
    def Go():
        def mapobj(futurekey, account):
            retval = {}
            
            if account:
                resultkey = "balance=%s" % (account.balance if account.balance else 0)
                retval[resultkey] = 1
            
            return retval

        def mappage(futurekey, keys):
            retval = {}

            for key in keys:            
                account = key.get()
                if account:
                    resultkey = "balance=%s" % (account.balance if account.balance else 0)
                    retval[resultkey] = retval.get(resultkey, 0) + 1
            
            return retval

        def combineresults(result1, result2):
            retval = {}
            def addresults(result):
                for key, value in result.items():
                    retval[key] = retval.get(key, 0) + value
            addresults(result1)
            addresults(result2)
            return retval
        
        futureobj = futurendbshardedpagemap(mappage, Account.query(), initialresult = {}, oncombineresultsf=combineresults, queue="background")
        return futureobj.key
    return "Summarize Accounts With Future Sharded Map", Go

def CountAllUnderscoreEntitiesExperiment():
    def Go():
        @future(queue = "background")
        def CountAllUnderscore(futurekey):
            def CountRemaining(futurekey, kind, cursor):
                accounts, cursor, kontinue = ndb.Query(kind = kind).fetch_page(
                    100, start_cursor = cursor
                )
                
                numaccounts = len(accounts)
                
                setlocalprogress(futurekey, numaccounts)
                
                if kontinue:
                    lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, numaccounts, lambda x,y: x + y)
            
                    future(CountRemaining, parentkey=futurekey, queue="background", onallchildsuccessf=lonallchildsuccessf)(kind, cursor)
                                    
                    raise FutureReadyForResult("still calculating")
                else:
                    return numaccounts
            
            didone = False
            for kind in metadata.get_kinds():
                if kind and kind[0] == "_":
                    didone = True
                    logging.debug("----------------- %s" % kind)
                    lonallchildsuccessf = GenerateOnAllChildSuccess(futurekey, 0, lambda x,y: x + y)
                    future(CountRemaining, parentkey=futurekey, queue="background", onallchildsuccessf=lonallchildsuccessf)(kind, None)

            if didone:
                raise FutureReadyForResult("still calculating")
            else:
                return 0
            
        countfuture = CountAllUnderscore()
        return countfuture.key
    return "Count All Underscore Entities", Go

