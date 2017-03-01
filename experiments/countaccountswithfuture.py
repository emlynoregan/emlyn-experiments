from model.account import Account
from taskutils import future, FutureUnderwayError, futureshardedmap
import logging
from google.appengine.ext import ndb
from google.appengine.ext.ndb import metadata
from taskutils.future import get_children
from taskutils.sharded import futureshardedpagemap

def CountAccountsWithFutureExperiment():
    def Go():
        def CountRemaining(cursor, futurekey):
            accounts, cursor, kontinue = Account.query().fetch_page(
                100, start_cursor = cursor
            )
            
            numaccounts = len(accounts)
            
            if kontinue:
                def OnSuccess(childfuture):
                    parentfuture = futurekey.get()
                    try:
                        if childfuture and parentfuture:
                            result = numaccounts + childfuture.get_result()
                            parentfuture.set_success(result)
                    except Exception, ex:
                        logging.exception(ex)
                        raise ex
#                     if childfuture:
#                         childfuture.key.delete()
                        
                def OnFailure(childfuture):
                    parentfuture = futurekey.get()
                    if childfuture and parentfuture:
                        try:
                            childfuture.get_result() # should throw
                        except Exception, ex:
                            parentfuture.set_failure(ex)
#                     if childfuture:
#                         childfuture.key.delete()
        
                future(CountRemaining, parentkey=futurekey, includefuturekey=True, queue="background", onsuccessf=OnSuccess, onfailuref=OnFailure)(cursor)
                                
                raise FutureUnderwayError("still calculating")
            else:
                return numaccounts
        
        countfuture = future(CountRemaining, includefuturekey=True, queue="background")(None)
        return countfuture.key
        
    return "Count Accounts With Future", Go

def CountAccountsWithFutureShardedMapExperiment():
    def Go():
        futureobj = futureshardedpagemap(None, Account.query(), queue="background")
        return futureobj.key
    return "Count Accounts With Future Sharded Map", Go

def CountAllUnderscoreEntitiesExperiment():
    def Go():
        @future(includefuturekey=True, queue = "background")
        def CountAllUnderscore(futurekey):
            def CountRemaining(kind, cursor, futurekey):
                accounts, cursor, kontinue = ndb.Query(kind = kind).fetch_page(
                    100, start_cursor = cursor
                )
                
                numaccounts = len(accounts)
                
                if kontinue:
                    def OnSuccess(childfuture):
                        parentfuture = futurekey.get()
                        try:
                            if childfuture and parentfuture:
                                result = numaccounts + childfuture.get_result()
                                parentfuture.set_success(result)
                        except Exception, ex:
                            logging.exception(ex)
                            raise ex
                            
                    def OnFailure(childfuture):
                        parentfuture = futurekey.get()
                        if childfuture and parentfuture:
                            try:
                                childfuture.get_result() # should throw
                            except Exception, ex:
                                parentfuture.set_failure(ex)
            
                    future(CountRemaining, parentkey=futurekey, includefuturekey=True, queue="background", onsuccessf=OnSuccess, onfailuref=OnFailure)(kind, cursor)
                                    
                    raise FutureUnderwayError("still calculating")
                else:
                    return numaccounts
            
            def OnSuccess(childfuture):
                parentfuture = futurekey.get()
                children = get_children(futurekey)
                result = 0
                noresult = False
                for child in children:
                    if child.has_result():
                        try:
                            result += child.get_result()
                        except Exception, ex:
                            parentfuture.set_failure(ex)
                            break
                    else:
                        noresult = True
                        break
                
                if not noresult:
                    parentfuture.set_success(result)
                    
            def OnFailure(childfuture):
                parentfuture = futurekey.get()
                if childfuture and parentfuture:
                    try:
                        childfuture.get_result() # should throw
                    except Exception, ex:
                        parentfuture.set_failure(ex)

            didone = False
            for kind in metadata.get_kinds():
                if kind and kind[0] == "_":
                    didone = True
                    logging.debug("----------------- %s" % kind)
                    future(CountRemaining, parentkey=futurekey, includefuturekey=True, queue="background", onsuccessf=OnSuccess, onfailuref=OnFailure)(kind, None)

            if didone:
                raise FutureUnderwayError("still calculating")
            else:
                return 0
            
        countfuture = CountAllUnderscore()
        return countfuture.key
    return "Count All Underscore Entities", Go
