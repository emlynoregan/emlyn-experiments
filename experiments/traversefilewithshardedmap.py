import logging
from taskutils.gcsfilesharded import gcsfileshardedpagemap,\
    futuregcsfileshardedpagemap

def LogPage(page):
    for line in page:
        logging.debug(line)

def LogPageFuture(futurekey, page):
    for line in page:
        logging.debug(line)
    return len(page)

def TraverseFileWithShardedMapExperiment():
    def Go():
        gcsfileshardedpagemap(LogPage, "/emlyn-experiments-samples/UTF-8-demo.txt")            
    return "Traverse File With Sharded Map", Go

def TraverseFileWithFutureShardedMapExperiment():
    def Go():
        futureobj = futuregcsfileshardedpagemap(LogPageFuture, "/emlyn-experiments-samples/UTF-8-demo.txt", queue="background")
        return futureobj.key
    return "Traverse File With Future Sharded Map", Go
