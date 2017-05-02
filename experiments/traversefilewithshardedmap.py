import logging
from taskutils.gcsfilesharded import gcsfileshardedpagemap,\
    futuregcsfileshardedpagemap

def LogPage(page):
    for line in page:
        logging.debug(line)

def TraverseFileWithShardedMapExperiment():
    def Go():
        gcsfileshardedpagemap(LogPage, "/emlyn-experiments-samples/UTF-8-demo.txt")            
    return "Traverse File With Sharded Map", Go

def TraverseFileWithFutureShardedMapExperiment():
    def Go():
        futureobj = futuregcsfileshardedpagemap(LogPage, "/emlyn-experiments-samples/UTF-8-demo.txt", queue="background")
        return futureobj.key
    return "Traverse File With Future Sharded Map", Go
