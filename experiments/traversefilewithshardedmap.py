import logging
from taskutils.gcsfilesharded import gcsfileshardedpagemap

def TraverseFileWithShardedMapExperiment():
    def Go():
        def LogPage(page):
            for line in page:
                logging.debug(line)

        gcsfileshardedpagemap(LogPage, "/emlyn-experiments-samples/UTF-8-demo.txt")            
    return "Traverse File With Sharded Map", Go
