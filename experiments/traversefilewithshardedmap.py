import logging
from taskutils.gcsfilesharded import gcsfileshardedpagemap,\
    futuregcsfileshardedpagemap, futuregcscompose
from experiments.gcsfilecombine import makefiles, deletefiles, countfiles
from taskutils.future import future, futuresequence

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

def MakeGCSFiles(futurekey, amount):
    return makefiles(futurekey, "/emlyn-experiments-samples/source", amount)
    
def Make100FilesExperiment():
    def Go():
        futureobj = future(MakeGCSFiles)(100)
        return futureobj.key
    return "Make 100 GCS Files", Go

def Make1000FilesExperiment():
    def Go():
        futureobj = future(MakeGCSFiles)(1000)
        return futureobj.key
    return "Make 1000 GCS Files", Go

def Make10000FilesExperiment():
    def Go():
        futureobj = future(MakeGCSFiles)(10000)
        return futureobj.key
    return "Make 10000 GCS Files", Go

def DeleteFilesExperiment():
    def Go():
        def DeleteSource(futurekey):
            return deletefiles(futurekey, "/emlyn-experiments-samples/source")
        def DeleteTarget(futurekey, result):
            return deletefiles(futurekey, "/emlyn-experiments-samples/target")
        def DeleteWorking(futurekey, result):
            return deletefiles(futurekey, "/emlyn-experiments-samples/working")
        
        futureobj = futuresequence([DeleteSource, DeleteTarget, DeleteWorking], futurenameprefix="delete")()
        return futureobj.key
    return "Delete GCS Files", Go

def GCSFileCombine1Experiment():
    def Go():
        futureobj = futuregcscompose("emlyn-experiments-samples", "source", "target")
        return futureobj.key
    return "Combine GCS Files to 1", Go

def CountFilesSourceExperiment():
    def Go():
        futureobj = future(countfiles)("/emlyn-experiments-samples/source")
        return futureobj.key
    return "Count Source Files", Go

def CountFilesTargetExperiment():
    def Go():
        futureobj = future(countfiles)("/emlyn-experiments-samples/target")
        return futureobj.key
    return "Count Target Files", Go

# def ComposeTargetFilesExperiment():
#     def Go():
#         futureobj = future(composefiles)("emlyn-experiments-samples", "target")
#         return futureobj.key
#     return "Compose Target Files", Go
