import cloudstorage as gcs
# from taskutils.future import future, GetFutureAndCheckReady,\
#     FutureNotReadyForResult, GenerateOnAllChildSuccess, FutureReadyForResult,\
#     setlocalprogress, GenerateStableId
import logging
import uuid
from taskutils.future import setlocalprogress
# from google.cloud import storage  #@UnresolvedImport

def makefiles(futurekey, gcspath, amount):
    for ix in range(amount):
        if not ix % 10:
            setlocalprogress(futurekey, ix)
        
        lfilename = "%s/file-%s.txt" % (gcspath, str(uuid.uuid4()))
        
        write_retry_params = gcs.RetryParams(urlfetch_timeout=60)
        with gcs.open(lfilename, 'w', content_type='text/plain', retry_params=write_retry_params) as loutfile:
            loutfile.write("text from file-%s\n" % ix)

    setlocalprogress(futurekey, amount)
    return amount

def countfiles(futurekey, gcspath):
    write_retry_params = gcs.RetryParams(urlfetch_timeout=60)
    return len(list(gcs.listbucket(gcspath, retry_params=write_retry_params)))

def deletefiles(futurekey, gcspath):
    logging.debug("Enter deletefiles: %s, %s" % (futurekey, gcspath))
    write_retry_params = gcs.RetryParams(urlfetch_timeout=60)
    retval = 0
    for ix, lfilestat in enumerate(gcs.listbucket(gcspath, retry_params=write_retry_params)):
        if not ix % 10:
            setlocalprogress(futurekey, ix)
        gcs.delete(lfilestat.filename)
        retval += 1
    setlocalprogress(futurekey, retval)
    return retval

# # def composefiles(futurekey, gcsbucket, gcspath):
# #     lgcsclient = storage.Client()
# #     bucket = lgcsclient.get_bucket(gcsbucket)
# #     blobs = list(bucket.list_blobs(prefix = gcspath))[:32]
# #     newblob = bucket.blob("composed")
# #     newblob.content_type = "text/plain"
# #     newblob.compose(blobs, )
# #     bucket.delete_blobs(blobs)
# #     setlocalprogress(futurekey, len(blobs))
# #     return len(blobs)
# 
# 
# def CalculateFileRanges(startindex, finishindex, numranges):
#     amount = finishindex - startindex
#     partitions = [((i * amount) / numranges) + startindex for i in range(numranges)] + [finishindex]
#     ranges = [(partitions[i], partitions[i+1]) for i in range(numranges)]
#     return ranges
# #     excludedcount = len([elem for elem in ranges if elem[1] - elem[0] <= 1])
# #     finalranges = [elem for elem in ranges if elem[1] - elem[0] > 1]
# #     return excludedcount, finalranges
# 
# def listbucket(gcsbucket, gcsprefix):
#     lgcsclient = storage.Client()
#     bucket = lgcsclient.get_bucket(gcsbucket)
#     return bucket.list_blobs(prefix = gcsprefix)
# 
# def copyblob(gcsbucket, oldblob, newblobname):
#     lgcsclient = storage.Client()
#     bucket = lgcsclient.get_bucket(gcsbucket)
#     newblob = bucket.blob(newblobname)
#     newblob.content_type = "text/plain"
# 
#     oldblobbuffer = oldblob.download_as_string()
#     newblob.upload_from_string(oldblobbuffer)
# #     token, b1, b2 = newblob.rewrite(oldblob)
# #     logging.debug("token: %s, %s, %s" % (token, b1, b2))
# #     while token:
# #         token, b1, b2 = newblob.rewrite(oldblob, token)
# #         logging.debug("token: %s, %s, %s" % (token, b1, b2))
#     
# 
# # def copyblob(gcsbucket, oldblob, newblobname):
# #     lgcsclient = storage.Client()
# #     bucket = lgcsclient.get_bucket(gcsbucket)
# #     bucket.rename_blob(oldblob, newblobname)
# 
# def composeblobs(gcsbucket, newblobname, blobs):
#     lgcsclient = storage.Client()
#     bucket = lgcsclient.get_bucket(gcsbucket)
#     ltotalcomponent_count = sum([blob.component_count for blob in blobs if blob and blob.component_count])
#     newblob = bucket.blob(newblobname)
#     newblob.content_type = "text/plain"
#     newblob.compose(blobs)
#     return {
#         "blobname": newblobname,
#         "count": len(blobs),
#         "component_count": ltotalcomponent_count
#     }
# 
# def deleteblobs(gcsbucket, blobs):
#     lgcsclient = storage.Client()
#     bucket = lgcsclient.get_bucket(gcsbucket)
#     bucket.delete_blobs(blobs)
# 
# def getblobsbyname(gcsbucket, *blobnames):
#     lgcsclient = storage.Client()
#     bucket = lgcsclient.get_bucket(gcsbucket)
#     retval = [bucket.get_blob(blobname) for blobname in blobnames]
#     retval = [blob for blob in retval if blob]
#     return retval
# 
# # def getfilename(pathprefix, fileprefix):
# #     return "%s/%s-%s" % (pathprefix, fileprefix, str(uuid.uuid4()))
# 
# def futuregcscomposefiles(gcsbucket=None, gcssourceprefix=None, gcstargetprefix=None, gcstargetfilename="output.txt", onsuccessf=None, onfailuref=None, onprogressf = None, initialresult = None, oncombineresultsf = None, weight = None, parentkey=None, **taskkwargs):
#     numgcsfiles = len(list(listbucket(gcsbucket, gcssourceprefix)))
#     
#     def GCSCombineToTarget(futurekey, startindex, finishindex, istop, **kwargs):
#         logging.debug("Enter GCSCombineToTarget: %s, %s" % (startindex, finishindex))
#         try:
#             def higherlevelcompose(lop, rop):
#                 try:
#                     retval = None
#                     if lop and rop:
#                         blobnames = [lop.get("blobname"), rop.get("blobname")]
#                         blobs = getblobsbyname(gcsbucket, *blobnames)
#                         if len(blobs) == 2:
#                             ltotalcomponent_count = sum([blob.component_count for blob in blobs])
#                             logging.debug("ltotalcomponent_count: %s" % ltotalcomponent_count)
#                             if ltotalcomponent_count > 1020:
#                                 logging.debug("doing copying")
#                                 newblobnames = ["%s-copy" % blobname for blobname in blobnames]
#                                 for ix, blob in enumerate(blobs):
#                                     try:
#                                         copyblob(gcsbucket, blob, newblobnames[ix])
#                                     except Exception:
#                                         logging.exception("deleteblobs(copy)")
#                                 try:
#                                     deleteblobs(gcsbucket, blobs)
#                                 except Exception:
#                                     logging.exception("deleteblobs(copy)")
#                                 
#                                 blobnames = newblobnames
#                                 blobs = getblobsbyname(gcsbucket, *blobnames)
#                                                         
#                             if len(blobs) == 2:
#                                 llocalfilename = gcstargetfilename if istop else GenerateStableId(blobnames[0] + blobnames[1])
#                                 lfilename = "%s/%s-%s" % (gcstargetprefix, "composed", llocalfilename)
#                                 retval = composeblobs(gcsbucket, lfilename, blobs)
#                                 retval["count"] = lop.get("count", 0) + rop.get("count", 0)
#                                 try:
#                                     deleteblobs(gcsbucket, blobs)
#                                 except Exception:
#                                     logging.exception("deleteblobs")
#                         else:
#                             raise Exception("Can't load blobs")
#                     else:
#                         retval = lop if lop else rop
#                     return retval
#                 except Exception, ex:
#                     logging.exception("higherlevelcompose")
#                     raise ex
#             
#             onallchildsuccessf = GenerateOnAllChildSuccess(futurekey, None, higherlevelcompose)
#             
#             numfiles = finishindex - startindex
#             
#             if numfiles > 32:
#                 ranges = CalculateFileRanges(startindex, finishindex, 2)
#                 logging.debug("ranges:%s" % ranges)
#                 for r in ranges:
#                     futurename = "split %s" % (r, )
#                     future(GCSCombineToTarget, futurename=futurename, onallchildsuccessf=onallchildsuccessf, parentkey=futurekey, weight = r[1]-r[0], **taskkwargs)(r[0], r[1], False)
#                 raise FutureReadyForResult()
#             else:
#                 lblobs = list(listbucket(gcsbucket, gcssourceprefix))[startindex:finishindex]
#                 lfilename = "%s/%s-%s-%s" % (gcstargetprefix, "composed", startindex, finishindex)
#                 retval = composeblobs(gcsbucket, lfilename, lblobs)
#                 return retval
#         finally:
#             logging.debug("Leave GCSCombineToTarget: %s, %s" % (startindex, finishindex))
#     
#     futurename = "gcscombinetotarget %s" % (numgcsfiles)
# 
#     return future(GCSCombineToTarget, futurename=futurename, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey=parentkey, weight = numgcsfiles, **taskkwargs)(0, numgcsfiles, True)
# 
#             
# # def futuregcsfilecombine(gcssourcepath=None, gcstargetpath=None, targetnumfiles=1000, onsuccessf=None, onfailuref=None, onprogressf = None, initialresult = None, oncombineresultsf = None, weight = 1, parentkey=None, **taskkwargs):
# # 
# #     numgcsfiles = len(list(gcs.listbucket(gcssourcepath)))
# # 
# #     def GCSCombineRange(futurekey, startindex, finishindex, **kwargs):
# #         logging.debug("Enter GCSCombineRange: %s, %s" % (startindex, finishindex))
# #         
# #         lgcsfilenames = [lfilestat.filename for lfilestat in list(gcs.listbucket(gcssourcepath))[startindex:finishindex]]
# # 
# #         lgcsoutfilename = "combine-%s" % str(uuid.uuid4())
# #         logging.debug("pre")
# #         with gcs.open("%s/%s" % (gcstargetpath, lgcsoutfilename), "w", content_type = "text/plain") as outf:
# #             logging.debug("opened outfile")
# #             for ix, lgcsfilename in enumerate(lgcsfilenames):
# #                 with gcs.open(lgcsfilename) as inf:
# #                     logging.debug("begin read + write")
# #                     if not ix % 10:
# #                         setlocalprogress(futurekey, ix)
# #                     outf.write(inf.read())
# #                     logging.debug("end read + write")
# # 
# #         logging.debug("Leave GCSCombineRange: %s, %s" % (startindex, finishindex))
# #         return finishindex - startindex
# #     
# #     def GCSCombineToTarget(futurekey, startindex, finishindex, numpieces, **kwargs):
# #         logging.debug("Enter GCSCombineToTarget: %s, %s, %s" % (startindex, finishindex, numpieces))
# # 
# #         onallchildsuccessf = GenerateOnAllChildSuccess(futurekey, 0, lambda x, y: x + y)
# #         
# #         if numpieces > 5:
# #             ranges = CalculateFileRanges(startindex, finishindex, 5)
# #             numpieceranges = CalculateFileRanges(0, numpieces, 5)
# #             logging.debug("ranges:%s" % ranges)
# #             for ix, r in enumerate(ranges):
# #                 numpiecesr = numpieceranges[ix][1] - numpieceranges[ix][0]
# # #                 numpiecesr = numpieces / 5 + (1 if numpieces % 5 and not ix else 0)
# #                 futurename = "split %s" % (r, )
# #                 future(GCSCombineToTarget, futurename=futurename, onallchildsuccessf=onallchildsuccessf, parentkey=futurekey, weight = r[1]-r[0], **taskkwargs)(r[0], r[1], numpiecesr)
# #         else:
# #             ranges = CalculateFileRanges(startindex, finishindex, numpieces)
# #             logging.debug("ranges:%s" % ranges)
# #             for ix, r in enumerate(ranges):
# #                 futurename = "combine %s" % (r, )
# #                 future(GCSCombineRange, futurename=futurename, onallchildsuccessf=onallchildsuccessf, parentkey=futurekey, weight = r[1]-r[0], **taskkwargs)(r[0], r[1])
# # 
# #         logging.debug("Leave GCSCombineToTarget: %s, %s, %s" % (startindex, finishindex, numpieces))
# #         raise FutureReadyForResult("combining...")
# #     
# #     futurename = "gcscombinetotarget %s" % (numgcsfiles)
# # 
# #     return future(GCSCombineToTarget, futurename=futurename, onsuccessf = onsuccessf, onfailuref = onfailuref, onprogressf = onprogressf, parentkey=parentkey, weight = numgcsfiles, **taskkwargs)(0, numgcsfiles, targetnumfiles)
# #             
#             
# def futuresequence(fseq, parentkey = None, onsuccessf=None, onfailuref=None, onallchildsuccessf=None, onprogressf=None, weight=None, timeoutsec=1800, maxretries=None, futurenameprefix=None, **taskkwargs):
#     flist = list(fseq)
#     
#     taskkwargs["futurename"] = "%s (top level)" % futurenameprefix if futurenameprefix else "-"
#     
#     @future(parentkey = parentkey, onsuccessf = onsuccessf, onfailuref = onfailuref, onallchildsuccessf=onallchildsuccessf, onprogressf = onprogressf, weight=weight, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)
#     def toplevel(futurekey, *args, **kwargs):
#         
#         def childonsuccessforindex(index):
#             def childonsuccess(childfuturekey):
#                 logging.debug("Enter childonsuccess")
#                 childfuture = GetFutureAndCheckReady(childfuturekey)
#                 
#                 taskkwargs["futurename"] = "%s [%s]" % (futurenameprefix if futurenameprefix else "-", index)
# 
#                 try:    
#                     result = childfuture.get_result()
#                 except Exception, ex:
#                     toplevelfuture = futurekey.get()
#                     if toplevelfuture:
#                         toplevelfuture.set_failure(ex)
#                     else:
#                         raise Exception("Can't load toplevel future for failure")
#                 else:
#                     islast = (index == len(flist))
#                     
#                     if islast:
#                         toplevelfuture = futurekey.get()
#                         if toplevelfuture:
#                             toplevelfuture.set_success_and_readyforesult(result)
#                         else:
#                             raise Exception("Can't load toplevel future for success")
#                     else:
#                         future(flist[index], parentkey=futurekey, onsuccessf=childonsuccessforindex(index+1), weight=weight/len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)(result)
#             
#             return childonsuccess
# 
#         taskkwargs["futurename"] = "%s [0]" % (futurenameprefix if futurenameprefix else "-")
#         future(flist[0], parentkey=futurekey, onsuccessf=childonsuccessforindex(1), weight=weight/len(flist) if weight else None, timeoutsec=timeoutsec, maxretries=maxretries, **taskkwargs)()
#                 
#         raise FutureNotReadyForResult("sequence started")
# 
#     return toplevel
