from google.appengine.api import taskqueue

def MaxTaskSizeExperiment():
    def Go():
        def EnqueueTaskWithPayload(payloadsize):
            ltask = taskqueue.Task("x" * payloadsize)
            ltask.add("default")
        EnqueueTaskWithPayload(1000010)
    return "Max Task Size Experiment", Go
