from model.helloworld import HelloWorld

def HelloWorldExperiment():
    def Go():
        lhw = HelloWorld()
        lhw.put()
        return lhw.key
    
    return "Hello World", Go
