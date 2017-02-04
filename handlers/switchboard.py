from flask import render_template

from experiments.helloworld import HelloWorldExperiment

def get_switchboard(app):
    @app.route('/')
    def switchboard():
        experiments = [
            HelloWorldExperiment()
        ]
        
        return render_template("switchboard.html", experiments = experiments)

    return switchboard

