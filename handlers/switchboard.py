from flask import render_template, request, redirect

from experiments.helloworld import HelloWorldExperiment
from experiments.incrementaccountsnaive import IncrementAccountsExperimentNaive
from experiments.incrementaccountswithtask import IncrementAccountsWithTaskExperiment
from experiments.incrementaccountswithshardedmap import IncrementAccountsWithShardedMapExperiment, IncrementAccountsWithFutureShardedMapExperiment
from experiments.deleteaccountswithshardedmap import DeleteAccountsWithShardedMapExperiment, DeleteAccountsWithFutureShardedMapExperiment,\
    DeleteAccountsWithShardedPageMapExperiment
from experiments.makeaccounts import MakeAccountsExperiment
from experiments.countaccountswithfuture import CountAccountsWithFutureExperiment, CountAccountsWithFutureShardedMapExperiment, CountAllUnderscoreEntitiesExperiment
from experiments.maxtasksize import MaxTaskSizeExperiment

def get_switchboard(app):
    experiments = [
        CountAllUnderscoreEntitiesExperiment(),
        CountAccountsWithFutureShardedMapExperiment(),
        IncrementAccountsWithFutureShardedMapExperiment(),
        DeleteAccountsWithFutureShardedMapExperiment(),
        CountAccountsWithFutureExperiment(),
        IncrementAccountsWithShardedMapExperiment(),
        IncrementAccountsWithTaskExperiment(),
        IncrementAccountsExperimentNaive(),
        DeleteAccountsWithShardedPageMapExperiment(),
        DeleteAccountsWithShardedMapExperiment(),
        MakeAccountsExperiment(),
        MaxTaskSizeExperiment(),
        HelloWorldExperiment()
    ]

    @app.route('/', methods=["GET", "POST"])
    def switchboard():
        if request.method == "GET":
            return render_template("switchboard.html", experiments = enumerate(experiments))
        else:
            if not request.form.get("run") is None:
                index = int(request.form.get("run"))
                experiment = experiments[index]
                resultobj = experiment[1]()
                reporturl = "report%s" % (("?key=%s" % resultobj.urlsafe()) if resultobj else "")
                return redirect(reporturl)
