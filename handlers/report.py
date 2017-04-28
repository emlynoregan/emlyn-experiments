from google.appengine.ext import ndb
from flask import request, render_template
import json
from taskutils.future import _Future

def get_report(app):
    @app.route('/report', methods=["GET", "POST"])
    def report():
        keystr = request.args.get('key')
        key = ndb.Key(urlsafe=keystr) if keystr else None
        obj = key.get() if key else None
        if obj:
            if request.method == "POST":
                cancel = request.form.get("cancel")
                if cancel:
                    obj.cancel()

            return render_template(
                "report.html", 
                objjson = json.dumps(obj.to_dict(), indent=2, sort_keys=True),
                keystr = keystr,
                iscancellable = isinstance(obj, _Future) and not obj.has_result()
            )
        else:
            return render_template(
                "report.html"
            )
            

    return report
