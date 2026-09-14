"""Run fixtures through the ORIGINAL data-sinks v1.3.4 compute_doc_id, unmodified."""
import json, sys, types

SRC = "/var/lib/docker/volumes/health-metrics-sink_data_sinks_code/_data/data-sinks-v1.3.4/couchdb-sink.py"

# The module imports kafka/couchdb/dotenv at top level; stub them so we can exec it
# without those packages installed, and without running main().
for name in ("kafka", "couchdb", "couchdb.http", "dotenv"):
    m = types.ModuleType(name)
    sys.modules.setdefault(name, m)
sys.modules["kafka"].KafkaConsumer = object
sys.modules["couchdb"].Server = object
sys.modules["couchdb"].http = sys.modules["couchdb.http"]
sys.modules["couchdb.http"].ResourceConflict = type("ResourceConflict", (Exception,), {})
sys.modules["dotenv"].load_dotenv = lambda *a, **k: None

src = open(SRC).read()
g = {"__name__": "not_main"}
exec(compile(src, SRC, "exec"), g)

compute_doc_id = g["compute_doc_id"]

out = []
for f in json.load(open("fixtures.json")):
    # These are module-level globals the function closes over.
    g["COUCHDB_ID_FIELD"] = f["field"]
    g["COUCHDB_ID_TEMPLATE"] = f["tpl"]
    doc = dict(f["doc"])
    doc["kafka-key"] = f["key"]          # the sink stamps this before computing the id
    try:
        doc_id, reason = compute_doc_id(f["mode"], f["key"], doc)
    except Exception as e:
        doc_id, reason = None, "EXCEPTION:%s" % type(e).__name__
    out.append({"n": f["n"], "id": doc_id, "reason": reason})

json.dump(out, open("python_results.json", "w"), indent=1)
for r in out:
    print("%-32s id=%-40s reason=%s" % (r["n"], repr(r["id"]), r["reason"]))
