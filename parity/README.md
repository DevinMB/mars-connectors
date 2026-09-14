# Document id parity harness

The CouchDB connector replaces five deployed copies of `DevinMB/data-sinks`
`couchdb-sink.py`. Those sinks are at-least-once, and the *only* thing that stops a
replay from duplicating documents is that a replayed record computes the same CouchDB
`_id`. So the Java port must agree with the Python character for character.

This harness proves it:

```bash
# 1. run the fixtures through the original, unmodified Python
python3 run_python.py            # -> python_results.json

# 2. run the same fixtures through the Java implementation
cd .. && docker run --rm -v "$PWD:/work:z" -v mars_m2:/root/.m2 -w /work \
  maven:3.9-eclipse-temurin-17 \
  mvn -q -pl connector-couchdb test-compile exec:java \
    -Dexec.mainClass=io.mars.connector.couchdb.ParityHarness \
    -Dexec.classpathScope=test \
    -Dexec.args="/work/parity/fixtures.json /work/parity/java_results.json"

# 3. diff
python3 - <<'PY'
import json
py=json.load(open("python_results.json")); jv=json.load(open("java_results.json"))
bad=[(p,j) for p,j in zip(py,jv) if p["id"]!=j["id"] or p["reason"]!=j["reason"]]
print("%d/%d identical" % (len(py)-len(bad), len(py)))
for p,j in bad: print(" ", p["n"], p, j)
PY
```

`python_results.json` is checked in as a golden file and asserted by
`DocIdParityTest`, so a regression fails the build rather than waiting to be noticed
as a quietly growing database.

## What this caught

The first run diverged on one fixture: Python renders a float `{ts}` token as
`1726300000.75`, while Jackson's `asText()` produced `1.72630000075E9`. Any template
using `{ts}` would have written a second document per record instead of upserting.
Fixed by `DocIdStrategy.pythonFloat()`.

## Regenerating the golden file

Only when the Python side genuinely changes — never to make a failing test pass.
The source of truth is
`/var/lib/docker/volumes/health-metrics-sink_data_sinks_code/_data/data-sinks-v1.3.4/couchdb-sink.py`
(v1.3.4 is a superset of the four other deployed versions).
