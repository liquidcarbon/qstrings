import json
from robyn import Robyn
from qstrings.config import log
from qstrings.Q import Q

app = Robyn(__file__)


@app.post("/")
def query(body):
    data = json.loads(body)
    log.info(data)
    s = data.pop("q", "")
    file = data.pop("file", "")
    engine = data.pop("engine", "duckdb")
    q = Q(s, file=file, **data)
    res = q.run(engine=engine, **data)
    return res.fetchall()


app.start(port=7117, host="0.0.0.0")
