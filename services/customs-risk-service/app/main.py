from fastapi import FastAPI, Depends
from pydantic import BaseModel
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from kafka import KafkaProducer
import json
from opensearchpy import OpenSearch
from contextlib import contextmanager
import logging

app = FastAPI()

# Database connection
DB_CONN = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'dbname': os.environ['DB_NAME'],
}

_connection_pool = None

def get_connection_pool():
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            **DB_CONN
        )
    return _connection_pool

logging.basicConfig(level=logging.INFO)

# Kafka producer
_kafka_producer = None

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
    return _kafka_producer

# OpenSearch client
os_client = OpenSearch(os.environ['OPENSEARCH_HOST'], verify_certs=False)

class Declaration(BaseModel):
    nin: str
    goods: str
    value: int
    origin_country: str


@contextmanager
def get_db():
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        yield conn
    finally:
        pool.putconn(conn)

@app.get("/health")
def health():
    return {"status": "ok"}


def compute_risk(decl: Declaration):
    score = 0
    rationale = []
    if decl.value > 100000:
        score += 70
        rationale.append("Valeur déclarée > 100k")
    if decl.origin_country.upper() in ["KY", "IR", "KP"]:
        score += 50
        rationale.append("Pays d'origine à risque")
    sensitive_goods = ["electronic components", "arms", "pharmaceutical"]
    if decl.goods.lower() in sensitive_goods:
        score += 20
        rationale.append("Marchandise sensible")
    if score >= 100:
        level = "HIGH"
    elif score >= 50:
        level = "MEDIUM"
    else:
        level = "LOW"
    return {"score": score, "level": level, "rationale": ", ".join(rationale)}

@app.post("/declarations")
def create_declaration(decl: Declaration):
    # persist declaration
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO declarations (nin, goods, value, origin_country) VALUES (%s,%s,%s,%s) RETURNING id",
                (decl.nin, decl.goods, decl.value, decl.origin_country),
            )
            decl_id = cur.fetchone()[0]
    # emit declaration event
    producer = get_kafka_producer()
    producer.send(os.environ['KAFKA_DECLARATION_TOPIC'], decl.dict())
    # compute risk and persist
    risk = compute_risk(decl)
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO declaration_risks (declaration_id, score, level, rationale) VALUES (%s,%s,%s,%s)",
                (decl_id, risk['score'], risk['level'], risk['rationale']),
            )
    # index into OpenSearch
    try:
        os_client.index(index="declaration_risks", body={"declaration_id": decl_id, **risk})
    except Exception as e:
        logging.error(f"Failed to index declaration risk to OpenSearch: {e}")
        pass
    # emit risk event
    producer = get_kafka_producer()
    producer.send(os.environ['KAFKA_RISK_TOPIC'], {"declaration_id": decl_id, **risk})
    return {"id": decl_id, "risk": risk}
