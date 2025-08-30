from fastapi import FastAPI, Depends
from pydantic import BaseModel
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import OperationalError
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from contextlib import contextmanager
import logging
import time

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
        max_retries = 5
        for attempt in range(max_retries):
            try:
                _connection_pool = SimpleConnectionPool(
                    minconn=1,
                    maxconn=10,
                    **DB_CONN
                )
                break
            except OperationalError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logging.warning(f"Database connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Database connection failed after {max_retries} attempts: {e}")
                    raise
    return _connection_pool

logging.basicConfig(level=logging.INFO)

# Kafka producer
_kafka_producer = None

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        max_retries = 5
        for attempt in range(max_retries):
            try:
                _kafka_producer = KafkaProducer(
                    bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                )
                break
            except NoBrokersAvailable as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logging.warning(f"Kafka connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logging.error(f"Kafka connection failed after {max_retries} attempts: {e}")
                    raise
    return _kafka_producer

# OpenSearch client
_opensearch_client = None

def get_opensearch_client():
    global _opensearch_client
    if _opensearch_client is None:
        max_retries = 3
        for attempt in range(max_retries):
            try:
                _opensearch_client = OpenSearch(os.environ['OPENSEARCH_HOST'], verify_certs=False)
                _opensearch_client.info()
                break
            except (OpenSearchConnectionError, Exception) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logging.warning(f"OpenSearch connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}")
                    time.sleep(wait_time)
                else:
                    logging.warning(f"OpenSearch connection failed after {max_retries} attempts: {e}")
                    _opensearch_client = None
                    break
    return _opensearch_client

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
        os_client = get_opensearch_client()
        if os_client:
            os_client.index(index="declaration_risks", body={"declaration_id": decl_id, **risk})
    except Exception as e:
        logging.error(f"Failed to index declaration risk to OpenSearch: {e}")
        pass
    # emit risk event
    producer = get_kafka_producer()
    producer.send(os.environ['KAFKA_RISK_TOPIC'], {"declaration_id": decl_id, **risk})
    return {"id": decl_id, "risk": risk}
