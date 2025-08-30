from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import OperationalError
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from contextlib import contextmanager
import logging
import time
# Token verification is simplified for this prototype

app = FastAPI()
security = HTTPBearer()

DB_CONN = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'dbname': os.environ['DB_NAME'],
}

_connection_pool = None

logging.basicConfig(level=logging.INFO)

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

# Simplified token verification – accepts any token
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return credentials.credentials


@contextmanager
def get_db():
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        yield conn
    finally:
        pool.putconn(conn)

class Citizen(BaseModel):
    nin: str
    name: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/citizens", dependencies=[Depends(verify_token)])
def create_citizen(citizen: Citizen):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO citizens (nin, name) VALUES (%s,%s) ON CONFLICT (nin) DO NOTHING",
                (citizen.nin, citizen.name),
            )
    return {"message": "created"}

@app.get("/citizens/{nin}", dependencies=[Depends(verify_token)])
def read_citizen(nin: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nin,name FROM citizens WHERE nin=%s", (nin,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Not found")
    return {"nin": row[0], "name": row[1]}

@app.get("/citizens", dependencies=[Depends(verify_token)])
def list_citizens():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT nin,name FROM citizens")
            rows = cur.fetchall()
    return [{"nin": r[0], "name": r[1]} for r in rows]
