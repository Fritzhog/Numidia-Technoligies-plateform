from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
import os
import psycopg2
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
# Token verification is simplified for this prototype

app = FastAPI()
security = HTTPBearer()

DB_CONN = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'dbname': os.environ['DB_NAME'],
}

# Simplified token verification – accepts any token
async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    return credentials.credentials


def get_db():
    conn = psycopg2.connect(**DB_CONN)
    conn.autocommit = True
    return conn

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
