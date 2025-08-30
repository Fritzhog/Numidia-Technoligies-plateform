from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor
import os
import json
import datetime
from kafka import KafkaProducer
from contextlib import contextmanager

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
SECRET_KEY = "secret-key"
ALGORITHM = "HS256"

DB_CONN = {
    'dbname': os.getenv("DB_NAME", "numidia"),
    'user': os.getenv("DB_USER", "numidia"),
    'password': os.getenv("DB_PASSWORD", "password"),
    'host': os.getenv("DB_HOST", "postgres"),
}

_connection_pool = None

def get_connection_pool():
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            cursor_factory=RealDictCursor,
            **DB_CONN
        )
    return _connection_pool

# Dummy verification for now
def verify_token(token: str = Depends(oauth2_scheme)):
    # In a real implementation, decode and verify the JWT token
    return {"user_id": "user"}

@contextmanager
def get_db():
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)

# Kafka producer setup
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
_kafka_producer = None

def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        _kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
    return _kafka_producer

@app.get("/health")
def health_check():
        return {"status": "ok"}

class Vehicle(BaseModel):
    vin: str
    owner_nin: str
    vehicle_type: str

@app.post("/vehicles")
def register_vehicle(vehicle: Vehicle, token=Depends(verify_token)):
    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(
                "INSERT INTO vehicles (vin, owner_nin, vehicle_type, registration_date) VALUES (%s, %s, %s, NOW()) RETURNING vin",
                (vehicle.vin, vehicle.owner_nin, vehicle.vehicle_type)
            )
            result = cur.fetchone()
            db.commit()
    vin = result["vin"] if result else vehicle.vin
    event = {
        "vin": vehicle.vin,
        "owner_nin": vehicle.owner_nin,
        "vehicle_type": vehicle.vehicle_type,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    producer = get_kafka_producer()
    producer.send("transport.vehicle.registered", event)
    producer.flush()
    return {"vin": vin}
