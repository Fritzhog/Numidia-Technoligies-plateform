from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import json
import datetime
from kafka import KafkaProducer

app = FastAPI()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
SECRET_KEY = "secret-key"
ALGORITHM = "HS256"

# Dummy verification for now
def verify_token(token: str = Depends(oauth2_scheme)):
    # In a real implementation, decode and verify the JWT token
    return {"user_id": "user"}

def get_db():
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        cursor_factory=RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()

# Kafka producer setup
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

@app.get("/health")
def health_check():
    return {"status": "transport-vehicle-service healthy"}

@app.post("/vehicles")
def register_vehicle(vehicle: dict, token=Depends(verify_token), db=Depends(get_db)):
    # Insert the vehicle into the database
    with db.cursor() as cur:
        cur.execute(
            "INSERT INTO vehicles (vin, owner_nin, vehicle_type, registration_date) VALUES (%s, %s, %s, NOW()) RETURNING vin",
            (
                vehicle.get("vin"),
                vehicle.get("owner_nin"),
                vehicle.get("vehicle_type")
            )
        )
        result = cur.fetchone()
        db.commit()
    vin = result["vin"] if result else vehicle.get("vin")
    # Emit Kafka event
    event = {
        "vin": vehicle.get("vin"),
        "owner_nin": vehicle.get("owner_nin"),
        "vehicle_type": vehicle.get("vehicle_type"),
        "timestamp": datetime.datetime.utcnow().isoformat()
    }
    producer.send("transport.vehicle.registered", event)
    producer.flush()
    return {"vin": vin}
