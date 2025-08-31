from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2 import OperationalError
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json
from opensearchpy import OpenSearch
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from contextlib import contextmanager
import logging
import time
import pandas as pd
import requests
from datetime import datetime
import uuid

app = FastAPI(title="Data Integration Service", description="Foundry-like data integration and pipeline orchestration")

DB_CONN = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'dbname': os.environ['DB_NAME'],
}

_connection_pool = None
_kafka_producer = None
_opensearch_client = None

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

@contextmanager
def get_db():
    pool = get_connection_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        yield conn
    finally:
        pool.putconn(conn)

class DataSource(BaseModel):
    id: Optional[str] = None
    name: str
    type: str  # 'database', 'api', 'file', 'kafka'
    connection_config: Dict[str, Any]
    description: Optional[str] = None

class DataPipeline(BaseModel):
    id: Optional[str] = None
    name: str
    source_id: str
    transformations: List[Dict[str, Any]]
    destination: Dict[str, Any]
    schedule: Optional[str] = None
    status: Optional[str] = "inactive"

class PipelineExecution(BaseModel):
    id: Optional[str] = None
    pipeline_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    records_processed: Optional[int] = 0
    error_message: Optional[str] = None

@app.get("/health")
def health():
    return {"status": "ok", "service": "data-integration"}

@app.post("/data-sources")
def create_data_source(source: DataSource):
    source.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_sources (id, name, type, connection_config, description)
                VALUES (%s, %s, %s, %s, %s)
            """, (source.id, source.name, source.type, json.dumps(source.connection_config), source.description))
    
    producer = get_kafka_producer()
    producer.send('data.source.created', source.dict())
    
    return {"id": source.id, "message": "Data source created"}

@app.get("/data-sources")
def list_data_sources():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, type, connection_config, description FROM data_sources")
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1], 
            "type": row[2],
            "connection_config": json.loads(row[3]) if row[3] else {},
            "description": row[4]
        }
        for row in rows
    ]

@app.post("/data-pipelines")
def create_data_pipeline(pipeline: DataPipeline):
    pipeline.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO data_pipelines (id, name, source_id, transformations, destination, schedule, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                pipeline.id, pipeline.name, pipeline.source_id,
                json.dumps(pipeline.transformations), json.dumps(pipeline.destination),
                pipeline.schedule, pipeline.status
            ))
    
    producer = get_kafka_producer()
    producer.send('data.pipeline.created', pipeline.dict())
    
    return {"id": pipeline.id, "message": "Data pipeline created"}

@app.get("/data-pipelines")
def list_data_pipelines():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, source_id, transformations, destination, schedule, status
                FROM data_pipelines
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "source_id": row[2],
            "transformations": json.loads(row[3]) if row[3] else [],
            "destination": json.loads(row[4]) if row[4] else {},
            "schedule": row[5],
            "status": row[6]
        }
        for row in rows
    ]

@app.post("/data-pipelines/{pipeline_id}/execute")
def execute_pipeline(pipeline_id: str, background_tasks: BackgroundTasks):
    execution_id = str(uuid.uuid4())
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_executions (id, pipeline_id, status, started_at)
                VALUES (%s, %s, %s, %s)
            """, (execution_id, pipeline_id, "running", datetime.now()))
    
    background_tasks.add_task(run_pipeline_execution, pipeline_id, execution_id)
    
    return {"execution_id": execution_id, "status": "started"}

def run_pipeline_execution(pipeline_id: str, execution_id: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, source_id, transformations, destination
                    FROM data_pipelines WHERE id = %s
                """, (pipeline_id,))
                pipeline_data = cur.fetchone()
                
                if not pipeline_data:
                    raise Exception("Pipeline not found")
                
                name, source_id, transformations_json, destination_json = pipeline_data
                transformations = json.loads(transformations_json) if transformations_json else []
                destination = json.loads(destination_json) if destination_json else {}
                
                cur.execute("""
                    SELECT name, type, connection_config
                    FROM data_sources WHERE id = %s
                """, (source_id,))
                source_data = cur.fetchone()
                
                if not source_data:
                    raise Exception("Data source not found")
                
                source_name, source_type, config_json = source_data
                config = json.loads(config_json) if config_json else {}
                
                data = extract_data(source_type, config)
                
                for transformation in transformations:
                    data = apply_transformation(data, transformation)
                
                records_processed = load_data(data, destination)
                
                cur.execute("""
                    UPDATE pipeline_executions 
                    SET status = %s, completed_at = %s, records_processed = %s
                    WHERE id = %s
                """, ("completed", datetime.now(), records_processed, execution_id))
                
                producer = get_kafka_producer()
                producer.send('data.pipeline.completed', {
                    "execution_id": execution_id,
                    "pipeline_id": pipeline_id,
                    "records_processed": records_processed
                })
                
    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE pipeline_executions 
                    SET status = %s, completed_at = %s, error_message = %s
                    WHERE id = %s
                """, ("failed", datetime.now(), str(e), execution_id))

def extract_data(source_type: str, config: Dict[str, Any]):
    if source_type == "database":
        return extract_from_database(config)
    elif source_type == "api":
        return extract_from_api(config)
    elif source_type == "kafka":
        return extract_from_kafka(config)
    else:
        raise Exception(f"Unsupported source type: {source_type}")

def extract_from_database(config: Dict[str, Any]):
    query = config.get("query", "SELECT * FROM citizens LIMIT 100")
    with get_db() as conn:
        df = pd.read_sql(query, conn)
    return df.to_dict('records')

def extract_from_api(config: Dict[str, Any]):
    url = config.get("url")
    headers = config.get("headers", {})
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def extract_from_kafka(config: Dict[str, Any]):
    topic = config.get("topic")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000
    )
    
    messages = []
    for message in consumer:
        messages.append(message.value)
        if len(messages) >= 100:
            break
    
    consumer.close()
    return messages

def apply_transformation(data: List[Dict], transformation: Dict[str, Any]):
    transform_type = transformation.get("type")
    
    if transform_type == "filter":
        condition = transformation.get("condition")
        return [item for item in data if eval_condition(item, condition)]
    elif transform_type == "map":
        mapping = transformation.get("mapping")
        return [apply_mapping(item, mapping) for item in data]
    elif transform_type == "aggregate":
        return apply_aggregation(data, transformation)
    else:
        return data

def eval_condition(item: Dict, condition: Dict):
    field = condition.get("field")
    operator = condition.get("operator")
    value = condition.get("value")
    
    item_value = item.get(field)
    
    if operator == "equals":
        return item_value == value
    elif operator == "greater_than":
        return item_value > value
    elif operator == "less_than":
        return item_value < value
    elif operator == "contains":
        return value in str(item_value)
    else:
        return True

def apply_mapping(item: Dict, mapping: Dict):
    result = {}
    for new_field, source_field in mapping.items():
        if source_field in item:
            result[new_field] = item[source_field]
    return result

def apply_aggregation(data: List[Dict], transformation: Dict):
    group_by = transformation.get("group_by")
    aggregations = transformation.get("aggregations", [])
    
    if not group_by:
        return data
    
    df = pd.DataFrame(data)
    grouped = df.groupby(group_by)
    
    result = []
    for name, group in grouped:
        agg_result = {group_by: name}
        for agg in aggregations:
            field = agg.get("field")
            operation = agg.get("operation")
            if operation == "count":
                agg_result[f"{field}_count"] = len(group)
            elif operation == "sum":
                agg_result[f"{field}_sum"] = group[field].sum()
            elif operation == "avg":
                agg_result[f"{field}_avg"] = group[field].mean()
        result.append(agg_result)
    
    return result

def load_data(data: List[Dict], destination: Dict[str, Any]):
    dest_type = destination.get("type")
    
    if dest_type == "opensearch":
        return load_to_opensearch(data, destination)
    elif dest_type == "database":
        return load_to_database(data, destination)
    elif dest_type == "kafka":
        return load_to_kafka(data, destination)
    else:
        logging.info(f"Loaded {len(data)} records to {dest_type}")
        return len(data)

def load_to_opensearch(data: List[Dict], destination: Dict[str, Any]):
    client = get_opensearch_client()
    if not client:
        raise Exception("OpenSearch client not available")
    
    index_name = destination.get("index", "data_pipeline_output")
    
    for record in data:
        client.index(index=index_name, body=record)
    
    return len(data)

def load_to_database(data: List[Dict], destination: Dict[str, Any]):
    table_name = destination.get("table")
    if not table_name:
        raise Exception("Database destination requires table name")
    
    with get_db() as conn:
        with conn.cursor() as cur:
            for record in data:
                columns = list(record.keys())
                values = list(record.values())
                placeholders = ','.join(['%s'] * len(values))
                query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
                cur.execute(query, values)
    
    return len(data)

def load_to_kafka(data: List[Dict], destination: Dict[str, Any]):
    topic = destination.get("topic")
    if not topic:
        raise Exception("Kafka destination requires topic name")
    
    producer = get_kafka_producer()
    for record in data:
        producer.send(topic, record)
    
    return len(data)

@app.get("/pipeline-executions/{execution_id}")
def get_pipeline_execution(execution_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, pipeline_id, status, started_at, completed_at, records_processed, error_message
                FROM pipeline_executions WHERE id = %s
            """, (execution_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Execution not found")
            
            return {
                "id": row[0],
                "pipeline_id": row[1],
                "status": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "records_processed": row[5],
                "error_message": row[6]
            }
