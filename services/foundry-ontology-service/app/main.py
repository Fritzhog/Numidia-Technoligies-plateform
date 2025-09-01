from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
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
import uuid
from datetime import datetime
import networkx as nx

app = FastAPI(title="Foundry Ontology Service", description="Digital twin modeling and object relationships")

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

class ObjectType(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    properties: Dict[str, Any]
    parent_type_id: Optional[str] = None

class ObjectInstance(BaseModel):
    id: Optional[str] = None
    type_id: str
    name: str
    properties: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = None

class Relationship(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    source_type_id: str
    target_type_id: str
    properties: Optional[Dict[str, Any]] = None

class ObjectLink(BaseModel):
    id: Optional[str] = None
    relationship_id: str
    source_object_id: str
    target_object_id: str
    properties: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

@app.get("/health")
def health():
    return {"status": "ok", "service": "foundry-ontology"}

@app.post("/object-types")
def create_object_type(obj_type: ObjectType):
    obj_type.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO object_types (id, name, description, properties, parent_type_id)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                obj_type.id, obj_type.name, obj_type.description,
                json.dumps(obj_type.properties), obj_type.parent_type_id
            ))
    
    producer = get_kafka_producer()
    producer.send('ontology.object_type.created', obj_type.dict())
    
    return {"id": obj_type.id, "message": "Object type created"}

@app.get("/object-types")
def list_object_types():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, properties, parent_type_id
                FROM object_types
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "properties": json.loads(row[3]) if row[3] else {},
            "parent_type_id": row[4]
        }
        for row in rows
    ]

@app.get("/object-types/{type_id}")
def get_object_type(type_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, properties, parent_type_id
                FROM object_types WHERE id = %s
            """, (type_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Object type not found")
            
            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "properties": json.loads(row[3]) if row[3] else {},
                "parent_type_id": row[4]
            }

@app.post("/objects")
def create_object_instance(obj: ObjectInstance):
    obj.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO object_instances (id, type_id, name, properties, metadata)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                obj.id, obj.type_id, obj.name,
                json.dumps(obj.properties), json.dumps(obj.metadata or {})
            ))
    
    producer = get_kafka_producer()
    producer.send('ontology.object.created', obj.dict())
    
    return {"id": obj.id, "message": "Object instance created"}

@app.get("/objects")
def list_object_instances(type_id: Optional[str] = None, limit: int = 100):
    with get_db() as conn:
        with conn.cursor() as cur:
            if type_id:
                cur.execute("""
                    SELECT id, type_id, name, properties, metadata
                    FROM object_instances WHERE type_id = %s LIMIT %s
                """, (type_id, limit))
            else:
                cur.execute("""
                    SELECT id, type_id, name, properties, metadata
                    FROM object_instances LIMIT %s
                """, (limit,))
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "type_id": row[1],
            "name": row[2],
            "properties": json.loads(row[3]) if row[3] else {},
            "metadata": json.loads(row[4]) if row[4] else {}
        }
        for row in rows
    ]

@app.get("/objects/{object_id}")
def get_object_instance(object_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT oi.id, oi.type_id, oi.name, oi.properties, oi.metadata,
                       ot.name as type_name, ot.description as type_description
                FROM object_instances oi
                JOIN object_types ot ON oi.type_id = ot.id
                WHERE oi.id = %s
            """, (object_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Object instance not found")
            
            return {
                "id": row[0],
                "type_id": row[1],
                "name": row[2],
                "properties": json.loads(row[3]) if row[3] else {},
                "metadata": json.loads(row[4]) if row[4] else {},
                "type_name": row[5],
                "type_description": row[6]
            }

@app.post("/relationships")
def create_relationship(rel: Relationship):
    rel.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO relationships (id, name, description, source_type_id, target_type_id, properties)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                rel.id, rel.name, rel.description,
                rel.source_type_id, rel.target_type_id,
                json.dumps(rel.properties or {})
            ))
    
    producer = get_kafka_producer()
    producer.send('ontology.relationship.created', rel.dict())
    
    return {"id": rel.id, "message": "Relationship created"}

@app.get("/relationships")
def list_relationships():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT r.id, r.name, r.description, r.source_type_id, r.target_type_id, r.properties,
                       st.name as source_type_name, tt.name as target_type_name
                FROM relationships r
                JOIN object_types st ON r.source_type_id = st.id
                JOIN object_types tt ON r.target_type_id = tt.id
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "source_type_id": row[3],
            "target_type_id": row[4],
            "properties": json.loads(row[5]) if row[5] else {},
            "source_type_name": row[6],
            "target_type_name": row[7]
        }
        for row in rows
    ]

@app.post("/object-links")
def create_object_link(link: ObjectLink):
    link.id = str(uuid.uuid4())
    link.created_at = datetime.now()
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO object_links (id, relationship_id, source_object_id, target_object_id, properties, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                link.id, link.relationship_id, link.source_object_id,
                link.target_object_id, json.dumps(link.properties or {}), link.created_at
            ))
    
    producer = get_kafka_producer()
    producer.send('ontology.object_link.created', link.dict())
    
    return {"id": link.id, "message": "Object link created"}

@app.get("/object-links")
def list_object_links(object_id: Optional[str] = None, relationship_id: Optional[str] = None):
    with get_db() as conn:
        with conn.cursor() as cur:
            if object_id:
                cur.execute("""
                    SELECT ol.id, ol.relationship_id, ol.source_object_id, ol.target_object_id, 
                           ol.properties, ol.created_at,
                           r.name as relationship_name,
                           so.name as source_object_name, to_.name as target_object_name
                    FROM object_links ol
                    JOIN relationships r ON ol.relationship_id = r.id
                    JOIN object_instances so ON ol.source_object_id = so.id
                    JOIN object_instances to_ ON ol.target_object_id = to_.id
                    WHERE ol.source_object_id = %s OR ol.target_object_id = %s
                """, (object_id, object_id))
            elif relationship_id:
                cur.execute("""
                    SELECT ol.id, ol.relationship_id, ol.source_object_id, ol.target_object_id, 
                           ol.properties, ol.created_at,
                           r.name as relationship_name,
                           so.name as source_object_name, to_.name as target_object_name
                    FROM object_links ol
                    JOIN relationships r ON ol.relationship_id = r.id
                    JOIN object_instances so ON ol.source_object_id = so.id
                    JOIN object_instances to_ ON ol.target_object_id = to_.id
                    WHERE ol.relationship_id = %s
                """, (relationship_id,))
            else:
                cur.execute("""
                    SELECT ol.id, ol.relationship_id, ol.source_object_id, ol.target_object_id, 
                           ol.properties, ol.created_at,
                           r.name as relationship_name,
                           so.name as source_object_name, to_.name as target_object_name
                    FROM object_links ol
                    JOIN relationships r ON ol.relationship_id = r.id
                    JOIN object_instances so ON ol.source_object_id = so.id
                    JOIN object_instances to_ ON ol.target_object_id = to_.id
                    LIMIT 100
                """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "relationship_id": row[1],
            "source_object_id": row[2],
            "target_object_id": row[3],
            "properties": json.loads(row[4]) if row[4] else {},
            "created_at": row[5],
            "relationship_name": row[6],
            "source_object_name": row[7],
            "target_object_name": row[8]
        }
        for row in rows
    ]

@app.get("/objects/{object_id}/graph")
def get_object_graph(object_id: str, depth: int = 2):
    graph = nx.DiGraph()
    visited = set()
    
    def build_graph(obj_id: str, current_depth: int):
        if current_depth > depth or obj_id in visited:
            return
        
        visited.add(obj_id)
        
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT oi.id, oi.name, oi.type_id, ot.name as type_name
                    FROM object_instances oi
                    JOIN object_types ot ON oi.type_id = ot.id
                    WHERE oi.id = %s
                """, (obj_id,))
                obj_data = cur.fetchone()
                
                if obj_data:
                    graph.add_node(obj_id, name=obj_data[1], type_id=obj_data[2], type_name=obj_data[3])
                
                cur.execute("""
                    SELECT ol.target_object_id, r.name as relationship_name, to_.name as target_name
                    FROM object_links ol
                    JOIN relationships r ON ol.relationship_id = r.id
                    JOIN object_instances to_ ON ol.target_object_id = to_.id
                    WHERE ol.source_object_id = %s
                """, (obj_id,))
                
                for target_id, rel_name, target_name in cur.fetchall():
                    graph.add_edge(obj_id, target_id, relationship=rel_name)
                    build_graph(target_id, current_depth + 1)
                
                cur.execute("""
                    SELECT ol.source_object_id, r.name as relationship_name, so.name as source_name
                    FROM object_links ol
                    JOIN relationships r ON ol.relationship_id = r.id
                    JOIN object_instances so ON ol.source_object_id = so.id
                    WHERE ol.target_object_id = %s
                """, (obj_id,))
                
                for source_id, rel_name, source_name in cur.fetchall():
                    graph.add_edge(source_id, obj_id, relationship=rel_name)
                    build_graph(source_id, current_depth + 1)
    
    build_graph(object_id, 0)
    
    nodes = [
        {
            "id": node,
            "name": graph.nodes[node].get("name", ""),
            "type_id": graph.nodes[node].get("type_id", ""),
            "type_name": graph.nodes[node].get("type_name", "")
        }
        for node in graph.nodes()
    ]
    
    edges = [
        {
            "source": edge[0],
            "target": edge[1],
            "relationship": graph.edges[edge].get("relationship", "")
        }
        for edge in graph.edges()
    ]
    
    return {
        "center_object_id": object_id,
        "depth": depth,
        "nodes": nodes,
        "edges": edges,
        "node_count": len(nodes),
        "edge_count": len(edges)
    }

@app.get("/ontology/search")
def search_ontology(query: str, object_type: Optional[str] = None, limit: int = 50):
    with get_db() as conn:
        with conn.cursor() as cur:
            if object_type:
                cur.execute("""
                    SELECT oi.id, oi.name, oi.type_id, ot.name as type_name,
                           oi.properties, oi.metadata
                    FROM object_instances oi
                    JOIN object_types ot ON oi.type_id = ot.id
                    WHERE ot.name = %s AND (
                        oi.name ILIKE %s OR 
                        oi.properties::text ILIKE %s OR
                        oi.metadata::text ILIKE %s
                    )
                    LIMIT %s
                """, (object_type, f"%{query}%", f"%{query}%", f"%{query}%", limit))
            else:
                cur.execute("""
                    SELECT oi.id, oi.name, oi.type_id, ot.name as type_name,
                           oi.properties, oi.metadata
                    FROM object_instances oi
                    JOIN object_types ot ON oi.type_id = ot.id
                    WHERE oi.name ILIKE %s OR 
                          oi.properties::text ILIKE %s OR
                          oi.metadata::text ILIKE %s OR
                          ot.name ILIKE %s
                    LIMIT %s
                """, (f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit))
            
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "type_id": row[2],
            "type_name": row[3],
            "properties": json.loads(row[4]) if row[4] else {},
            "metadata": json.loads(row[5]) if row[5] else {}
        }
        for row in rows
    ]

@app.get("/ontology/stats")
def get_ontology_stats():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM object_types")
            type_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM object_instances")
            instance_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM relationships")
            relationship_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM object_links")
            link_count = cur.fetchone()[0]
            
            cur.execute("""
                SELECT ot.name, COUNT(oi.id) as instance_count
                FROM object_types ot
                LEFT JOIN object_instances oi ON ot.id = oi.type_id
                GROUP BY ot.id, ot.name
                ORDER BY instance_count DESC
            """)
            type_distribution = [{"type": row[0], "count": row[1]} for row in cur.fetchall()]
    
    return {
        "object_types": type_count,
        "object_instances": instance_count,
        "relationships": relationship_count,
        "object_links": link_count,
        "type_distribution": type_distribution
    }
