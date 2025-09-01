from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
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
from datetime import datetime, timedelta
from croniter import croniter
import asyncio
import threading

app = FastAPI(title="Workflow Orchestration Service", description="Apollo-like deployment and pipeline management")

DB_CONN = {
    'host': os.environ['DB_HOST'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'dbname': os.environ['DB_NAME'],
}

_connection_pool = None
_kafka_producer = None
_opensearch_client = None
_scheduler_running = False

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

class WorkflowDefinition(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    steps: List[Dict[str, Any]]
    schedule: Optional[str] = None  # cron expression
    enabled: Optional[bool] = True
    timeout_minutes: Optional[int] = 60

class WorkflowExecution(BaseModel):
    id: Optional[str] = None
    workflow_id: str
    status: str  # 'pending', 'running', 'completed', 'failed', 'cancelled'
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    current_step: Optional[int] = 0
    step_results: Optional[List[Dict[str, Any]]] = None
    error_message: Optional[str] = None

class DeploymentTarget(BaseModel):
    id: Optional[str] = None
    name: str
    type: str  # 'kubernetes', 'docker', 'serverless'
    config: Dict[str, Any]
    status: Optional[str] = "active"

class Deployment(BaseModel):
    id: Optional[str] = None
    name: str
    target_id: str
    artifact_config: Dict[str, Any]
    environment_config: Dict[str, Any]
    status: Optional[str] = "pending"

@app.get("/health")
def health():
    return {"status": "ok", "service": "workflow-orchestration"}

@app.post("/workflows")
def create_workflow(workflow: WorkflowDefinition):
    workflow.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO workflow_definitions (id, name, description, steps, schedule, enabled, timeout_minutes)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                workflow.id, workflow.name, workflow.description,
                json.dumps(workflow.steps), workflow.schedule,
                workflow.enabled, workflow.timeout_minutes
            ))
    
    producer = get_kafka_producer()
    producer.send('workflow.definition.created', workflow.dict())
    
    return {"id": workflow.id, "message": "Workflow created"}

@app.get("/workflows")
def list_workflows():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, steps, schedule, enabled, timeout_minutes
                FROM workflow_definitions
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "steps": json.loads(row[3]) if row[3] else [],
            "schedule": row[4],
            "enabled": row[5],
            "timeout_minutes": row[6]
        }
        for row in rows
    ]

@app.get("/workflows/{workflow_id}")
def get_workflow(workflow_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, steps, schedule, enabled, timeout_minutes
                FROM workflow_definitions WHERE id = %s
            """, (workflow_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Workflow not found")
            
            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "steps": json.loads(row[3]) if row[3] else [],
                "schedule": row[4],
                "enabled": row[5],
                "timeout_minutes": row[6]
            }

@app.post("/workflows/{workflow_id}/execute")
def execute_workflow(workflow_id: str, background_tasks: BackgroundTasks):
    execution_id = str(uuid.uuid4())
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, steps, timeout_minutes
                FROM workflow_definitions WHERE id = %s AND enabled = true
            """, (workflow_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Workflow not found or disabled")
            
            cur.execute("""
                INSERT INTO workflow_executions (id, workflow_id, status, started_at, current_step, step_results)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (execution_id, workflow_id, "pending", datetime.now(), 0, json.dumps([])))
    
    background_tasks.add_task(run_workflow_execution, workflow_id, execution_id)
    
    return {"execution_id": execution_id, "status": "started"}

def run_workflow_execution(workflow_id: str, execution_id: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, steps, timeout_minutes
                    FROM workflow_definitions WHERE id = %s
                """, (workflow_id,))
                workflow_data = cur.fetchone()
                
                if not workflow_data:
                    raise Exception("Workflow not found")
                
                name, steps_json, timeout_minutes = workflow_data
                steps = json.loads(steps_json) if steps_json else []
                
                cur.execute("""
                    UPDATE workflow_executions 
                    SET status = 'running'
                    WHERE id = %s
                """, (execution_id,))
                
                step_results = []
                
                for i, step in enumerate(steps):
                    cur.execute("""
                        UPDATE workflow_executions 
                        SET current_step = %s
                        WHERE id = %s
                    """, (i, execution_id))
                    
                    step_result = execute_workflow_step(step, step_results)
                    step_results.append(step_result)
                    
                    cur.execute("""
                        UPDATE workflow_executions 
                        SET step_results = %s
                        WHERE id = %s
                    """, (json.dumps(step_results), execution_id))
                    
                    if step_result.get("status") == "failed":
                        raise Exception(f"Step {i} failed: {step_result.get('error')}")
                
                cur.execute("""
                    UPDATE workflow_executions 
                    SET status = 'completed', completed_at = %s
                    WHERE id = %s
                """, (datetime.now(), execution_id))
                
                producer = get_kafka_producer()
                producer.send('workflow.execution.completed', {
                    "execution_id": execution_id,
                    "workflow_id": workflow_id,
                    "workflow_name": name,
                    "steps_completed": len(steps)
                })
                
    except Exception as e:
        logging.error(f"Workflow execution failed: {e}")
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE workflow_executions 
                    SET status = 'failed', completed_at = %s, error_message = %s
                    WHERE id = %s
                """, (datetime.now(), str(e), execution_id))

def execute_workflow_step(step: Dict[str, Any], previous_results: List[Dict[str, Any]]):
    step_type = step.get("type")
    step_config = step.get("config", {})
    
    try:
        if step_type == "data_pipeline":
            return execute_data_pipeline_step(step_config)
        elif step_type == "analytics_query":
            return execute_analytics_query_step(step_config)
        elif step_type == "ml_training":
            return execute_ml_training_step(step_config)
        elif step_type == "notification":
            return execute_notification_step(step_config)
        elif step_type == "deployment":
            return execute_deployment_step(step_config)
        elif step_type == "wait":
            return execute_wait_step(step_config)
        else:
            return {"status": "skipped", "message": f"Unknown step type: {step_type}"}
    
    except Exception as e:
        return {"status": "failed", "error": str(e)}

def execute_data_pipeline_step(config: Dict[str, Any]):
    pipeline_id = config.get("pipeline_id")
    if not pipeline_id:
        return {"status": "failed", "error": "Pipeline ID required"}
    
    time.sleep(2)  # Simulate processing time
    
    return {
        "status": "completed",
        "message": f"Data pipeline {pipeline_id} executed successfully",
        "records_processed": 150
    }

def execute_analytics_query_step(config: Dict[str, Any]):
    query_id = config.get("query_id")
    if not query_id:
        return {"status": "failed", "error": "Query ID required"}
    
    time.sleep(1)
    
    return {
        "status": "completed",
        "message": f"Analytics query {query_id} executed successfully",
        "result_count": 42
    }

def execute_ml_training_step(config: Dict[str, Any]):
    model_id = config.get("model_id")
    if not model_id:
        return {"status": "failed", "error": "Model ID required"}
    
    time.sleep(3)
    
    return {
        "status": "completed",
        "message": f"ML model {model_id} trained successfully",
        "accuracy": 0.85
    }

def execute_notification_step(config: Dict[str, Any]):
    message = config.get("message", "Workflow step completed")
    recipients = config.get("recipients", [])
    
    producer = get_kafka_producer()
    producer.send('workflow.notification', {
        "message": message,
        "recipients": recipients,
        "sent_at": datetime.now().isoformat()
    })
    
    return {
        "status": "completed",
        "message": f"Notification sent to {len(recipients)} recipients"
    }

def execute_deployment_step(config: Dict[str, Any]):
    deployment_id = config.get("deployment_id")
    if not deployment_id:
        return {"status": "failed", "error": "Deployment ID required"}
    
    time.sleep(2)
    
    return {
        "status": "completed",
        "message": f"Deployment {deployment_id} completed successfully"
    }

def execute_wait_step(config: Dict[str, Any]):
    wait_seconds = config.get("seconds", 5)
    time.sleep(wait_seconds)
    
    return {
        "status": "completed",
        "message": f"Waited for {wait_seconds} seconds"
    }

@app.get("/workflows/{workflow_id}/executions")
def list_workflow_executions(workflow_id: str, limit: int = 50):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, status, started_at, completed_at, current_step, error_message
                FROM workflow_executions 
                WHERE workflow_id = %s
                ORDER BY started_at DESC
                LIMIT %s
            """, (workflow_id, limit))
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "status": row[1],
            "started_at": row[2],
            "completed_at": row[3],
            "current_step": row[4],
            "error_message": row[5]
        }
        for row in rows
    ]

@app.get("/executions/{execution_id}")
def get_workflow_execution(execution_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT we.id, we.workflow_id, we.status, we.started_at, we.completed_at, 
                       we.current_step, we.step_results, we.error_message,
                       wd.name as workflow_name
                FROM workflow_executions we
                JOIN workflow_definitions wd ON we.workflow_id = wd.id
                WHERE we.id = %s
            """, (execution_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Execution not found")
            
            return {
                "id": row[0],
                "workflow_id": row[1],
                "status": row[2],
                "started_at": row[3],
                "completed_at": row[4],
                "current_step": row[5],
                "step_results": json.loads(row[6]) if row[6] else [],
                "error_message": row[7],
                "workflow_name": row[8]
            }

@app.post("/deployment-targets")
def create_deployment_target(target: DeploymentTarget):
    target.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO deployment_targets (id, name, type, config, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (target.id, target.name, target.type, json.dumps(target.config), target.status))
    
    producer = get_kafka_producer()
    producer.send('workflow.deployment_target.created', target.dict())
    
    return {"id": target.id, "message": "Deployment target created"}

@app.get("/deployment-targets")
def list_deployment_targets():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, type, config, status
                FROM deployment_targets
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "type": row[2],
            "config": json.loads(row[3]) if row[3] else {},
            "status": row[4]
        }
        for row in rows
    ]

@app.post("/deployments")
def create_deployment(deployment: Deployment):
    deployment.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO deployments (id, name, target_id, artifact_config, environment_config, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                deployment.id, deployment.name, deployment.target_id,
                json.dumps(deployment.artifact_config), json.dumps(deployment.environment_config),
                deployment.status
            ))
    
    producer = get_kafka_producer()
    producer.send('workflow.deployment.created', deployment.dict())
    
    return {"id": deployment.id, "message": "Deployment created"}

@app.get("/deployments")
def list_deployments():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT d.id, d.name, d.target_id, d.artifact_config, d.environment_config, d.status,
                       dt.name as target_name, dt.type as target_type
                FROM deployments d
                JOIN deployment_targets dt ON d.target_id = dt.id
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "target_id": row[2],
            "artifact_config": json.loads(row[3]) if row[3] else {},
            "environment_config": json.loads(row[4]) if row[4] else {},
            "status": row[5],
            "target_name": row[6],
            "target_type": row[7]
        }
        for row in rows
    ]

@app.get("/orchestration/stats")
def get_orchestration_stats():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM workflow_definitions WHERE enabled = true")
            active_workflows = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM workflow_executions WHERE status = 'running'")
            running_executions = cur.fetchone()[0]
            
            cur.execute("""
                SELECT COUNT(*) as total, status
                FROM workflow_executions
                WHERE started_at >= NOW() - INTERVAL '24 hours'
                GROUP BY status
            """)
            recent_executions = {row[1]: row[0] for row in cur.fetchall()}
            
            cur.execute("SELECT COUNT(*) FROM deployment_targets WHERE status = 'active'")
            active_targets = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM deployments")
            total_deployments = cur.fetchone()[0]
    
    return {
        "active_workflows": active_workflows,
        "running_executions": running_executions,
        "recent_executions_24h": recent_executions,
        "active_deployment_targets": active_targets,
        "total_deployments": total_deployments,
        "generated_at": datetime.now()
    }

def start_scheduler():
    global _scheduler_running
    if _scheduler_running:
        return
    
    _scheduler_running = True
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

def run_scheduler():
    while _scheduler_running:
        try:
            check_scheduled_workflows()
            time.sleep(60)  # Check every minute
        except Exception as e:
            logging.error(f"Scheduler error: {e}")
            time.sleep(60)

def check_scheduled_workflows():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, schedule
                FROM workflow_definitions
                WHERE enabled = true AND schedule IS NOT NULL
            """)
            workflows = cur.fetchall()
            
            current_time = datetime.now()
            
            for workflow_id, name, schedule in workflows:
                try:
                    cron = croniter(schedule, current_time)
                    next_run = cron.get_prev(datetime)
                    
                    cur.execute("""
                        SELECT MAX(started_at)
                        FROM workflow_executions
                        WHERE workflow_id = %s
                    """, (workflow_id,))
                    last_run = cur.fetchone()[0]
                    
                    if not last_run or last_run < next_run:
                        logging.info(f"Triggering scheduled workflow: {name}")
                        execution_id = str(uuid.uuid4())
                        
                        cur.execute("""
                            INSERT INTO workflow_executions (id, workflow_id, status, started_at, current_step, step_results)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (execution_id, workflow_id, "pending", current_time, 0, json.dumps([])))
                        
                        threading.Thread(
                            target=run_workflow_execution,
                            args=(workflow_id, execution_id),
                            daemon=True
                        ).start()
                        
                except Exception as e:
                    logging.error(f"Error scheduling workflow {name}: {e}")

@app.on_event("startup")
def startup_event():
    start_scheduler()

@app.on_event("shutdown")
def shutdown_event():
    global _scheduler_running
    _scheduler_running = False
