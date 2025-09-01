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
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, LinearRegression
from sklearn.metrics import accuracy_score, mean_squared_error, classification_report
import plotly.graph_objects as go
import plotly.express as px
from plotly.utils import PlotlyJSONEncoder
import seaborn as sns
import matplotlib.pyplot as plt
import io
import base64

app = FastAPI(title="Analytics Engine Service", description="Advanced analytics, ML workflows, and collaborative tools")

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

class AnalyticsQuery(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    query_type: str  # 'sql', 'aggregation', 'ml_prediction'
    query_config: Dict[str, Any]
    visualization_config: Optional[Dict[str, Any]] = None

class MLModel(BaseModel):
    id: Optional[str] = None
    name: str
    model_type: str  # 'classification', 'regression', 'clustering'
    algorithm: str  # 'random_forest', 'logistic_regression', 'linear_regression'
    features: List[str]
    target: Optional[str] = None
    training_config: Dict[str, Any]
    status: Optional[str] = "untrained"

class Dashboard(BaseModel):
    id: Optional[str] = None
    name: str
    description: Optional[str] = None
    layout: Dict[str, Any]
    widgets: List[Dict[str, Any]]
    filters: Optional[Dict[str, Any]] = None

@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics-engine"}

@app.post("/analytics/queries")
def create_analytics_query(query: AnalyticsQuery):
    query.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO analytics_queries (id, name, description, query_type, query_config, visualization_config)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                query.id, query.name, query.description, query.query_type,
                json.dumps(query.query_config), json.dumps(query.visualization_config or {})
            ))
    
    producer = get_kafka_producer()
    producer.send('analytics.query.created', query.dict())
    
    return {"id": query.id, "message": "Analytics query created"}

@app.get("/analytics/queries")
def list_analytics_queries():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, query_type, query_config, visualization_config
                FROM analytics_queries
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "query_type": row[3],
            "query_config": json.loads(row[4]) if row[4] else {},
            "visualization_config": json.loads(row[5]) if row[5] else {}
        }
        for row in rows
    ]

@app.post("/analytics/queries/{query_id}/execute")
def execute_analytics_query(query_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, query_type, query_config, visualization_config
                FROM analytics_queries WHERE id = %s
            """, (query_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Query not found")
            
            name, query_type, config_json, viz_config_json = row
            config = json.loads(config_json) if config_json else {}
            viz_config = json.loads(viz_config_json) if viz_config_json else {}
            
            if query_type == "sql":
                result = execute_sql_query(config)
            elif query_type == "aggregation":
                result = execute_aggregation_query(config)
            elif query_type == "ml_prediction":
                result = execute_ml_prediction(config)
            else:
                raise HTTPException(status_code=400, detail="Unsupported query type")
            
            visualization = None
            if viz_config and result.get("data"):
                visualization = generate_visualization(result["data"], viz_config)
            
            return {
                "query_id": query_id,
                "query_name": name,
                "result": result,
                "visualization": visualization,
                "executed_at": datetime.now()
            }

def execute_sql_query(config: Dict[str, Any]):
    sql_query = config.get("sql")
    if not sql_query:
        raise HTTPException(status_code=400, detail="SQL query required")
    
    with get_db() as conn:
        try:
            df = pd.read_sql(sql_query, conn)
            return {
                "data": df.to_dict('records'),
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "summary": df.describe().to_dict() if len(df) > 0 else {}
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"SQL execution error: {str(e)}")

def execute_aggregation_query(config: Dict[str, Any]):
    table = config.get("table")
    group_by = config.get("group_by", [])
    aggregations = config.get("aggregations", [])
    filters = config.get("filters", [])
    
    if not table:
        raise HTTPException(status_code=400, detail="Table name required")
    
    sql_parts = [f"SELECT"]
    
    if group_by:
        sql_parts.append(", ".join(group_by))
        if aggregations:
            sql_parts.append(",")
    
    if aggregations:
        agg_parts = []
        for agg in aggregations:
            func = agg.get("function", "COUNT")
            column = agg.get("column", "*")
            alias = agg.get("alias", f"{func.lower()}_{column}")
            agg_parts.append(f"{func}({column}) as {alias}")
        sql_parts.append(", ".join(agg_parts))
    
    sql_parts.extend([f"FROM {table}"])
    
    if filters:
        where_parts = []
        for filter_item in filters:
            column = filter_item.get("column")
            operator = filter_item.get("operator", "=")
            value = filter_item.get("value")
            if column and value is not None:
                if operator in ["=", "!=", ">", "<", ">=", "<="]:
                    where_parts.append(f"{column} {operator} '{value}'")
                elif operator == "LIKE":
                    where_parts.append(f"{column} LIKE '%{value}%'")
        
        if where_parts:
            sql_parts.extend(["WHERE", " AND ".join(where_parts)])
    
    if group_by:
        sql_parts.extend(["GROUP BY", ", ".join(group_by)])
    
    sql_query = " ".join(sql_parts)
    
    with get_db() as conn:
        try:
            df = pd.read_sql(sql_query, conn)
            return {
                "data": df.to_dict('records'),
                "columns": df.columns.tolist(),
                "row_count": len(df),
                "sql_query": sql_query
            }
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Aggregation query error: {str(e)}")

def execute_ml_prediction(config: Dict[str, Any]):
    model_id = config.get("model_id")
    input_data = config.get("input_data", {})
    
    if not model_id:
        raise HTTPException(status_code=400, detail="Model ID required")
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, model_type, algorithm, features, target, model_data
                FROM ml_models WHERE id = %s AND status = 'trained'
            """, (model_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Trained model not found")
            
            name, model_type, algorithm, features_json, target, model_data_json = row
            features = json.loads(features_json) if features_json else []
            model_data = json.loads(model_data_json) if model_data_json else {}
            
            feature_values = [input_data.get(feature, 0) for feature in features]
            
            if algorithm == "random_forest" and model_type == "classification":
                prediction = predict_random_forest_classification(feature_values, model_data)
            elif algorithm == "logistic_regression":
                prediction = predict_logistic_regression(feature_values, model_data)
            else:
                raise HTTPException(status_code=400, detail="Unsupported model type/algorithm")
            
            return {
                "model_id": model_id,
                "model_name": name,
                "prediction": prediction,
                "input_features": dict(zip(features, feature_values)),
                "predicted_at": datetime.now()
            }

def predict_random_forest_classification(features: List[float], model_data: Dict[str, Any]):
    feature_importance = model_data.get("feature_importance", [0.5] * len(features))
    weighted_score = sum(f * w for f, w in zip(features, feature_importance))
    
    if weighted_score > 0.5:
        return {"class": "HIGH", "probability": min(weighted_score, 1.0)}
    else:
        return {"class": "LOW", "probability": max(1 - weighted_score, 0.0)}

def predict_logistic_regression(features: List[float], model_data: Dict[str, Any]):
    coefficients = model_data.get("coefficients", [0.1] * len(features))
    intercept = model_data.get("intercept", 0.0)
    
    linear_combination = intercept + sum(f * c for f, c in zip(features, coefficients))
    probability = 1 / (1 + np.exp(-linear_combination))
    
    return {"probability": float(probability), "class": "POSITIVE" if probability > 0.5 else "NEGATIVE"}

def generate_visualization(data: List[Dict], viz_config: Dict[str, Any]):
    chart_type = viz_config.get("type", "bar")
    x_column = viz_config.get("x_column")
    y_column = viz_config.get("y_column")
    
    if not data or not x_column:
        return None
    
    df = pd.DataFrame(data)
    
    try:
        if chart_type == "bar":
            fig = px.bar(df, x=x_column, y=y_column, title=viz_config.get("title", "Bar Chart"))
        elif chart_type == "line":
            fig = px.line(df, x=x_column, y=y_column, title=viz_config.get("title", "Line Chart"))
        elif chart_type == "scatter":
            fig = px.scatter(df, x=x_column, y=y_column, title=viz_config.get("title", "Scatter Plot"))
        elif chart_type == "pie":
            fig = px.pie(df, names=x_column, values=y_column, title=viz_config.get("title", "Pie Chart"))
        else:
            fig = px.bar(df, x=x_column, y=y_column, title="Default Chart")
        
        return json.loads(fig.to_json())
    except Exception as e:
        logging.error(f"Visualization generation error: {e}")
        return None

@app.post("/ml/models")
def create_ml_model(model: MLModel):
    model.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO ml_models (id, name, model_type, algorithm, features, target, training_config, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                model.id, model.name, model.model_type, model.algorithm,
                json.dumps(model.features), model.target,
                json.dumps(model.training_config), model.status
            ))
    
    producer = get_kafka_producer()
    producer.send('analytics.ml_model.created', model.dict())
    
    return {"id": model.id, "message": "ML model created"}

@app.get("/ml/models")
def list_ml_models():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, model_type, algorithm, features, target, training_config, status
                FROM ml_models
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "model_type": row[2],
            "algorithm": row[3],
            "features": json.loads(row[4]) if row[4] else [],
            "target": row[5],
            "training_config": json.loads(row[6]) if row[6] else {},
            "status": row[7]
        }
        for row in rows
    ]

@app.post("/ml/models/{model_id}/train")
def train_ml_model(model_id: str, background_tasks: BackgroundTasks):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT name, model_type, algorithm, features, target, training_config
                FROM ml_models WHERE id = %s
            """, (model_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Model not found")
            
            cur.execute("""
                UPDATE ml_models SET status = 'training' WHERE id = %s
            """, (model_id,))
    
    background_tasks.add_task(train_model_background, model_id)
    
    return {"model_id": model_id, "status": "training_started"}

def train_model_background(model_id: str):
    try:
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT name, model_type, algorithm, features, target, training_config
                    FROM ml_models WHERE id = %s
                """, (model_id,))
                row = cur.fetchone()
                
                if not row:
                    return
                
                name, model_type, algorithm, features_json, target, config_json = row
                features = json.loads(features_json) if features_json else []
                config = json.loads(config_json) if config_json else {}
                
                data_source = config.get("data_source", "declarations")
                training_data = get_training_data(data_source, features, target)
                
                if len(training_data) < 10:
                    raise Exception("Insufficient training data")
                
                model_data = train_model(training_data, features, target, algorithm, model_type)
                
                cur.execute("""
                    UPDATE ml_models 
                    SET status = 'trained', model_data = %s, trained_at = %s
                    WHERE id = %s
                """, (json.dumps(model_data), datetime.now(), model_id))
                
                producer = get_kafka_producer()
                producer.send('analytics.ml_model.trained', {
                    "model_id": model_id,
                    "model_name": name,
                    "training_metrics": model_data.get("metrics", {})
                })
                
    except Exception as e:
        logging.error(f"Model training failed: {e}")
        with get_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE ml_models 
                    SET status = 'failed', error_message = %s
                    WHERE id = %s
                """, (str(e), model_id))

def get_training_data(data_source: str, features: List[str], target: str):
    if data_source == "declarations":
        sql = f"""
            SELECT d.value, d.origin_country, dr.score, dr.level
            FROM declarations d
            JOIN declaration_risks dr ON d.id = dr.declaration_id
            WHERE dr.score IS NOT NULL
        """
    else:
        sql = f"SELECT * FROM {data_source} LIMIT 1000"
    
    with get_db() as conn:
        df = pd.read_sql(sql, conn)
    
    return df

def train_model(data: pd.DataFrame, features: List[str], target: str, algorithm: str, model_type: str):
    available_features = [f for f in features if f in data.columns]
    if not available_features:
        raise Exception("No valid features found in data")
    
    X = data[available_features]
    
    X = X.fillna(X.mean() if X.select_dtypes(include=[np.number]).shape[1] > 0 else 0)
    
    if target and target in data.columns:
        y = data[target]
    else:
        y = (data.iloc[:, 0] > data.iloc[:, 0].median()).astype(int)
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    if algorithm == "random_forest":
        if model_type == "classification":
            model = RandomForestClassifier(n_estimators=100, random_state=42)
        else:
            model = RandomForestRegressor(n_estimators=100, random_state=42)
    elif algorithm == "logistic_regression":
        model = LogisticRegression(random_state=42)
    elif algorithm == "linear_regression":
        model = LinearRegression()
    else:
        raise Exception(f"Unsupported algorithm: {algorithm}")
    
    model.fit(X_train, y_train)
    
    y_pred = model.predict(X_test)
    
    if model_type == "classification":
        accuracy = accuracy_score(y_test, y_pred)
        metrics = {"accuracy": accuracy}
    else:
        mse = mean_squared_error(y_test, y_pred)
        metrics = {"mse": mse}
    
    if hasattr(model, 'feature_importances_'):
        feature_importance = model.feature_importances_.tolist()
    elif hasattr(model, 'coef_'):
        feature_importance = model.coef_.tolist() if len(model.coef_.shape) == 1 else model.coef_[0].tolist()
    else:
        feature_importance = [1.0 / len(available_features)] * len(available_features)
    
    model_data = {
        "feature_importance": feature_importance,
        "features": available_features,
        "metrics": metrics,
        "algorithm": algorithm,
        "model_type": model_type
    }
    
    if hasattr(model, 'coef_'):
        model_data["coefficients"] = model.coef_.tolist() if len(model.coef_.shape) == 1 else model.coef_[0].tolist()
    
    if hasattr(model, 'intercept_'):
        model_data["intercept"] = float(model.intercept_) if np.isscalar(model.intercept_) else model.intercept_.tolist()
    
    return model_data

@app.post("/dashboards")
def create_dashboard(dashboard: Dashboard):
    dashboard.id = str(uuid.uuid4())
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO dashboards (id, name, description, layout, widgets, filters)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                dashboard.id, dashboard.name, dashboard.description,
                json.dumps(dashboard.layout), json.dumps(dashboard.widgets),
                json.dumps(dashboard.filters or {})
            ))
    
    producer = get_kafka_producer()
    producer.send('analytics.dashboard.created', dashboard.dict())
    
    return {"id": dashboard.id, "message": "Dashboard created"}

@app.get("/dashboards")
def list_dashboards():
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, layout, widgets, filters
                FROM dashboards
            """)
            rows = cur.fetchall()
    
    return [
        {
            "id": row[0],
            "name": row[1],
            "description": row[2],
            "layout": json.loads(row[3]) if row[3] else {},
            "widgets": json.loads(row[4]) if row[4] else [],
            "filters": json.loads(row[5]) if row[5] else {}
        }
        for row in rows
    ]

@app.get("/dashboards/{dashboard_id}")
def get_dashboard(dashboard_id: str):
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, description, layout, widgets, filters
                FROM dashboards WHERE id = %s
            """, (dashboard_id,))
            row = cur.fetchone()
            
            if not row:
                raise HTTPException(status_code=404, detail="Dashboard not found")
            
            return {
                "id": row[0],
                "name": row[1],
                "description": row[2],
                "layout": json.loads(row[3]) if row[3] else {},
                "widgets": json.loads(row[4]) if row[4] else [],
                "filters": json.loads(row[5]) if row[5] else {}
            }

@app.get("/analytics/insights")
def get_analytics_insights():
    insights = []
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT level, COUNT(*) as count
                FROM declaration_risks
                GROUP BY level
                ORDER BY count DESC
            """)
            risk_distribution = cur.fetchall()
            
            if risk_distribution:
                insights.append({
                    "type": "risk_distribution",
                    "title": "Declaration Risk Distribution",
                    "data": [{"level": row[0], "count": row[1]} for row in risk_distribution],
                    "insight": f"Most declarations are {risk_distribution[0][0]} risk ({risk_distribution[0][1]} cases)"
                })
            
            cur.execute("""
                SELECT COUNT(*) as high_value_count, AVG(value) as avg_value
                FROM declarations
                WHERE value > 50000
            """)
            high_value_data = cur.fetchone()
            
            if high_value_data and high_value_data[0] > 0:
                insights.append({
                    "type": "high_value_declarations",
                    "title": "High Value Declarations",
                    "data": {"count": high_value_data[0], "average_value": high_value_data[1]},
                    "insight": f"{high_value_data[0]} high-value declarations with average value ${high_value_data[1]:,.2f}"
                })
            
            cur.execute("""
                SELECT d.origin_country, AVG(dr.score) as avg_risk_score, COUNT(*) as declaration_count
                FROM declarations d
                JOIN declaration_risks dr ON d.id = dr.declaration_id
                GROUP BY d.origin_country
                HAVING COUNT(*) >= 2
                ORDER BY avg_risk_score DESC
                LIMIT 5
            """)
            country_risks = cur.fetchall()
            
            if country_risks:
                insights.append({
                    "type": "country_risk_analysis",
                    "title": "Countries by Risk Score",
                    "data": [
                        {"country": row[0], "avg_risk_score": row[1], "declaration_count": row[2]}
                        for row in country_risks
                    ],
                    "insight": f"Highest risk country: {country_risks[0][0]} (avg score: {country_risks[0][1]:.1f})"
                })
    
    return {
        "insights": insights,
        "generated_at": datetime.now(),
        "total_insights": len(insights)
    }
