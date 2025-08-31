CREATE TABLE IF NOT EXISTS citizens (
    nin VARCHAR PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE IF NOT EXISTS declarations (
    id SERIAL PRIMARY KEY,
    nin VARCHAR REFERENCES citizens (nin),
    goods VARCHAR,
    value INTEGER,
    origin_country VARCHAR
);

CREATE TABLE IF NOT EXISTS declaration_risks (
    id SERIAL PRIMARY KEY,
    declaration_id INTEGER REFERENCES declarations(id),
    score INTEGER,
    level VARCHAR,
    rationale VARCHAR,
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehicles (
    id SERIAL PRIMARY KEY,
    vin VARCHAR UNIQUE,
    owner_nin VARCHAR REFERENCES citizens (nin),
    vehicle_type VARCHAR,
    registration_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS data_sources (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    connection_config JSONB,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS data_pipelines (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    source_id VARCHAR REFERENCES data_sources(id),
    transformations JSONB,
    destination JSONB,
    schedule VARCHAR,
    status VARCHAR DEFAULT 'inactive',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_executions (
    id VARCHAR PRIMARY KEY,
    pipeline_id VARCHAR REFERENCES data_pipelines(id),
    status VARCHAR NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    records_processed INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS object_types (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    properties JSONB,
    parent_type_id VARCHAR REFERENCES object_types(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS object_instances (
    id VARCHAR PRIMARY KEY,
    type_id VARCHAR REFERENCES object_types(id),
    name VARCHAR NOT NULL,
    properties JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS relationships (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    source_type_id VARCHAR REFERENCES object_types(id),
    target_type_id VARCHAR REFERENCES object_types(id),
    properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS object_links (
    id VARCHAR PRIMARY KEY,
    relationship_id VARCHAR REFERENCES relationships(id),
    source_object_id VARCHAR REFERENCES object_instances(id),
    target_object_id VARCHAR REFERENCES object_instances(id),
    properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS analytics_queries (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    query_type VARCHAR NOT NULL,
    query_config JSONB,
    visualization_config JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ml_models (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    model_type VARCHAR NOT NULL,
    algorithm VARCHAR NOT NULL,
    features JSONB,
    target VARCHAR,
    training_config JSONB,
    status VARCHAR DEFAULT 'untrained',
    model_data JSONB,
    trained_at TIMESTAMP,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dashboards (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    layout JSONB,
    widgets JSONB,
    filters JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    description TEXT,
    steps JSONB,
    schedule VARCHAR,
    enabled BOOLEAN DEFAULT true,
    timeout_minutes INTEGER DEFAULT 60,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id VARCHAR PRIMARY KEY,
    workflow_id VARCHAR REFERENCES workflow_definitions(id),
    status VARCHAR NOT NULL,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    current_step INTEGER DEFAULT 0,
    step_results JSONB,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS deployment_targets (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,
    config JSONB,
    status VARCHAR DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS deployments (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    target_id VARCHAR REFERENCES deployment_targets(id),
    artifact_config JSONB,
    environment_config JSONB,
    status VARCHAR DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_declarations_nin ON declarations(nin);
CREATE INDEX IF NOT EXISTS idx_declaration_risks_declaration_id ON declaration_risks(declaration_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_nin ON vehicles(owner_nin);

CREATE INDEX IF NOT EXISTS idx_data_pipelines_source_id ON data_pipelines(source_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_executions_pipeline_id ON pipeline_executions(pipeline_id);
CREATE INDEX IF NOT EXISTS idx_object_instances_type_id ON object_instances(type_id);
CREATE INDEX IF NOT EXISTS idx_object_links_source_object_id ON object_links(source_object_id);
CREATE INDEX IF NOT EXISTS idx_object_links_target_object_id ON object_links(target_object_id);
CREATE INDEX IF NOT EXISTS idx_object_links_relationship_id ON object_links(relationship_id);
CREATE INDEX IF NOT EXISTS idx_workflow_executions_workflow_id ON workflow_executions(workflow_id);
CREATE INDEX IF NOT EXISTS idx_deployments_target_id ON deployments(target_id);

CREATE USER keycloak WITH PASSWORD 'keycloak';
CREATE DATABASE keycloak OWNER keycloak;

