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

CREATE INDEX IF NOT EXISTS idx_declarations_nin ON declarations(nin);
CREATE INDEX IF NOT EXISTS idx_declaration_risks_declaration_id ON declaration_risks(declaration_id);
CREATE INDEX IF NOT EXISTS idx_vehicles_nin ON vehicles(owner_nin);

CREATE USER keycloak WITH PASSWORD 'keycloak';
CREATE DATABASE keycloak OWNER keycloak;

