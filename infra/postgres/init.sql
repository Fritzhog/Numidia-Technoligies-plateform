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
    license_plate VARCHAR UNIQUE,
    nin VARCHAR REFERENCES citizens (nin),
    model VARCHAR,
    registration_date DATE DEFAULT CURRENT_DATE
);

CREATE USER keycloak WITH PASSWORD 'keycloak';
CREATE DATABASE keycloak OWNER keycloak;

