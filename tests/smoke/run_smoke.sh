#!/bin/sh

# Wait for services health endpoints to return ok
wait_for_health() {
  url=$1
  until curl -s "$url" | grep -q "ok"; do
    sleep 2
  done
}

wait_for_health http://citizens-service:8000/health
wait_for_health http://customs-risk-service:8000/health
wait_for_health http://transport-vehicle-service:8000/health

# Obtain access token from Keycloak (assumes direct access grants enabled)
TOKEN=$(curl -s -X POST \
  -d "client_id=frontend" \
  -d "username=demo@numidia.dz" \
  -d "password=Passw0rd!" \
  -d "grant_type=password" \
  http://keycloak:8080/realms/numidia/protocol/openid-connect/token | jq -r '.access_token')

# Create a citizen
curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"nin":"NIN1001","name":"Alice"}' \
  http://citizens-service:8000/citizens

# Create a customs declaration
curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"nin":"NIN1001","origin_country":"KY","value":120000,"goods":"electronic components"}' \
  http://customs-risk-service:8000/declarations

# Register a vehicle
curl -s -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"nin":"NIN1001","vehicle":"ABC-123"}' \
  http://transport-vehicle-service:8000/vehicles

# Give some time for the risk computation pipeline
sleep 5

# Verify that at least one risk is present
COUNT=$(curl -s http://customs-risk-service:8000/risks | jq '. | length')
if [ "$COUNT" -gt 0 ]; then
  echo "Risks available: $COUNT"
  exit 0
else
  echo "No risks found!"
  exit 1
fi
