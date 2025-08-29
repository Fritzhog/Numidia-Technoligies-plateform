FROM python:3.11-slim
RUN apt-get update && apt-get install -y curl jq && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY run_smoke.sh /app/run_smoke.sh
RUN chmod +x /app/run_smoke.sh
CMD ["/bin/sh", "/app/run_smoke.sh"]
