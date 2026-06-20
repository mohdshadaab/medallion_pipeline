COMPOSE=docker compose
APP=$(COMPOSE) run --rm app

.PHONY: up down build db-init run run-drift run-evented discover bronze silver agent gold status proposals approve reject eval logs clean

up:
	$(COMPOSE) up -d postgres kafka

down:
	$(COMPOSE) down

build:
	$(COMPOSE) build

db-init:
	$(APP) python -m src.cli db-init

run:
	$(APP) python -m src.cli run

run-drift:
	$(COMPOSE) run --rm -e RAW_TICKETS_PATH=/app/data/drift_raw_tickets.csv app python -m src.cli run

run-evented:
	$(COMPOSE) --profile workers up --build

discover:
	$(APP) python -m src.services.file_discovery

bronze:
	$(APP) python -m src.services.bronze_worker

silver:
	$(APP) python -m src.services.silver_worker

agent:
	$(APP) python -m src.services.agent_worker

gold:
	$(APP) python -m src.services.gold_worker

status:
	$(APP) python -m src.cli status

proposals:
	$(APP) python -m src.cli proposals

approve:
	$(APP) python -m src.cli approve $(ID)

reject:
	$(APP) python -m src.cli reject $(ID)

eval:
	$(APP) python -m src.eval.run_eval

logs:
	$(COMPOSE) logs -f --tail=200

clean:
	$(COMPOSE) down -v
