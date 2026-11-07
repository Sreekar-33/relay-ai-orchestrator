SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
TOKEN ?= relay-demo-token
API ?= http://localhost:8080
WORLD ?= http://localhost:9210

.PHONY: up down reset logs ps smoke drill drill-crash chaos db test
up:
	docker compose up -d --build
	bash scripts/wait_for.sh $(API)/health 180
down:
	docker compose down
reset:
	docker compose down -v
	$(MAKE) up
logs:
	docker compose logs -f api worker
ps:
	docker compose ps
smoke:
	mkdir -p docs/verification
	python3 scripts/smoke_test.py --url $(API) --token $(TOKEN) --world $(WORLD) | tee docs/verification/smoke_test.txt
drill:
	bash scripts/drill_kill_resume.sh delay
drill-crash:
	bash scripts/drill_kill_resume.sh crash-window
chaos:
	bash scripts/drill_chaos.sh
db:
	docker compose up -d postgres mock-world
test:
	.venv/bin/pytest -q
