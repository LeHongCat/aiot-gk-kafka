# Apache Kafka: Sensor Load Distribution via Consumer Groups (Group 2, MDL26)

Kafka runs in Docker. A producer pushes simulated sensor data into a 3-partition topic, and three consumers in one group process it. Benchmark results: [REPORT.md](REPORT.md).

## Setup

Requires Docker (running) and Python 3.10+. From the repo root:

```bash
python -m venv .venv
source .venv/bin/activate        # Windows PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
docker compose up -d
docker compose ps
```

Wait until `kafka` is **healthy**. Run all commands below from the repo root with the virtual environment activated. Optional: copy `.env.example` to `.env` to change the default settings.

## Scenario 1: Demo, one sensor handled by one consumer

```bash
python -m monitoring.demo_live --step
```

Press Enter to send one message at a time and watch it travel: data generation, sending, which partition and offset Kafka stores it at, and which consumer receives it. Add `--no-key` to compare what happens when no message key is set.

## Scenario 2: Benchmark, 5,000 msg/s with 3 consumers

```bash
python -m monitoring.run_scenario --run-id T3-r1 --rate 5000 --consumers 3 --duration 60
python -m monitoring.summary
```

Results (throughput, lag, CPU/RAM, loss/duplicate check) are written to `output/T3-r1/`. Change `--rate` and `--consumers` for other scenarios; each `--run-id` can only be used once.

## Cleanup

```bash
python -m monitoring.clean --kafka    # delete output/ and the topics/groups created by the scripts
docker compose down                   # stop Kafka
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `failed to connect to the docker API` | Start Docker. |
| `Timeout: ... assigning partitions` | Kafka is not healthy yet; wait, then run again. |
| `already has data` or `Topic ... already exists` | Use a new `--run-id`, or run `python -m monitoring.clean --kafka`. |
