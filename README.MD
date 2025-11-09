# QueueCTL - Background Job Queue CLI System

## Setup Instructions

1. Clone the repository:

   ```bash
   git clone https://github.com/luckyasrita-16/backend_FLAM.git
   cd queuectl1
   ```

2. Create and activate a Python virtual environment:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Run CLI commands inside the activated environment.

***

## Usage Examples

### Enqueue a new job

```bash
python -m queuectl1.cli enqueue '{"id":"job1","command":"echo Hello World"}'
```

### Start multiple workers

```bash
python -m queuectl1.cli worker start --count 3
```

### Stop all workers gracefully

```bash
python -m queuectl1.cli worker stop
```

### Show job status summary

```bash
python -m queuectl1.cli status
```

### List jobs by state (pending, completed, failed, dead)

```bash
python -m queuectl1.cli list --state pending
```

### View Dead Letter Queue (DLQ) jobs

```bash
python -m queuectl1.cli dlq list
```

### Retry a job from DLQ

```bash
python -m queuectl1.cli dlq retry job1
```

### Modify config: set max retries

```bash
python -m queuectl1.cli config set max_retries 5
```

***

## Architecture Overview

QueueCTL is a CLI-based background job queue system with the following design:

- Jobs contain fields: id, command, state (pending, processing, completed, failed, dead), attempts count, max retries, timestamps.
- Supports persistent storage using SQLite for job data and configuration.
- Workers run as concurrent threads executing job commands.
- Failed jobs retry automatically with exponential backoff delay based on configurable base.
- Jobs exceeding max retries move to Dead Letter Queue.
- CLI commands support enqueueing jobs, starting/stopping workers, querying status and jobs, managing DLQ, and configuration.

***

## Assumptions & Trade-offs

- Commands are executed directly in the system shell, with no sandboxing.
- Worker concurrency uses threading; multiprocessing or async evaluation not implemented.
- Simple exponential backoff without jitter or randomness.
- SQLite selected for simplicity and reliability.
- Configuration stored in SQLite and manageable via CLI.
- No job priority or scheduling included.

***

## Testing Instructions

1. Tests are located in `tests/` folder.

2. Run tests using `pytest` inside your virtual environment:

   ```bash
   pytest tests/
   ```

3. Tests cover:
   - Enqueue and process jobs successfully.
   - Retry failed jobs and moving them to Dead Letter Queue.
   - DLQ listing and job retry from DLQ.
   - Concurrent worker processing.

4. Manually verify CLI commands during usage as demonstrated above.

!-----------------------------!
