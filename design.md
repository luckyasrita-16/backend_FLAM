1 # QueueCTL Design and Architecture

## Overview

QueueCTL is a CLI-based background job queue system that manages job lifecycle with persistent storage, worker concurrency, retry with exponential backoff, and Dead Letter Queue (DLQ) support.

## Components

- **CLI**: User interface implemented with Python Click. Supports commands for enqueueing jobs, managing workers, checking status, listing jobs, managing DLQ, and configuration.
- **Job Model**: Each job includes ID, command, state, attempts, max retries, timestamps.
- **Queue Manager**: Handles enqueueing, fetching next pending job, updating job states.
- **Worker**: Runs jobs concurrently, executes commands, handles retries and backoff, moves jobs to DLQ on persistent failure.
- **Storage Layer**: SQLite database handles persistent job, DLQ, and config data.
- **DLQ Manager**: Lists and retries dead jobs.

## Job Lifecycle

States:
- Pending → Processing → Completed  
- Failed → (retry with backoff) → Failed or Dead (moved to DLQ)

Exponential backoff is calculated as:  
`delay = base ^ attempts` seconds

## Concurrency & Safety

- Multiple workers run as threads to process jobs in parallel.
- SQLite ensures persistent storage and atomic updates.
- Locking prevents duplicate job processing.

## Configuration

- Retry count and backoff base are configurable via CLI.
- Defaults are applied if no config set.

## Trade-offs

- Backoff is simple power function, no jitter/random delays.
- Workers use threads; not multiprocess (simpler for this assignment).
- SQLite chosen for persistence due to ease of use and reliability.

## Future Improvements

- Add job output logging.
- Web dashboard for live job monitoring.
- Job priority or scheduling.
- Use multiprocessing or async for worker scalability.

---
