import subprocess
import threading
import time
import signal
import sys
from datetime import datetime
from queuectl1.queue import get_next_pending_job, update_job, set_job_state
from queuectl1.storage import move_job_to_dlq, get_config

# Thread-safe flag to stop workers gracefully
stop_event = threading.Event()

def worker_loop(worker_id):
    print(f"Worker {worker_id} started.")

    while not stop_event.is_set():
        job = get_next_pending_job()
        if not job:
            # No pending job, sleep before retrying
            time.sleep(1)
            continue

        # Mark job as processing
        set_job_state(job.id, 'processing')

        print(f"Worker {worker_id} processing job: {job.id} - {job.command}")

        # Run the job command
        try:
            result = subprocess.run(job.command, shell=True)
            exit_code = result.returncode
        except Exception as e:
            exit_code = 1
            print(f"Worker {worker_id} error running job {job.id}: {e}")

        # Determine success/failure
        if exit_code == 0:
            # Success: Mark completed
            set_job_state(job.id, 'completed')
            print(f"Worker {worker_id} completed job {job.id}")
        else:
            # Failure: Retry logic
            job.increment_attempts()
            if job.has_retries_left():
                # Calculate exponential backoff delay
                base_str = get_config('backoff_base', '2')  # Default base 2
                try:
                    base = float(base_str)
                except ValueError:
                    base = 2
                delay = base ** job.attempts
                # Update attempts and set state back to failed (retryable)
                set_job_state(job.id, 'failed', attempts=job.attempts)
                print(f"Worker {worker_id} failed job {job.id}, will retry after {delay:.1f} sec (attempt {job.attempts})")
                time.sleep(delay)
            else:
                # Move to Dead Letter Queue
                move_job_to_dlq(job.id, failed_reason="Max retries exceeded")
                print(f"Worker {worker_id} moved job {job.id} to Dead Letter Queue")

    print(f"Worker {worker_id} exiting gracefully.")

def start_workers(count=1):
    threads = []
    for i in range(count):
        t = threading.Thread(target=worker_loop, args=(i+1,))
        t.daemon = True
        t.start()
        threads.append(t)

    # Listen for shutdown signal
    def signal_handler(sig, frame):
        print("\nReceived shutdown signal, stopping workers...")
        stop_event.set()
        # Wait for threads to exit
        for thread in threads:
            thread.join()
        print("All workers stopped. Exiting.")
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep main thread alive while workers run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        signal_handler(None, None)

def stop_workers():
    print("Stop requested: Setting stop event.")
    stop_event.set()

def active_worker_count():
    # For this simple threading example, count active threads
    return threading.active_count() - 1  # exclude main thread
