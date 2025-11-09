import subprocess
import time
import json

def run_cli_command(command):
    proc = subprocess.run(command, shell=True, capture_output=True, text=True)
    return proc.stdout, proc.stderr, proc.returncode

def test_enqueue_and_process():
    # Enqueue a job
    job = {"id": "testjob", "command": "echo test output"}
    out, err, code = run_cli_command(f"python -m queuectl.cli enqueue '{json.dumps(job)}'")
    assert "Job enqueued" in out

    # List should show this job pending
    out, _, _ = run_cli_command("python -m queuectl.cli list --state pending")
    assert "testjob" in out

    # Start a worker in a background process to process the queue
    import threading

    def start_worker():
        subprocess.run("python -m queuectl.cli worker start --count 1", shell=True)

    worker_thread = threading.Thread(target=start_worker, daemon=True)
    worker_thread.start()

    # Wait for worker to process
    time.sleep(3)

    # Check completed jobs
    out, _, _ = run_cli_command("python -m queuectl.cli list --state completed")
    assert "testjob" in out

def test_failed_job_and_dlq():
    # Enqueue a failing job
    job = {"id": "failjob", "command": "exit 1"}
    out, _, _ = run_cli_command(f"python -m queuectl.cli enqueue '{json.dumps(job)}'")
    assert "Job enqueued" in out

    import threading

    def start_worker():
        subprocess.run("python -m queuectl.cli worker start --count 1", shell=True)

    worker_thread = threading.Thread(target=start_worker, daemon=True)
    worker_thread.start()

    # Wait longer for retries and DLQ
    time.sleep(15)

    # Check that job shows in DLQ
    out, _, _ = run_cli_command("python -m queuectl.cli dlq list")
    assert "failjob" in out

    # Retry DLQ job
    out, _, _ = run_cli_command("python -m queuectl.cli dlq retry failjob")
    assert "moved back" in out or "moved" in out or "retry" in out

