import subprocess
import time

def run_command(cmd):
    print(f">>> {cmd}")
    proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(proc.stdout.decode(), proc.stderr.decode())
    return proc

def test_multiple_workers():
    # Enqueue multiple jobs
    jobs = [
        {"id": "tjob1", "command": "echo t1"},
        {"id": "tjob2", "command": "echo t2"},
        {"id": "failjob", "command": "exit 1"}
    ]
    for job in jobs:
        run_command(f'python -m queuectl.cli enqueue \'{str(job)}\'')

    # Start multiple workers
    run_command('python -m queuectl.cli worker start --count 3')

    # Sleep to let workers process
    time.sleep(5)

    # Check job states
    run_command('python -m queuectl.cli status')
    run_command('python -m queuectl.cli list --state completed')
    run_command('python -m queuectl.cli dlq list')

def test_backoff_and_dlq():
    # Enqueue a job that will always fail
    run_command('python -m queuectl.cli enqueue \'{"id":"backoffjob","command":"exit 1"}\'')
    run_command('python -m queuectl.cli worker start --count 1')
    time.sleep(15)  # Allow time for backoff retries
    run_command('python -m queuectl.cli dlq list')
    run_command('python -m queuectl.cli dlq retry backoffjob')
    run_command('python -m queuectl.cli list --state pending')

if __name__ == "__main__":
    print("Testing multiple workers and concurrency:")
    test_multiple_workers()
    print("\nTesting retry with backoff and DLQ:")
    test_backoff_and_dlq()
