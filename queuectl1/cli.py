import click
import json
from queuectl1.queue import enqueue_job, list_jobs_by_state
from queuectl1.worker import start_workers, stop_workers, active_worker_count
from queuectl1.storage import init_db
from queuectl1.dlq import list_dlq_jobs, retry_dlq_job
from queuectl1.config import set_config_value, get_config_value

@click.group()
def cli():
    """QueueCTL CLI - Manage background job queue system."""
    init_db()  # Initialize DB/Tables at startup

@cli.command()
@click.argument('job_json')
def enqueue(job_json):
    """Add a new job (JSON string) to the queue."""
    try:
        job = json.loads(job_json)
        enqueue_job(job)
        click.echo(f"Job enqueued with ID: {job.get('id')}")
    except json.JSONDecodeError:
        click.echo("Invalid JSON format for job")
    except Exception as e:
        click.echo(f"Error enqueueing job: {e}")

@cli.group()
def worker():
    """Commands to start/stop workers."""
    pass

@worker.command('start')
@click.option('--count', default=1, help='Number of workers to start')
def start(count):
    """Start one or more worker processes."""
    start_workers(count)
    click.echo(f"Started {count} worker(s)")

@worker.command('stop')
def stop():
    """Stop all running workers gracefully."""
    stop_workers()
    click.echo("Requested workers to stop gracefully")

@cli.command()
def status():
    """Show summary of job states and active workers."""
    states = ['pending', 'processing', 'completed', 'failed', 'dead']
    click.echo("Jobs summary:")
    for state in states:
        jobs = list_jobs_by_state(state)
        click.echo(f"  {state}: {len(jobs)}")
    count = active_worker_count()
    click.echo(f"Active workers: {count}")

@cli.command('list')
@click.option('--state', required=True, help='List jobs by state')
def list_jobs(state):
    """List jobs filtered by state."""
    jobs = list_jobs_by_state(state)
    if not jobs:
        click.echo(f"No jobs found with state '{state}'")
    else:
        for job in jobs:
            click.echo(job)

@cli.group()
def dlq():
    """Dead Letter Queue commands."""
    pass

@dlq.command('list')
def dlq_list():
    """List jobs in the Dead Letter Queue."""
    jobs = list_dlq_jobs()
    if not jobs:
        click.echo("Dead Letter Queue is empty")
    else:
        for job in jobs:
            click.echo(job)

@dlq.command('retry')
@click.argument('job_id')
def dlq_retry(job_id):
    """Retry a job from the Dead Letter Queue by ID."""
    success = retry_dlq_job(job_id)
    if success:
        click.echo(f"Job {job_id} moved back to queue for retry")
    else:
        click.echo(f"Job {job_id} not found in Dead Letter Queue")

@cli.group()
def config():
    """Configuration management commands."""
    pass

@config.command('set')
@click.argument('key')
@click.argument('value')
def config_set(key, value):
    """Set configuration key to given value."""
    set_config_value(key, value)
    click.echo(f"Config '{key}' set to '{value}'")

@config.command('get')
@click.argument('key')
def config_get(key):
    """Get configuration value by key."""
    value = get_config_value(key)
    click.echo(f"{key} = {value if value is not None else '(not set)'}")

if __name__ == '__main__':
    cli()
