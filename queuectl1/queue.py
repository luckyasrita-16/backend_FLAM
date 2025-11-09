from datetime import datetime
from queuectl1.job import Job
from queuectl1.storage import save_job, get_pending_job, update_job_state, list_jobs_by_state

def enqueue_job(job_data):
    """
    Enqueue a new job to the queue.
    Accepts a dictionary containing job fields.
    """
    # Create a Job instance from data dict
    job = Job.from_dict(job_data)
    # Ensure job is in 'pending' state initially
    job.state = "pending"
    job.created_at = datetime.utcnow().isoformat()
    job.updated_at = job.created_at
    job.attempts = 0
    
    # Save the job persistently
    save_job(job.to_dict())

def get_next_pending_job():
    """
    Fetch the next pending job from storage to process.
    Returns a Job instance or None if no pending job exists.
    """
    data = get_pending_job()
    if data:
        return Job.from_dict(data)
    return None

def update_job(job: Job):
    """
    Update existing job in storage with current state.
    """
    job.updated_at = datetime.utcnow().isoformat()
    save_job(job.to_dict())

def set_job_state(job_id, new_state, attempts=None):
    """
    Utility to update job state and attempt count.
    """
    update_job_state(job_id, new_state, attempts=attempts, updated_at=datetime.utcnow().isoformat())

def list_jobs(state):
    """
    List all jobs filtered by given state.
    Returns list of job dicts.
    """
    return list_jobs_by_state(state)
