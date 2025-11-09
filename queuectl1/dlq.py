from queuectl1.storage import list_dlq_jobs, retry_dlq_job

def list_dlq():
    """
    Return the list of jobs in the Dead Letter Queue.
    """
    return list_dlq_jobs()

def retry_dlq(job_id):
    """
    Retry a job from Dead Letter Queue by moving it back to pending queue.
    Returns True if successful, False if job not found.
    """
    return retry_dlq_job(job_id)
