import json
from datetime import datetime

class Job:
    VALID_STATES = {"pending", "processing", "completed", "failed", "dead"}

    def __init__(self, id, command, state="pending",
                 attempts=0, max_retries=3,
                 created_at=None, updated_at=None):
        self.id = id
        self.command = command
        self.state = state if state in Job.VALID_STATES else "pending"
        self.attempts = attempts
        self.max_retries = max_retries
        self.created_at = created_at or datetime.utcnow().isoformat()
        self.updated_at = updated_at or self.created_at

    def to_dict(self):
        return {
            "id": self.id,
            "command": self.command,
            "state": self.state,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "created_at": self.created_at,
            "updated_at": self.updated_at
        }

    @staticmethod
    def from_dict(data):
        return Job(
            id=data["id"],
            command=data["command"],
            state=data.get("state", "pending"),
            attempts=data.get("attempts", 0),
            max_retries=data.get("max_retries", 3),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at")
        )

    def increment_attempts(self):
        self.attempts += 1
        self.updated_at = datetime.utcnow().isoformat()

    def update_state(self, new_state):
        if new_state in Job.VALID_STATES:
            self.state = new_state
            self.updated_at = datetime.utcnow().isoformat()
        else:
            raise ValueError(f"Invalid job state: {new_state}")

    def has_retries_left(self):
        return self.attempts < self.max_retries

    def __str__(self):
        return json.dumps(self.to_dict(), indent=2)
