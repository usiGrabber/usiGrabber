import os
import random

USIGRABBER_JOB_ID_ENV_VAR = "USIGRABBER_JOB_ID"
SLURM_JOB_ID = "SLURM_JOB_ID"


def get_job_id() -> str:
    if os.environ.get(SLURM_JOB_ID):
        job_id = f"slurm-{os.environ.get(SLURM_JOB_ID)}"
        return job_id
    elif os.environ.get(USIGRABBER_JOB_ID_ENV_VAR):
        job_id = os.environ.get(USIGRABBER_JOB_ID_ENV_VAR)
        assert job_id  # Why does the typechecker not get that this cant be None?
        return job_id
    else:
        # Potential race condition right here if individual threats were to do this at the same time
        job_id = f"local-{random.randint(0, 10**6)}"
        os.environ[USIGRABBER_JOB_ID_ENV_VAR] = job_id
        return job_id
