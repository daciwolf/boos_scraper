"""
This module tests the communication queue without a message.
"""

from timeoutpool import (TimeoutJobBase,
                        TimeoutPool)

class ExceptionJob(TimeoutJobBase):
    """
    An exception job for testing
    """
    def __init__(self):
        """
        The constructor of the exception job
        """

    def execute(self):
        """
        Execute the exception job

        Returns:
            dict: the result of the exception job
        """
        raise ValueError("exception in job")

    def timeout(self):
        """
        Carry out the timeout process for the exception job

        Returns:
            dict: the return of the timeout post-processing
        """
        return {}

jobs = [ExceptionJob() for _ in range(5)]

def test_exception_job():
    """
    Testing the exception job.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.execute(jobs)

    assert len(results) == len(jobs)
