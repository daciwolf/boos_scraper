"""
This module tests the communication queue with big messages.
"""

from timeoutpool import (TimeoutJobBase,
                        TimeoutPool)

class HugeMessageJob(TimeoutJobBase):
    """
    A huge message job for testing
    """
    def __init__(self):
        """
        The constructor of the huge message job
        """

    def execute(self):
        """
        Execute the huge message job

        Returns:
            dict: the result of the huge message job
        """
        return {'message': "a"*100000}

    def timeout(self):
        """
        Carry out the timeout process for the huge message job

        Returns:
            dict: the return of the timeout post-processing
        """
        return {'message': None}

jobs = [HugeMessageJob() for _ in range(5)]

def test_huge_message_job():
    """
    Testing the huge message job.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.execute(jobs)

    assert len(results) == len(jobs)
