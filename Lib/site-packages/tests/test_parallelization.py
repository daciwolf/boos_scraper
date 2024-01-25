"""
This module tests the parallelization ecosystem.
"""

import time

import pytest

from timeoutpool import (TimeoutJobBase,
                        TimeoutPool)

class SleepJob(TimeoutJobBase):
    """
    Sleep job for testing
    """
    def __init__(self, sleep):
        """
        The constructor of the sleep job
        """
        self.sleep = sleep

    def execute(self):
        """
        Execute the sleep job

        Returns:
            dict: the result of the sleep job
        """
        time.sleep(self.sleep)
        return {'slept': self.sleep}

    def timeout(self):
        """
        Carry out the timeout process for the sleep job

        Returns:
            dict: the return of the timeout post-processing
        """
        return {'slept': None}

sleeps = [1, 2, 6, 7]
sleeps_no_timeout = [1, 2]

sleep_jobs = [SleepJob(sleep) for sleep in sleeps]
sleep_jobs_no_timeout = [SleepJob(sleep) for sleep in sleeps_no_timeout]

def sleep_job(sleep):
    """
    Sleep job function

    Args:
        sleep (float): the time to sleep

    Returns:
        dict: the result of the sleeping
    """
    time.sleep(sleep)
    return {'slept': sleep}

######################
# testing with lists #
######################

def test_jobs_objects_timeout():
    """
    Testing the job objects with timeout.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.execute(sleep_jobs)

    assert len(results) == len(sleeps)

    assert all(isinstance(result, dict) for result in results)

def test_jobs_functions_timeout():
    """
    Testing the functions with timeout.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.apply(sleep_job, sleeps)

    assert len(results) == len(sleeps)

    dict_count = sum(isinstance(result, dict) for result in results)

    assert 0 < dict_count < len(sleeps)

def test_jobs_objects_no_timeout():
    """
    Testing the job objects without timeout.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=-1)

    results = tpool.execute(sleep_jobs_no_timeout)

    assert len(results) == len(sleeps_no_timeout)

    assert all(isinstance(result, dict) for result in results)

def test_jobs_functions_no_timeout():
    """
    Testing the job functions without timeout.
    """

    tpool = TimeoutPool(n_jobs=2, timeout=-1)

    results = tpool.apply(sleep_job, sleeps_no_timeout)

    assert len(results) == len(sleeps_no_timeout)

    dict_count = sum(isinstance(result, dict) for result in results)

    assert dict_count == len(sleeps_no_timeout)

def test_exceptions():
    """
    Testing the exception in the base class
    """

    toj = TimeoutJobBase()

    with pytest.raises(RuntimeError) as _:
        toj.execute()

    with pytest.raises(RuntimeError) as _:
        toj.timeout()

###########################
# testing with generators #
###########################

def test_jobs_objects_timeout_generator():
    """
    Testing the job objects with timeout.
    """
    sleep_jobs_generator = (SleepJob(sleep) for sleep in sleeps)

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.execute(sleep_jobs_generator)

    assert len(results) == len(sleeps)

    assert all(isinstance(result, dict) for result in results)

def test_jobs_functions_timeout_generator():
    """
    Testing the functions with timeout.
    """
    sleeps_generator = (sleep for sleep in sleeps)

    tpool = TimeoutPool(n_jobs=2, timeout=4)

    results = tpool.apply(sleep_job, sleeps_generator)

    assert len(results) == len(sleeps)

    dict_count = sum(isinstance(result, dict) for result in results)

    assert 0 < dict_count < len(sleeps)

def test_jobs_objects_no_timeout_generator():
    """
    Testing the job objects without timeout.
    """
    sleep_jobs_no_timeout_generator = (SleepJob(sleep) for sleep in sleeps_no_timeout)

    tpool = TimeoutPool(n_jobs=2, timeout=-1)

    results = tpool.execute(sleep_jobs_no_timeout_generator)

    assert len(results) == len(sleeps_no_timeout)

    assert all(isinstance(result, dict) for result in results)

def test_jobs_functions_no_timeout_generator():
    """
    Testing the job functions without timeout.
    """
    sleeps_no_timeout_generator = (sleep for sleep in sleeps_no_timeout)

    tpool = TimeoutPool(n_jobs=2, timeout=-1)

    results = tpool.apply(sleep_job, sleeps_no_timeout_generator)

    assert len(results) == len(sleeps_no_timeout)

    dict_count = sum(isinstance(result, dict) for result in results)

    assert dict_count == len(sleeps_no_timeout)
