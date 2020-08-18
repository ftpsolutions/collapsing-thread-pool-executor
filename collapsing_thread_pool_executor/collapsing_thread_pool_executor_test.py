import datetime
import time
import unittest
from concurrent.futures import as_completed

from hamcrest import assert_that, less_than_or_equal_to, greater_than_or_equal_to

from collapsing_thread_pool_executor import CollapsingThreadPoolExecutor


def short_sleep():
    time.sleep(0.1)


def long_sleep():
    time.sleep(1)


class CollapsingThreadPoolExecutorTest(unittest.TestCase):
    def setUp(self):
        self._subject = CollapsingThreadPoolExecutor(
            max_workers=10,
            thread_name_prefix='Test',
            permitted_thread_age_in_seconds=1
        )

    def test_some_fast_work(self):
        before = datetime.datetime.now()

        futures = []
        for i in range(0, 20):
            futures += [self._subject.submit(short_sleep)]

        for future in as_completed(futures):
            _ = future.result()

        after = datetime.datetime.now()

        assert_that(
            (after - before).total_seconds(),
            less_than_or_equal_to(0.3)
        )

    def test_some_slow_work(self):
        before = datetime.datetime.now()

        futures = []
        for i in range(0, 20):
            futures += [self._subject.submit(long_sleep)]

        for future in as_completed(futures):
            _ = future.result()

        after = datetime.datetime.now()

        assert_that(
            (after - before).total_seconds(),
            greater_than_or_equal_to(1.9)
        )
