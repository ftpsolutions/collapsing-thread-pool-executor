import datetime
import logging
import sys
import time
import unittest

from hamcrest import assert_that, equal_to, less_than_or_equal_to
from mock import MagicMock, call

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
        work = MagicMock()
        work.side_effect = short_sleep()

        before = datetime.datetime.now()

        for i in range(0, 20):
            self._subject.submit(work)

        while len(work.mock_calls) < 20:
            time.sleep(0.01)

        after = datetime.datetime.now()

        assert_that(
            work.mock_calls,
            equal_to([call()] * 20)
        )

        assert_that(
            (after - before).total_seconds(),
            less_than_or_equal_to(0.2)
        )

    def test_some_slow_work(self):
        for i in range(0, 5):
            work = MagicMock()
            work.side_effect = long_sleep()

            before = datetime.datetime.now()

            for i in range(0, 20):
                self._subject.submit(work)

            while len(work.mock_calls) < 20:
                time.sleep(0.01)

            after = datetime.datetime.now()

            assert_that(
                work.mock_calls,
                equal_to([call()] * 20)
            )

            assert_that(
                (after - before).total_seconds(),
                less_than_or_equal_to(0.2)
            )
