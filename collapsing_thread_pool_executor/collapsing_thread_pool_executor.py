import atexit
import sys
import threading
import weakref
from concurrent.futures import _base
from logging import getLogger
from uuid import uuid4

try:  # Python3
    import queue
except Exception:  # Python2
    import Queue as queue

try:  # Python2
    from concurrent.futures.thread import cpu_count
except BaseException:  # Python3
    from multiprocessing import cpu_count

# for the clean shutdown piece
_workers = weakref.WeakSet()
_shutdown = False
_thread_pools = weakref.WeakSet()


# for the clean shutdown piece
def _python_exit():
    global _shutdown

    _shutdown = True

    for w in _workers:
        w.work_item_manager.work_item = None
        w.work_item_available_event.set()

    for tp in _thread_pools:
        tp.shutdown()


atexit.register(_python_exit)  # for the clean shutdown piece


class _WorkItem(object):
    def __init__(self, future, fn, args, kwargs):
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if not self.future.set_running_or_notify_cancel():
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
        except BaseException:
            e, tb = sys.exc_info()[1:]
            self.future.set_exception_info(e, tb)
        else:
            self.future.set_result(result)


class _WorkItemManager(object):
    def __init__(self):
        self._lock = threading.Lock()
        self._work_item = None

    @property
    def work_item(self):
        with self._lock:
            return self._work_item

    @work_item.setter
    def work_item(self, work_item):
        with self._lock:
            self._work_item = work_item


class _Worker(threading.Thread):
    def __init__(self, executor_reference, work_item_manager, work_item_available_event, worker_available_callback,
                 timeout, name):
        super(_Worker, self).__init__(
            name=name
        )

        self._executor_reference = executor_reference
        self._work_item_manager = work_item_manager
        self._work_item_available_event = work_item_available_event
        self._worker_available_callback = worker_available_callback
        self._timeout = timeout

    @property
    def work_item_manager(self):
        return self._work_item_manager

    @property
    def work_item_available_event(self):
        return self._work_item_available_event

    def run(self):
        try:
            while True:
                # declare this thread as available
                self._worker_available_callback(self)

                # wait until task or shutdown on timeout
                work_available = self._work_item_available_event.wait(timeout=self._timeout)

                self._work_item_available_event.clear()

                if work_available:
                    work_item = self._work_item_manager.work_item
                    if work_item is not None:
                        self._work_item_manager.work_item = None
                else:  # shutdown this thread if there no was no work given
                    return

                if work_item is not None:  # do the work
                    work_item.run()
                    del work_item  # Delete references to object. See issue16284
                    continue

                # this path only executes if the work_item was None (pool shutdown commanded)
                executor = self._executor_reference()
                # Exit if:
                #   - The interpreter is shutting down OR
                #   - The executor that owns the worker has been collected OR
                #   - The executor that owns the worker has been shutdown.
                if _shutdown or executor is None:
                    return

                del executor
        except BaseException:
            _base.LOGGER.critical('Exception in worker', exc_info=True)


# based on concurrent.futures.thread.ThreadPoolexecutor
class CollapsingThreadPoolExecutor(_base.Executor):
    def __init__(self, max_workers=None, thread_name_prefix=None,
                 permitted_thread_age_in_seconds=30, logger=None):
        if max_workers is None:
            # Use this number because ThreadPoolExecutor is often
            # used to overlap I/O instead of CPU work.
            max_workers = (cpu_count() or 1) * 5
        if max_workers <= 0:
            raise ValueError("max_workers must be greater than 0")

        self._max_workers = max_workers
        self._thread_name_prefix = thread_name_prefix or '{0}'.format(hex(id(self))[2:])
        self._permitted_thread_age_in_seconds = permitted_thread_age_in_seconds

        self._logger = logger if logger is not None else getLogger(self.__class__.__name__)

        self._work_queue = queue.Queue()
        self._workers = set()
        self._workers_lock = threading.Lock()
        self._available_workers_queue = queue.LifoQueue()

        self._shutdown = False
        self._shutdown_lock = threading.Lock()

        self._cleanup_thread_shutdown_queue = queue.Queue()
        self._cleanup_threads_lock = threading.Lock()
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_threads
        )
        self._cleanup_thread.daemon = True
        self._cleanup_thread.start()

        self._work_queue_thread = threading.Thread(
            target=self._handle_work_queue,
        )
        self._work_queue_thread.daemon = True
        self._work_queue_thread.start()

        _thread_pools.add(self)

    def _worker_available(self, worker):
        self._available_workers_queue.put(worker)

    def _cleanup_threads(self):
        last_num_workers = -1
        while True:
            with self._shutdown_lock:
                if self._shutdown:
                    return

            dead_workers = []
            with self._workers_lock:
                for w in self._workers:
                    if w.ident and not w.isAlive():
                        dead_workers += [w]

                for w in dead_workers:
                    self._workers.remove(w)
                    self._logger.debug('removed {0}'.format(w))

                num_workers = len(self._workers)

            for w in dead_workers:
                self._logger.debug('joining {0}'.format(w))
                w.join()
                self._logger.debug('joined {0}'.format(w))

            if num_workers != last_num_workers:
                last_num_workers = num_workers
                self._logger.debug('{0} workers running'.format(
                    num_workers
                ))

            # makes for an interruptable sleep
            try:
                self._cleanup_thread_shutdown_queue.get(
                    timeout=self._permitted_thread_age_in_seconds)
                return
            except queue.Empty:
                pass

    def _handle_work_queue(self):
        while True:
            with self._shutdown_lock:
                if self._shutdown:
                    return

            # wait for some work
            try:
                work_item = self._work_queue.get(timeout=5)
                if work_item is None:  # shutdown commanded
                    return
            except queue.Empty:
                continue

            # wait for a worker
            wait = False
            worker = None
            while worker is None:
                try:
                    w = self._available_workers_queue.get_nowait() if not wait else self._available_workers_queue.get(
                        timeout=5
                    )
                except queue.Empty:
                    wait = self._adjust_thread_count()
                    continue

                if w is None:  # shutdown commanded
                    return
                elif w.ident and not w.isAlive():  # dead worker
                    continue

                worker = w

                break

            # give the work_item to the worker
            worker.work_item_manager.work_item = work_item

            # notify it of work to be done
            worker.work_item_available_event.set()

    def _adjust_thread_count(self):
        # When the executor gets lost, the weakref callback will wake up
        # the worker threads.
        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        with self._workers_lock:
            num_workers = len(self._workers)

        if num_workers == self._max_workers:
            return False

        thread_name = '{0}_{1}'.format(self._thread_name_prefix, uuid4())

        work_item_manager = _WorkItemManager()
        work_item_available_event = threading.Event()
        work_item_available_event.clear()

        w = _Worker(
            weakref.ref(self, weakref_cb),
            work_item_manager,
            work_item_available_event,
            self._worker_available,
            self._permitted_thread_age_in_seconds,
            name=thread_name,
        )
        w.daemon = True

        w.start()

        self._logger.debug('added {0}'.format(w))

        with self._workers_lock:
            self._workers.add(w)

        _workers.add(w)

    def submit(self, fn, *args, **kwargs):
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')

            f = _base.Future()
            w = _WorkItem(f, fn, args, kwargs)

            self._work_queue.put(w)

            return f

    submit.__doc__ = _base.Executor.submit.__doc__

    def shutdown(self, wait=True):
        self._logger.debug('setting shutdown flag')
        with self._shutdown_lock:
            self._shutdown = True

        self._logger.debug('shutting down work queue')
        self._work_queue.put(None)

        self._logger.debug('shutting down work queue thread')
        self._available_workers_queue.put(None)

        self._logger.debug('shutting down cleanup thread')
        self._cleanup_thread_shutdown_queue.put(1)

        self._logger.debug('joining cleanup thread')
        self._cleanup_thread.join()
        self._logger.debug('joined cleanup thread')

        if wait:
            with self._workers_lock:
                for w in self._workers:
                    self._logger.debug('joining {0}'.format(w))
                    w.join()
                    self._logger.debug('joined {0}'.format(w))

    shutdown.__doc__ = _base.Executor.shutdown.__doc__
