# CollapsingThreadPoolExecutor

The CollapsingThreadPoolExecutor is inspired by and compatible with the ThreadPoolExecutor from the
"futures" module, it operates differently in that worker threads are handled with a stack which results in the same worker or workers doing all the work (and idle workers being destroyed).

## How to install

    $ pip install collapsing-thread-pool-executor

## How to develop

**Prerequisites**

* python3 w/ pip
* python2 w/ pip
* virtualenvwrapper
* entr

**Set up the environments**

    $ mkvirtualenv -p `which python2.7` collapsing-thread-pool-executor-py2
    $ pip install .
    $ pip install -r requirements.txt

    # mkvirtualenv -p `which python3` collapsing-thread-pool-executor-py3
    $ pip install .
    # pip install -r requirements.txt

**Watch the tests**

    # watch python2 tests in one window
    $ workon collapsing-thread-pool-executor-py2
    $ find ./ -name '*.py' | entr -c py.test -v --log-level=DEBUG collapsing_thread_pool_executor
    
    # watch python3 tests in one window
    $ workon collapsing-thread-pool-executor-py3
    $ find ./ -name '*.py' | entr -c py.test -v --log-level=DEBUG collapsing_thread_pool_executor

## Examples

The example below will execute `some_task()` 100 times; as `some_task()` should take a second to execute and as we've allocated 10 workers, the whole thing should take about 10 seconds.

    import time

    from collapsing_thread_pool_executor import CollapsingThreadPoolExecutor

    def some_task():
        time.sleep(1)

    # all arguments are optional
    pool = CollapsingThreadPoolExecutor(
        workers=10,
        thread_name_prefix='SomePool',
        permitted_thread_age_in_seconds=60,
    )

    for i in range(0, 100):
        pool.submit(some_task)
