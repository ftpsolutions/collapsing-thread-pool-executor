try:  # Python2
    from collapsing_thread_pool_executor import CollapsingThreadPoolExecutor
except BaseException:  # Python3
    from collapsing_thread_pool_executor.collapsing_thread_pool_executor import CollapsingThreadPoolExecutor

# stop editors like PyCharm from optimizing this away (as it's not used in this module)
_ = CollapsingThreadPoolExecutor
