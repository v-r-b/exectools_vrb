from io import TextIOWrapper
from exectools_vrb import *

print("""
################################################
### sample usage of try_run 
### which succeeds in the end
################################################
""")
i = 0
good_i = 3
count = good_i

def count_log_fn(func_name: str, func_args: tuple, func_kwargs: dict,
                 run_count: int, exc: BaseException) -> None:
    print("Exception", exc, "occurred in function", func_name, "(", func_args, func_kwargs, ") in run nr.", run_count)

def count_upwards():
    global i, good_i

    i += 1
    print("i is now", i)
    if i < good_i:
        print("This is bad and leads to an exception")
        raise ValueError("Value for i is too little")
        1/0
    else:
        print("This is good")

try_run(count_upwards, count, 0.5, retry_exc_types=[ValueError], logfunc=count_log_fn)
assert i == count, "i should be " + str(count)

print("""
################################################
### sample usage of try_run 
### which fails in the end
################################################
""")
i = 0
good_i = 3
count = good_i-1    # this will not be enough

try:
    try_run(count_upwards, count, 0.5, retry_exc_types=[ValueError], logfunc=count_log_fn)
except BaseException as exc:
    assert isinstance(exc, ValueError), "should have caught a ValueError here"
    print("caught a ValueError as expected")
assert i == count, "i should be " + str(count)

print("""
################################################
### sample usage of TaskOnRequestThread
################################################
""")
def worker():
    print("carried out!")


tort = TaskOnRequestThread(worker, sys.stdout)
tort.start_thread()
print("### 1")
tort.run_periodically(1, 10)
print("### 2")
time.sleep(3)
print("### 3")
tort.quit(True)
print("### 4")
