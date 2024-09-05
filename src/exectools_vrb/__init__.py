"""
# exectools_vrb

Various tools to execute functions in python.

  - On PyPI: https://pypi.org/project/exectools-vrb/
  - On GitHub: https://github.com/v-r-b/exectools_vrb 

This module defines:

```function try_run()```
  
Exception proof function call. If the call succeeds, return the result of func.
If not, repeat the call under certain circumstances instead of raising an
exception immediately. Can be used if exceptions are expected, e.g. TimeoutErrors.

```type TryRunLogFunc```
  
Function type for logging exceptions to be used with in try_run(). 

```class TaskOnRequestThread```

Thread class which carries out a task (spefified by a callback
function) whenever it is requested to do so (by setting a flag).
There's also a method which will run this task periodically.
"""

import sys, time, threading, signal, traceback, logging
from datetime import datetime, timezone
from functools import partial
from io import TextIOWrapper
from typing import Any, Callable, TextIO

logger = logging.getLogger(__name__)

TryRunLogFunc = Callable[[str, tuple, dict, int, BaseException], None]
""" Function type for logging exceptions to be used with try_run(). 
Function definitions may be of the form:

a_log_func(func_name: str, func_args: tuple, func_kwargs: dict,
           run_count: int, exc: BaseException) -> None

Args:
    func_name (str): name of function in which the exception occurred
    func_args (tuple): positional arguments to the function called.
    func_kwargs (dict[str, Any]): keyword arguments to the function called
    run_count (int): number of the current execution attempt
    exc (BaseException): exception that has occurred
"""

def try_run(func: Callable, count: int = 2, delay: float = 1.0, *,
            retry_exc_types: list[BaseException.__class__]=[BaseException], 
            logfunc: TryRunLogFunc|None=None) -> Any:
    """ Exception proof function call. If the call succeeds, return the result of func.
    If not, repeat the call under certain circumstances instead of raising an
    exception immediately. Can be used if exceptions are expected, e.g. TimeoutErrors.
   
    If func has positional and/or keyword arguments, use partial from functools.

    Args:
        func (Callable): function to be called, may be a partial function
        count (int, optional): max. number of attempts before giving up. 
            If count is less than 1, exactly one call is made. Defaults to 2.
        delay (float, optional): delay in seconds between two calls. Defaults to 1.0 
        retry_exc_types (list|None, optional): list of "allowed" exception types for a retry. 
            Defaults to [BaseException] ( = any exception allowed)
        logfunc (LogFunc|None, optional): log function to log "allowed" errors. e.g. as warnings. Defaults to None.

    Returns:
        Any: return value of func

    Examples:
    1) function with no arguments at all
    try_run(myfunc, 4)
    Resulting call: myfunc(), max. 4 attempts

    2) function with two pos. arguments
    try_run(partial(myfunc, "Hello", "World"), delay=5)
    Resulting call: myfunc("Hello", "World"), delay of 5 seconds
    
    3) function with one pos. argument and one keyword argument
    try_run(partial(myfunc, "Hello", message="World"), logfunc=logexc)
    Resulting call: myfunc("Hello", message="World"), call logexc in case of an exception

    4) function with keyword arguments only
    try_run(partial(myfunc, s1="Hello", s2="World"), retry_exc_types=[TimeoutError])
    Resulting call: myfunc(s1="Hello", s2="World"), retry only in case of TimeoutError
    
    If an exception occurs during the first count-1 tries, test if it's type (or one of
    its supertypes) is in retry_exc_types.

    If yes: call logfunc (if given) and repeat the call. The parameters to logfunc will be 
    - the name of the function called, its positional args (tuple) and keyword args (dict)
    - the run count,
    - the exception that has occurred. 

    Otherwise: reraise the exception.

    If there's any exception at the last try, it will not be caught.
    """
    # call max. count-1 times (last call at the end of the function)
    for i in range(count-1):
        try:
            return func()
        except BaseException as exc:
            allowed_exc_found = False
            # check for "allowed" exception (considering subtyping)
            for exc_type in retry_exc_types:
                if isinstance(exc, exc_type):
                    allowed_exc_found = True
                    break
            if allowed_exc_found:
                # on "allowed" exceptions call log function, if given
                if logfunc is not None:
                    if isinstance(func, partial):
                        logfunc(func.func.__name__, func.args, func.keywords, i, exc)
                    else:
                        logfunc(func.__name__, (), {}, i, exc)
                # then pause before next call
                time.sleep(delay)
            else:
                # reraise disallowed exceptions
                raise

    # last time call without try/except
    return func()



class TaskOnRequestThread:
    """Thread class which carries out a task (spefified by a callback
    function) whenever it is requested to do so (by setting a flag).
    There's also a method which will run this task periodically.
    """

    MINUTE_SECONDS: int = 60
    """ number of seconds in one minute """
    HOUR_SECONDS: int = 3600
    """ number of seconds in one hour """
    DAY_SECONDS: int = 86400
    """ number of seconds in one day """
    WEEK_SECONDS: int = 604800
    """ number of seconds in one week """
    INFINITE_SECONDS: int = 10**100
    """ "infinite" number of seconds """

    def __init__(self, worker, do_debug: bool|int|TextIO|None = None):
        """Constructor. Initializes some internal fields 
        and stores the argument's values. If an external
        callback function (worker argument) with parameters
        shall be called, a partial function can be used:

        argworker = partial(some_callback_fn, arg1, args, arg3)

        tort = TaskOnRequestThread(argworker)

        Args:
            worker (callback function w/o args): external callback function
                        to be used by the thread. It has no arguments (see above).
            do_debug (bool, optional): TextIO to print to, loglevel to use logger
                        or True to use sys.stdout. None or False: no logging

        For backwards compatibility, do_debug may be set to True (its type was bool until
        version 0.0.2), which has the same effect as passing sys.stdout as an argument.                        
        """
        # when set to True, the callback function will be called.
        self._run_worker = False
        # when set to True, the thread will be terminated.
        self._do_quit = False
        # callback function to be called
        self._worker = worker
        # False or None means: NO Logging
        # True means: write progress information to stdout.
        # TextIO means: write progress information there
        # int means: use logger with specified loglevel
        if do_debug == True:
            do_debug = sys.stdout
        self._do_debug = do_debug
        # the new thread itself
        self._thread = None
        # the id of the new thread
        self._thread_ident = 0

    def _debug(self, msg: str):
        """Print a message to stdout or to a logger, depending on self._do_debug

        Args:
            msg (str): message to print
        """
        if not self._do_debug:
            return
        if isinstance(self._do_debug, int):
            logger.log(self._do_debug, msg)
        elif isinstance(self._do_debug, TextIO) or isinstance(self._do_debug, TextIOWrapper): 
            print(msg, file=self._do_debug)
        else:
            raise ValueError(f"do_debug must be of type int or TextIO, but is of type {type(self._do_debug)}")

    def _internal_worker(self):
        """The function which is run in a separate thread.
        It handles the states of the _run_worker flag (it
        calls the callback function _worker then) and the
        _do_quit flag (it terminates then) within an endless loop. 
        """
        self._thread_ident = threading.get_ident()
        self._debug(f"(iw) Started internal worker thread {self._thread_ident}.")
        while True:
            # wait until run or quit is requested
            while not self._run_worker and not self._do_quit:
                time.sleep(1)
            # Calling return ends the method and the thread
            if self._do_quit:
                self._debug(f"(iw) Quitting from internal worker thread {self._thread_ident}.")
                return
            # Call external worker function and set._run_worker to False
            # to make next call possible
            elif self._run_worker:
                self._debug(f"(iw) Calling external worker function in thread {self._thread_ident}.")
                # use try/except statements to avoid breaking the thread.
                # In case of an exception a message will be printed to stderr.
                try:
                    self._worker()
                except Exception as exc:
                    print(f"External worker function has terminated abnormally "
                          f"in thread {self._thread_ident}.", file=sys.stderr)
                    print(f"Exception {exc.__class__.__name__} has been caught:\n"
                          f"{traceback.format_exc()}", file=sys.stderr)
                    self._run_worker = False
                    continue
                
                self._debug(f"(iw) External worker function has finished in thread {self._thread_ident}.")
                self._run_worker = False

    def start_thread(self):
        """Start _internal_worker() in a separate thread.
        """
        # do not try to start twice
        if self.is_alive():
            self._debug(f"[st] Internal worker thread {self._thread_ident} is already running.")
            return
        else:
            self._debug("[st] Starting new internal worker thread.")
        self._run_worker = False
        self._do_quit = False
        # create and start thread
        self._thread = threading.Thread(target=self._internal_worker)
        self._thread.start()

    def quit(self, do_wait: bool = False):
        """Stop thread by setting _do_quit flag. If do_wait is True,
        the method blocks until the thread has been terminating.
        Otherwise it returns immediately after setting the flag.

        Args:
            do_wait (bool, optional): wait for thread to terminate, if True. Defaults to False.
        """
        self._debug(f"[q ] Stopping internal worker thread {self._thread_ident}.")
        # set _do_quit flag, which will be handled by _internal_worker()
        self._do_quit = True
        if do_wait:
            # wait until thread terminates
            while self.is_alive():
                self._debug(f"[q ] Waiting for end of thread {self.get_thread_ident()}")
                time.sleep(1)

    def run_worker(self) -> bool:
        """Initiate a run of the external callback function by setting
        the _run_worker flag. While the external callback function is running
        (_run_worker == True), any further request to call it will be ignored.

        Returns:
            bool: True, if the callback function run was initiated,
                  False, if there already was a running callback function.
        """
        if self._run_worker:
            # Already running. Don't request new start
            self._debug(f"[rw] External worker is already running in thread {self._thread_ident}.")
            return False
        else:
            # set _run_worker flad, which will be handled in _internal_worker()
            self._run_worker = True
            self._debug(f"[rw] Request new start of external worker in thread {self._thread_ident}.")
            return True
    
    def is_alive(self) -> bool:
        """Informs whether the thread is running or nor.

        Returns:
            bool: True, if the thread is running, False otherwise.
        """
        if isinstance(self._thread, threading.Thread):
            return self._thread.is_alive()
        return False

    def get_thread(self) -> threading.Thread | None:
        """Returns a reference to the thread object itself.

        Returns:
            threading.Thread: the thread object itself
        """
        return self._thread 

    def get_thread_ident(self) -> int:
        """Returns the thread id.

        Returns:
            int: the thread id
        """
        return self._thread_ident 

    def join(self, timeout: None = None) -> None:
        """ Calls join on the internal _thread object

        Args:
            timeout (float, optional): see threading.join(). Defaults to None.
        """
        if isinstance(self._thread, threading.Thread):
            self._thread.join(timeout)

    def _timestamp_utc(self) -> int:
        """Returns a UTC timestamp of the current time

        Returns:
            int: UTC timestamp of the current time (in seconds)
        """
        return int(datetime.timestamp(datetime.now(timezone.utc)))

    def run_periodically(self, interval_s: int, 
                               duration_s: int = INFINITE_SECONDS):
        """Calls run_worker() every interval_s seconds. The loop will
        end after duration_s seconds.

        Args:
            interval_s (int): time in seconds between two calls of run_worker()
            duration_s (int, optional): duration of the loop in seconds. 
                Defaults to INFINITE_SECONDS.
        """
        # save start timestamp
        start_timestamp_utc = self._timestamp_utc()
        ts_diff = 0
        # as long a we shall not quit and
        # as long as the execution time is less than duration_s:
        while not self._do_quit and (ts_diff < duration_s):
            # every interval_s seconds do:
            if ts_diff % interval_s == 0:
                self._debug(f"[rp] Call run_worker() in thread {self._thread_ident} at t={ts_diff}")
                self.run_worker()
            time.sleep(0.5)
            ts_diff = self._timestamp_utc() - start_timestamp_utc
        # after duration_s seconds: stop thread and wait for it to terminate
        self.quit(True)
        ts_diff = self._timestamp_utc() - start_timestamp_utc
        self._debug(f"[rp] End of thread {self._thread_ident} at t={ts_diff}")

    def _signal_handler(self, signum, frame):
        """Signal handler which calls quit(True) to stop the thread.
        The handler will block until the thread terminated to allow
        clean termination of the program. Signum and frame are the
        standard arguments for a signal handler.

        Args:
            signum (int): Signal that has been received.
            frame (stackframe): current position in the call stack.
        """
        print(f"Signal {signum} caught. Stopping execution.", file=sys.stderr)
        # stop thread and wait for it to terminate
        self.quit(True)

    def install_signal_handler(self, signalnum):
        """Install _signal_handler as signal handler for the given signal.

        Args:
            signalnum (int): Signal that shall be handled by _signal_handler()

        Returns:
            signal._HANDLER: former signal handler for the signal.
        """
        return signal.signal(signalnum, self._signal_handler)
