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