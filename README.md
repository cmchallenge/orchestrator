# Orchestrator

Basic usage:

Tasks are python scipts. A task can be scheduled for immediate execution as follows. Task names must
be unique.
```Python
from orchestrator import Orchestrator

orch = Orchestrator()
# pass in the task name and path to python scipt
orch.schedule("reticulate_splines", "/path/to/spline_reticulator.py")
```

Any output from a task is sent to a data directory (one is created automatically if none is specified).
In addition, the response code for each executed task is written to the data dir to indicate whether a
task completed successfully.

```Python
orch = Orchestrator('path/to/data/dir')
orch.schedule("flux_capacitation", "./my_flux_cap.py")
```

A task can be scheduled to be executed in the future by using the `execution_time` argument. It specifies
the epoch time (in ms) at which the script should be executed. A script can be schedule to be run 3 seconds 
from now as follows:
```Python
import time 

start = time.time() * 1000 + 3000

orch = Orchestrator()
orch.schedule("future_hello", "./say_hello.py", execution_time = start)
```

Tasks can have dependencies. A task will not be executed until its dependency has been run, even if a 
dependency blocks past a dependent task's scheduled execution time. Dependencies are specifies using 
the `depends_on` parameter, which takes a list of task names:
```Python
import time 

start = time.time() * 1000 + 3000

orch = Orchestrator()
orch.schedule("task_A", "task_A.py", execution_time = start)
orch.schedule("task_B", "task_B.py", execution_time = start + 3000, depends_on = ['task_A', 'task_B'])

orch.schedule("needs_A_and_B", "wait_for_AB.py", execution_time = start + 4000)
```

A task can be listed as recurring. The `recur_period` parameters specifies the timespan (in ms)
between recurring executions of the task. For example, to run a task every 5.5 seconds:

```Python
orch = Orchestrator()
orch.schedule("start_my_roomba", "./roomba_exec.py",recur_period = 5500)
```

