import logging
import os.path
import time
import tempfile
from orchestrator_errors import SchedulingException, NoSuchDirectoryException
from threading import Lock, Timer
import subprocess

logger = logging.getLogger(__name__)

class Orchestrator(object):
    '''The orchestrator class contains methods for scheduling tasks to be executed.
    Each task is a Python script. When a task is scheduled it is given a name, and 
    an optional list of names to serve as dependencies. The dependencies of a task
    must be executed before the task is executed, therefore an error will be thrown
    if a task is scheduled to be executed earlier than one of its dependencies.

    Future work: cache to disk so we can recover from server failure.

    '''
    def __init__(self, output_directory=None):
        '''Create an Orchestrator.

        Arguments:
            output_directory(str): pathname of the directory in which to put the 
            files containing the stdout and stderr of the executed tasks.
        '''
        # DAG of tasks to be run
        self.tasks = {}
        self.timer = {}

        # These are tasks that have no dependencies and therefore have timer object associated with them.
        # Each task has a timer object. In the future we could sort the tasks in order of scheduled execution 
        # time, and only create a timer for the next task to execute.
        self.next_to_execute = {}

        # Create a default output directory.
        if output_directory is None:
            output_directory = tempfile.mkdtemp()
            logger.info("No output directory specified, using {}".format(output_directory))

        if not os.path.isdir(output_directory):
            raise NoSuchDirectoryException('A valid output directory was not specified for orchestrator log files.')
        self.output_directory = output_directory


        # Ensures that there aren't any issues with concurrent threads adding/removing
        # tasks.
        self.mutex = Lock()

    def schedule(self, task_name, task_path, execution_time = None, depends_on = None, parameters = None):
        '''Schedule a task to be executed by the system. Regardless of the provided 
        execution time, a task will not be executed until all of its dependencies have
        been executed as well. A task is simply a python file.

        Arguments:
            task_name(str): A unique name for the task. If the task name is not unique
            then an exception will be thrown.

            task_path(str): The absolute path to an executable Python file on the system
            which is to be executed at the provided execution_time.

            execution_time(int): The epoch time (in milliseconds) at which to execute the 
            task. If the provided time is in the past, an exception will be thrown.

            depends_on(list(int)): A list of dependencies which must be run before 
            the task is executed. If a dependency is not found in the tasks list, it will 
            be ignored.

            parameters(list): A list of parameters to pass to the 

        Returns:
            The time (in ms) until the task is to be executed. 
        '''
        # Be overly safe. Everything is a critical section.
        self.mutex.acquire()

        # get the current time
        now_in_ms = int(time.time() * 1000)

        # No duplicate tasks
        if task_name in self.tasks:
            self.mutex.release()
            raise SchedulingException("The task name \"{}\" was not unique".format(task_name))

        if execution_time == None:
            # Execute now if time is unspecified.
            execution_time = now_in_ms

        if parameters is None:
            parameters = []

        if depends_on == None:
            depends_on = set()
        
        # Drop dependencies that aren't in the set of tasks to be scheduled.
        depends_on = {dep for dep in depends_on if dep in self.tasks}

        # check if the task is scheduled at a valid time. (ie, not before a dependency)
        for dep in depends_on:
            if self.tasks[dep]['execution_time'] > execution_time:
                self.mutex.release()
                raise SchedulingException("A task was scheduled to be executed before one of its dependencies")
        
        # Add task as dependent. Needed if the parent task gets cancelled.
        for dep in depends_on:
            self.tasks[dep]['dependents'].add(task_name)
        
        # create filename for stdout/stderr of task
        output_file = tempfile.mktemp(dir=self.output_directory, prefix=task_name)

        task = {
            'task_name' : task_name,
            'task_path' : task_path,
            'execution_time' : execution_time,
            'depends_on' : depends_on,
            'dependents' : set(),
            'output_file' : output_file,
            'parameters' : parameters
        }

        self.tasks[task_name] = task

        # If there are no dependencies for this task, schedule immediately.
        if len(depends_on) == 0:
            self._create_executor_and_schedule(task)

        # We are done reading/writing to the tasks graph
        self.mutex.release()
        return execution_time - now_in_ms

    def cancel(self, task_name):
        '''
        Cancel a task that has been scheduled to be run. If the task has already been
        executed, then no action is taken. If the task has not been run, then it will
        be un-scheduled.

        Arguments:
            task_name(str): The unique name of the task to be cancelled. If no such task
            is found, then an exception is thrown.

        Returns:
            The task that was cancelled.
        '''
        self.mutex.acquire()

        # Can't cancel a task which doesn't exist.
        if task_name not in self.tasks:
            self.mutex.release()
            raise SchedulingException("Task cancel failed: unable to find  \"{}\".".format(task_name))

        # stop the timer if the task was scheduled. remove it from the list.
        if task_name in self.next_to_execute:
            self.next_to_execute.pop(task_name).cancel()

        # If we do not remove downstream dependencies, then cancel the dependency 
        dependents = self.tasks[task_name]['dependents']
        depends_on = self.tasks[task_name]['depends_on']

        # dependent tasks no longer need to wait for this one
        for dependent in dependents:
            dependent_task = self.tasks[dependent]
            dependent_task['depends_on'].remove(task_name)
            
            # If a dependent was freed up, check if it can be scheduled.
            if len(dependent_task['depends_on']) == 0:
                self._create_executor_and_schedule(dependent_task)

        # this task is no longer a dependent of other tasks.
        for depends in depends_on:
            self.tasks[depends]['dependents'].remove(task_name)

        cancelled_task = self.tasks.pop(task_name)
        # schedule freed up tasks here

        self.mutex.release()
        return cancelled_task

    def remove(self, task_name):
        '''Identical to Orchestrator.cancel

        Exists in the API to logically distinguish removing a task because it has executed versus
        canceling a task before it has been run.
        '''
        return self.cancel(task_name)

    def _create_executor_and_schedule(self, task):
        '''Creates the timer for the task and schedules itself to be run.
        '''
        task_name = task['task_name']
        executor = _TaskExecutor(task_name, self)

        # Schedule the task
        seconds_from_now = task['execution_time']/1000.0 - time.time()
        timer = Timer(seconds_from_now, executor)
        timer.start()
        self.next_to_execute[task_name] = timer


class _TaskExecutor(object):
    def __init__(self, task_name, orchestrator):
        '''The _TaskExecutor class is a callable which serves two purposes:
                1) executes the python scripts once the timer runs out
                2) removes the task from the Orchestrator's graph
        '''
        self.task_name = task_name
        self.orchestrator = orchestrator

    def __call__(self):
        '''Executes the python script in the background once the task's timer completes. The 
        output of the executed python script is sent to a file specified by the Orchestrator.
        Arguments:
            None

        Returns:
            None
        '''
        task = self.orchestrator.tasks[self.task_name]
        task_path = task['task_path']
        parameters = task['parameters']
        
        # open the output file for writing
        with open(task['output_file'], 'w+') as output_file:
            # execute the python script in a subprocess
            process = subprocess.Popen(['python', task_path, *parameters], stdout=output_file, stderr=output_file)
            # wait until it is finished executing to close the file.
            process.wait()

        self.orchestrator.remove(self.task_name)

