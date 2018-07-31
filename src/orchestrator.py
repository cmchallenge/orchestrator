import logging
import os.path
import time
from threading import Lock

logger = logging.getLogger(__name__)

class Orchestrator(object):
    '''The orchestrator class contains methods for scheduling tasks to be executed.
    Each task is a Python script. When a task is scheduled it is given a name, and 
    an optional list of names to serve as dependencies. The dependencies of a task
    must be executed before the task is executed, therefore an error will be thrown
    if a task is scheduled to be executed earlier than one of its dependencies.

    Future work: cache to disk so we can recover from server failure.

    '''
    def __init__(self):
        # DAG of tasks to be run
        self.tasks = {}

        # Ensures that there aren't any issues with concurrent threads adding/removing
        # tasks.
        self.mutex = Lock()

    def schedule(self, task_name, task_path, execution_time = None, depends_on = None):
        '''Schedule a task to be executed by the system. Regardless of the provided 
        execution time, a task will not be executed until all of its dependencies have
        been executed as well.

        Arguments:
            task_name(str): A unique name for the task. If the task name is not unique
            then False will be returned.

            task_path(str): The absolute path to an executable Python file on the system
            which is to be executed at the provided execution_time.

            execution_time(int): The epoch time (in milliseconds) at which to execute the 
            task. If the provided time is in the past, the task will be executed as soon
            as possible.

            depends_on(list(int)): A list of dependencies which must be run before 
            the task is executed. If a dependency is not found in the tasks list, it will 
            be ignored.

        Returns:
            True if the task was scheduled successfully, False otherwise.
        '''
        # Be overly safe. Everything is a critical section.
        self.mutex.acquire()

        # No duplicate tasks
        if task_name in self.tasks:
            self.mutex.release()
            return False

        if execution_time == None:
            # Execute now if time is unspecified.
            execution_time = int(time.time() * 1000)

        if depends_on == None:
            depends_on = set()
        
        # Drop dependencies that aren't in the set of tasks to be scheduled.
        depends_on = {dep for dep in depends_on if dep in self.tasks}

        # Add task as dependent. Needed if the parent task gets cancelled.
        for dep in depends_on:
            self.tasks['dep']['dependents'].add(task_name)

        task = {
            'task_path' : task_path,
            'execution_time' : execution_time,
            'depends_on' : depends_on,
            'dependents' : set()
        }

        self.tasks[task_name] = task

        # We are done reading/writing to the tasks graph
        self.mutex.release()
        return False

    def cancel(self, task_name):
        '''
        Cancel a task that has been scheduled to be run. If the task has already been
        executed, then no action is taken. If the task has not been run, then it will
        be un-scheduled.

        Arguments:
            task_name(str): The unique name of the task to be cancelled. If no such task
            is found, then no action is taken.

        Returns:
            True if the task was successfully cancelled, False if unsucessful.
        '''
        self.mutex.acquire()

        # Can't cancel a task which doesn't exist.
        if task_name not in self.tasks:
            self.mutex.release()
            return False
        
        # If we do not remove downstream dependencies, then cancel the dependency 
        dependents = self.tasks[task_name]['dependents']
        depends_on = self.tasks[task_name]['depends_on']

        # dependent tasks no longer need to wait for this one
        for dependent in dependents:
            self.tasks[dependent]['depends_on'].remove(task_name)

        # this task is no longer a dependent of other tasks.
        for depends in depends_on:
            self.tasks[depends]['dependents'].remove(task_name)

        self.task.pop[task_name]
        # schedule freed up tasks here

        self.mutex.release()
        return True