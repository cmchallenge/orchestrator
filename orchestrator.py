import logging

logger = logging.getLogger(__name__)

def Orchestrator(object):
    '''The orchestrator class contains methods for scheduling tasks to be executed.
    Each task is a Python script. When a task is scheduled it is given a name, and 
    an optional list of names to serve as dependencies. The dependencies of a task
    must be executed before the task is executed, therefore an error will be thrown
    if a task is scheduled to be executed earlier than one of its dependencies.

    Future work: cache to disk so we can recover from server failure.

    '''
    def __init__(self):
        self.tasks = {}
    
    def run(self):
        pass

    def schedule(self, task_name, task_path, execution_time = None, dependencies = None):
        '''Schedule a task to be executed by the system. Regardless of the provided 
        execution time, a task will not be executed until all of its dependencies have
        been executed as well.

        Arguments:
            task_name(str): A unique name for the task. If the task name is not unique
            then the value -1 will be returned.

            task_path(str): The absolute path to an executable Python file on the system
            which is to be executed at the provided execution_time.

            execution_time(int): The epoch time (in milliseconds) at which to execute the 
            task. If the provided time is in the past, the task will be executed as soon
            as possible.

            dependencies(list(int)): A list of dependency name which must be run before 
            the task is executed. If a dependency is not found in the tasks list, it will 
            be ignored.

        Returns:
            The return value is the number of milliseconds to wait from now until the 
            task is executed. Returns -1 if there was an error in scheduling the task.
        '''
        pass

    def cancel(self, task_name):
        '''
        Cancel a task that has been scheduled to be run. If the task has already been
        executed, then no action is taken. If the task has not been run, then it will
        be un-scheduled as well as any tasks that are dependent on the cancelled task.

        Arguments:
            task_name(str): The unique name of the task to be cancelled. If no such task
            is found, then no action is taken.

        Returns:
            The return value is a list of tasks that have been cancelled.
        '''
        pass