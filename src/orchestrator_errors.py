class SchedulingException(Exception):
    '''An exception to throw when there is an error in scheduling a task.
    '''
    def __init__(self, msg):
        '''
        Arguments: 
            msg(str): a message to indicate what when wrong while schedule the task.
        '''
        Exception.__init__(self, msg)

class NoSuchDirectoryException(Exception):
    '''An exception to throw when the output directory for the Orchestrator can not be found.
    '''
    def __init__(self, msg):
        '''
        Arguments: 
            msg(str): a message to indicate that an invalid output directory was supplied.
        '''
        Exception.__init__(self, msg)