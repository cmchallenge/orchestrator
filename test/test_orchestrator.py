import unittest
from orchestrator import Orchestrator
from orchestrator_errors import SchedulingException
import tempfile
import time

class TestOrchestrator(unittest.TestCase):
    def test_orch(self):
        orch = Orchestrator()
        self.assertIsNotNone(orch)

    def test_orch_graph(self):
        '''Test that a graph can be created, that tasks can be scheduled and cancelled.
        '''
        now = int(time.time() * 1000)
        orch = Orchestrator()

        # fake task to schedule. Ensure that it was scheduled correctly.
        sched_response = orch.schedule('a', '/not/a/real/path.py', execution_time=now + 314159)
        self.assertTrue(sched_response)

        # another fake task to schedule. Ensure that it was scheduled correctly.
        sched_response = orch.schedule('b', '/another/fake/path.py', depends_on=['a'], execution_time=now + 414159)
        self.assertTrue(sched_response)

        # check that output file names are not nonoe
        output_a = orch.tasks['a'].pop('output_file')
        output_b = orch.tasks['b'].pop('output_file')
        self.assertIsNotNone(output_a)
        self.assertIsNotNone(output_b)

        # This should be the current dependency graph.
        should_be = {
            'a': {
                'task_name': 'a', 
                'task_path': '/not/a/real/path.py', 
                'execution_time': now + 314159, 
                'depends_on': set(), 
                'dependents': {'b'}
                }, 
            'b': {
                'task_name': 'b',
                'task_path': '/another/fake/path.py',
                'execution_time': now + 414159,
                'depends_on': {'a'},
                'dependents': set()
                }
            }

        self.assertEqual(orch.tasks, should_be)

        # Cancel a task and check that the graph correctly updates.
        cancel_response = orch.cancel('a')
        self.assertTrue(cancel_response)

        should_be_after_cancel = {'b': {
                'task_name': 'b',
                'task_path': '/another/fake/path.py', 
                'execution_time': now + 414159,
                'depends_on': set(),
                'dependents': set()
            }
        }

        self.assertEqual(orch.tasks, should_be_after_cancel)
        orch.cancel('b')

    def test_impossible_schedule(self):
        '''Create a task which is supposed to execute before one of its dependencies.
        Verify that an exception is thrown.
        '''
        now = int(time.time() * 1000)
        orch = Orchestrator()

        # fake task to schedule. Ensure that it was scheduled correctly.
        sched_response = orch.schedule('a', '/not/a/real/path.py', execution_time=now + 314159)
        self.assertTrue(sched_response)

        # Create an anonymous function to schedule the task that fails
        should_fail = lambda: orch.schedule('b', 'fake.py', depends_on=['a'], execution_time=now + 314158)

        # Ensure that the task throws an exception when scheduled since it is set to be run 
        # before its dependency
        self.assertRaises(SchedulingException, should_fail)
        orch.cancel('a')

    def test_non_unique_task_name(self):
        '''Create a task with a non-unique name. Verify that an exception is thrown.
        '''
        now = int(time.time() * 1000)
        orch = Orchestrator()

        # fake task to schedule. Ensure that it was scheduled correctly.
        sched_response = orch.schedule('a', '/not/a/real/path.py', execution_time=now + 314159)
        self.assertTrue(sched_response)

        # Create an anonymous function to schedule the task that fails
        should_fail = lambda: orch.schedule('a', 'fake.py', depends_on=['a'], execution_time=now + 414159)

        # Ensure that the task throws an exception when scheduled since it doesn't have a unique name.
        self.assertRaises(SchedulingException, should_fail)
        orch.cancel('a')
        