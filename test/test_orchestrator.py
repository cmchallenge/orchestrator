import unittest
from orchestrator import Orchestrator
import tempfile

class TestOrchestrator(unittest.TestCase):
    def test_orch(self):
        orch = Orchestrator()
        self.assertIsNotNone(orch)

    def test_orch_graph(self):
        orch = Orchestrator()
        orch.schedule('a', '/not/a/real/path.py', execution_time=314159)
        sched_response = orch.schedule('b', '/another/fake/path.py', depends_on=['a'], execution_time=111)

        self.assertTrue(sched_response)

        should_be = {
            'a': {
                'task_path':'/not/a/real/path.py',
                'execution_time': 314159,
                'depends_on': set(),
                'dependents': {'b'}
            },
            'b': {
                'task_path': '/another/fake/path.py', 
                'execution_time': 111,
                 'depends_on': {'a'},
                  'dependents': set()
            }
        }

        self.assertEqual(orch.tasks, should_be)

        cancel_response = orch.cancel('a')
        self.assertTrue(cancel_response)

        should_be_after_cancel = {'b': {
                'task_path': '/another/fake/path.py', 
                'execution_time': 111,
                 'depends_on': set(),
                  'dependents': set()
            }
        }

        self.assertEqual(orch.tasks, should_be_after_cancel)
        