import unittest
from orchestrator import Orchestrator

class TestOrchestrator(unittest.TestCase):
    def setUp(self):
        pass

    def test_orch(self):
        orch = Orchestrator()
        self.assertIsNotNone(orch)