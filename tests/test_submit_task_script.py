import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "submit_task.py"
SPEC = importlib.util.spec_from_file_location("submit_task_script", SCRIPT_PATH)
submit_task_script = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(submit_task_script)


class SubmitTaskScriptTests(unittest.TestCase):
    def test_parse_payload_reassembles_command_prompt_arguments(self):
        payload = submit_task_script.parse_payload(["'{\"value\":", "21}'"])

        self.assertEqual(payload, {"value": 21})

    def test_parse_payload_restores_command_prompt_stripped_key_quotes(self):
        payload = submit_task_script.parse_payload(["'{value:", "21}'"])

        self.assertEqual(payload, {"value": 21})


if __name__ == "__main__":
    unittest.main()
