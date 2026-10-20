import unittest

from app.tasks.models import Task
from app.tasks.registry import (
    DuplicateTaskType,
    InvalidTaskHandler,
    TaskRegistry,
    UnknownTaskType,
)


class TaskRegistryTests(unittest.TestCase):
    def test_register_and_get_handler(self):
        registry = TaskRegistry()

        def calculate(task):
            return task.payload["value"] * 2

        registry.register("calculate", calculate)

        self.assertIs(registry.get("calculate"), calculate)

    def test_run_calls_matching_handler(self):
        registry = TaskRegistry()
        task = Task.create("calculate", {"value": 21})

        def calculate(task):
            return {"result": task.payload["value"] * 2}

        registry.register("calculate", calculate)

        result = registry.run(task)

        self.assertEqual(result, {"result": 42})

    def test_unknown_task_type_raises_error(self):
        registry = TaskRegistry()
        task = Task.create("missing", {})

        with self.assertRaises(UnknownTaskType):
            registry.run(task)

    def test_duplicate_registration_is_rejected(self):
        registry = TaskRegistry()

        def handler(task):
            return None

        registry.register("calculate", handler)

        with self.assertRaises(DuplicateTaskType):
            registry.register("calculate", handler)

    def test_non_callable_handler_is_rejected(self):
        registry = TaskRegistry()

        with self.assertRaises(InvalidTaskHandler):
            registry.register("calculate", "not-a-function")



if __name__ == "__main__":
    unittest.main()