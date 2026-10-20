import unittest

from app.queue.manager import WorkQueue, WorkQueueEmpty, WorkQueueFull


class WorkQueueTests(unittest.TestCase):
    def test_enqueue_and_dequeue_task_id(self):
        queue = WorkQueue()

        queue.enqueue("task-1")

        self.assertEqual(queue.depth, 1)
        self.assertEqual(queue.dequeue(), "task-1")
        self.assertEqual(queue.depth, 0)

    def test_empty_queue_raises_clear_error(self):
        queue = WorkQueue()

        with self.assertRaises(WorkQueueEmpty):
            queue.dequeue()

    def test_bounded_queue_raises_when_full(self):
        queue = WorkQueue(maxsize=1)

        queue.enqueue("task-1")

        with self.assertRaises(WorkQueueFull):
            queue.enqueue("task-2")

    def test_empty_task_id_is_rejected(self):
        queue = WorkQueue()

        with self.assertRaises(ValueError):
            queue.enqueue("")

    def test_negative_maxsize_is_rejected(self):
        with self.assertRaises(ValueError):
            WorkQueue(maxsize=-1)

    def test_default_maxsize_is_bounded(self):
        queue = WorkQueue()
        self.assertGreater(queue._queue.maxsize, 0)


if __name__ == "__main__":
    unittest.main()