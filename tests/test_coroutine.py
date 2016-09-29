import unittest

from hedgehog.utils import coroutine


class CoroutineTests(unittest.TestCase):
    def test_no_coroutine(self):
        def sum(count):
            sum = 0
            for _ in range(0, count):
                num = yield sum
                sum += num
            yield sum

        add = sum(2)
        next(add)
        self.assertEqual(add.send(2), 2)
        self.assertEqual(add.send(3), 5)
        with self.assertRaises(StopIteration):
            next(add)

    def test_coroutine(self):
        @coroutine
        def sum(count):
            sum = 0
            for _ in range(0, count):
                num, = yield sum
                sum += num
            yield sum

        add = sum(2)
        self.assertEqual(add(2), 2)
        self.assertEqual(add(3), 5)
        with self.assertRaises(StopIteration):
            add()
