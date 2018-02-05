import pytest
from hedgehog.utils import coroutine


class TestCoroutine(object):
    def test_no_coroutine(self):
        def sum(count):
            sum = 0
            for _ in range(0, count):
                num = yield sum
                sum += num
            yield sum

        add = sum(2)
        next(add)
        assert add.send(2) == 2
        assert add.send(3) == 5
        with pytest.raises(StopIteration):
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
        assert add(2) == 2
        assert add(3) == 5
        with pytest.raises(StopIteration):
            add()
