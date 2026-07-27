from producer.runner import Backoff


def test_exponential_growth_and_cap(monkeypatch):
    monkeypatch.setattr("producer.runner.random.uniform", lambda a, b: 1.0)
    b = Backoff(base_s=1.0, cap_s=60.0)
    assert [b.next() for _ in range(8)] == [1, 2, 4, 8, 16, 32, 60, 60]


def test_reset(monkeypatch):
    monkeypatch.setattr("producer.runner.random.uniform", lambda a, b: 1.0)
    b = Backoff(base_s=1.0, cap_s=60.0)
    b.next()
    b.next()
    b.reset()
    assert b.next() == 1.0


def test_jitter_bounds():
    b = Backoff(base_s=1.0, cap_s=60.0)
    for _ in range(100):
        b.attempt = 2
        assert 2.0 <= b.next() <= 6.0
