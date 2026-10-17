"""next_munition_trail: the pure trail rule behind munition_track.trail_ecef."""
from persistence.postgres import MUNITION_TRAIL_SAMPLES, next_munition_trail


def test_appends_a_triple():
    assert next_munition_trail([1.0, 2.0, 3.0], 4.0, 5.0, 6.0) == [1, 2, 3, 4, 5, 6]


def test_caps_at_ten_triples_dropping_the_oldest():
    trail = None
    for i in range(1, 13):
        trail = next_munition_trail(trail, float(i), 0.0, 0.0)
    assert MUNITION_TRAIL_SAMPLES == 10
    assert len(trail) == 30
    assert trail[0::3] == [float(i) for i in range(3, 13)]


def test_duplicate_of_last_triple_returns_prev_unchanged():
    prev = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    assert next_munition_trail(prev, 4.0, 5.0, 6.0 + 5e-7) is prev
    assert prev == [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]


def test_non_duplicate_equal_to_older_triple_is_kept():
    prev = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    assert next_munition_trail(prev, 1.0, 2.0, 3.0) == [1, 2, 3, 4, 5, 6, 1, 2, 3]


def test_difference_beyond_tolerance_is_not_a_duplicate():
    assert next_munition_trail([1.0, 2.0, 3.0], 1.0, 2.0, 3.0 + 1e-3) == [
        1.0, 2.0, 3.0, 1.0, 2.0, 3.001]


def test_malformed_prev_restarts():
    for bad in (None, [], [1.0], [1.0, 2.0], [1.0, 2.0, 3.0, 4.0]):
        assert next_munition_trail(bad, 7.0, 8.0, 9.0) == [7.0, 8.0, 9.0]
