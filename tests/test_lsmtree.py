import os

import pytest
from LSMTree import LSMTree


@pytest.fixture
def tree():
    # Setup
    tree = LSMTree()
    yield tree
    # Teardown
    if os.path.exists("storage"):
        os.rmdir("storage")


def test_put_and_get(tree):
    tree.put("test_key", "test_value")
    assert tree.get("test_key") == "test_value"
    assert tree.get("non_existent_key") is None


def test_memtable_overflow(tree):
    large_value = "x" * (tree._memtable_max_size + 1)
    tree.put("test_key", large_value)
    assert tree.get("test_key") == large_value
    assert (
        tree._memtable.get("test_key") is None
    )  # The data should have been flushed to disk


def test_find_block_range_for_key(tree):
    mock_block_offsets = {"a": 0, "b": 2, "c": 3}
    assert tree._find_block_range_for_key("b", mock_block_offsets) == ("a", "c")
    assert tree._find_block_range_for_key("z", mock_block_offsets) == ("c", None)


def test_find_key_in_segment(tree):
    tree._memtable = {"a": 1, "b": 2, "c": 3}
    tree._flush_memtable()
    assert tree._find_key_in_segment("b", tree.data_segments[0]) == 2
    assert tree._find_key_in_segment("z", tree.data_segments[0]) is None
