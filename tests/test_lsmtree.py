import shutil

import pytest

from lsmtree import LSMTree


@pytest.fixture
def tree():
    # Setup
    if shutil.os.path.exists("storage"):
        shutil.rmtree("storage")
    tree = LSMTree()
    yield tree


def test_put_and_get(tree):
    tree.put("test_key", "test_value")
    assert tree.get("test_key") == "test_value"
    assert tree.get("non_existent_key") is None


def test_memtable_overflow(tree):
    large_value = "x" * (tree._memtable_max_size // 2)
    large_value2 = "y" * (tree._memtable_max_size // 2 + 2)
    tree.put("test_key", large_value)
    tree.put("test_key2", large_value2)
    assert tree.get("test_key") == large_value
    assert (
        tree._memtable.get("test_key") is None
    )  # The data should have been flushed to disk


def test_read_from_disk(tree):
    tree.put("test_key", "test_value")
    tree._flush_memtable()
    assert tree.get("test_key") == "test_value"


def test_find_block_range_for_key(tree):
    mock_block_offsets = {"a": 0, "c": 3, "d": 5}
    assert tree._find_block_range_for_key("b", mock_block_offsets) == ("a", "c")
    assert tree._find_block_range_for_key("z", mock_block_offsets) == ("d", None)


def test_find_key_in_segment(tree):
    tree._memtable = {"a": "1", "b": 2, "c": 3.2}
    tree._flush_memtable()
    assert tree._find_item_in_segment("a", tree.data_segments[0]) == "1"
    assert tree._find_item_in_segment("b", tree.data_segments[0]) == 2
    assert tree._find_item_in_segment("c", tree.data_segments[0]) == 3.2
    assert tree._find_item_in_segment("z", tree.data_segments[0]) is None
