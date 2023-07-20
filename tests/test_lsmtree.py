import os
import shutil
import threading
from unittest import mock

import pytest

from lsmtree import LSMTree


@pytest.fixture
def tree():
    # Setup
    if os.path.exists("storage"):
        shutil.rmtree("storage")
    tree = LSMTree(storage_location="storage/")
    yield tree
    # Teardown
    tree._stop_merge_scheduler()


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
    assert tree._find_item_in_segment("a", tree._data_segments[0])[0] == "1"
    assert tree._find_item_in_segment("b", tree._data_segments[0])[0] == 2
    assert tree._find_item_in_segment("c", tree._data_segments[0])[0] == 3.2
    assert tree._find_item_in_segment("z", tree._data_segments[0]) is None


def test_delete(tree):
    tree.put("a", "1")
    tree.put("b", 2)
    tree.put("c", 3.2)
    tree.delete("b")
    assert tree.get("b") is None
    tree._flush_memtable()
    assert tree._find_item_in_segment("b", tree._data_segments[0]) is None
    tree.delete("a")
    assert tree.get("a") is None
    assert tree.get("c") == 3.2


def test_merge_and_compact(tree):
    total_segment_size = 0
    for i in range(10):
        a = i if i % 2 == 0 else i - 1
        tree.put(str(a), "value")
        tree._flush_memtable()
        total_segment_size += os.path.getsize(tree._data_segments[-1][0])
    tree._merge_and_compact()
    assert len(tree._data_segments) == 1
    assert os.path.getsize(tree._data_segments[0][0]) == total_segment_size / 2
    assert tree.get("0") == "value"


def test_merge_is_called():
    mock_merge = mock.Mock()
    with mock.patch.object(LSMTree, "_merge_and_compact", new=mock_merge):
        tree = LSMTree(storage_location="storage/", merge_interval=0.01)
        tree.put("a", "1")
        tree._flush_memtable()
        tree.put("b", 2)
        tree._flush_memtable()
        threading.Event().wait(0.02)
        assert mock_merge.call_count > 0
        tree._stop_merge_scheduler()


def test_update_order(tree):
    tree.put("a", "1")
    tree._flush_memtable()
    tree.put("a", 2)
    assert tree.get("a") == 2
    tree._flush_memtable()
    assert tree.get("a") == 2
    tree._merge_and_compact()
    assert tree.get("a") == 2
    tree.put("a", 3.0)
    tree._flush_memtable()
    assert tree.get("a") == 3.0
    tree._merge_and_compact()
    assert tree.get("a") == 3.0
