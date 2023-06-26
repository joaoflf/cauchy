import bisect
import os
import struct
import sys
from typing import BinaryIO, Optional, Tuple

import sortedcontainers


class LSMTree:
    """
    A Log-Structured Merge Tree (LSM Tree) to serve as the storage engine for the database.

    Args:
        memtable_max_size (int): The maximum size of the memtable in megabytes.
        sstable_block_size (int): The size of each block in the SSTable in kilobytes.
    """

    def __init__(self, memtable_max_size: int = 64, sstable_block_size: int = 4):
        self._memtable = sortedcontainers.SortedDict()
        self._memtable_max_size = memtable_max_size * 1024 * 1024
        self._sstable_block_size = sstable_block_size * 1024
        self.data_segments = []
        self.last_sstable_id = 0
        if not os.path.exists("storage"):
            os.mkdir("storage")

    def get(self, key: str) -> Optional[str]:
        memtable_result = self._memtable.get(key)
        if memtable_result is not None:
            return memtable_result
        else:
            for segment in self.data_segments:
                segment_result = self._find_key_in_segment(key, segment)
                if segment_result is not None:
                    return segment_result

    def put(self, key: str, value: str):
        self._memtable.update({key: value})
        if self._is_memtable_over_threshold():
            self._flush_memtable()

    # def delete(self, key):
    #        del self._memtable.pop(key)

    def _is_memtable_over_threshold(self):
        return len(self._memtable) >= 3
        return sys.getsizeof(self._memtable) > self._memtable_max_size

    def _flush_memtable(self):
        """
        Flushes the memtable to disk and creates a new memtable.
        Stores in a new SSTable the data that was in the memtable,
        respecting the block size and keeps a sparse index of block offset and first key in block.
        """
        data_segment_name = f"storage/segment_{self.last_sstable_id}"
        current_block_size = 0
        is_first_block = True
        offsets = {}
        test_count = 0
        with open(data_segment_name, "wb") as f:
            for key, value in self._memtable.items():
                test_count += 1

                # if is_first_block, then we need to add the offset of the first block
                if is_first_block:
                    offsets[key] = 0
                    is_first_block = False

                encoded_key, encoded_value = str(key).encode("utf-8"), str(
                    value
                ).encode("utf-8")
                key_length, value_length = len(encoded_key), len(encoded_value)

                # construct the format string with 4 bytes for the key and value length
                # and strings with the length of the key and value
                format_string = ">i{}si{}s".format(key_length, value_length)

                if test_count > 2:
                    # if (
                    #    current_block_size + struct.calcsize(format_string)
                    #    > self._sstable_block_size
                    # ):
                    current_block_size = 0
                    f.flush()
                    offsets[key] = f.tell()
                else:
                    current_block_size += struct.calcsize(format_string)

                f.write(
                    struct.pack(
                        format_string,
                        key_length,
                        encoded_key,
                        value_length,
                        encoded_value,
                    )
                )

        self.data_segments.append((data_segment_name, offsets))
        self._memtable.clear()

    def _find_block_range_for_key(
        self, key: str, block_offsets: dict
    ) -> Tuple[int, int]:
        """
        Returns the offsets of the SSTable blocks that bound the key in the segment.
        """
        block_offset_keys = list(block_offsets.keys())
        position = bisect.bisect_left(block_offset_keys, key)
        lower_bound = block_offset_keys[position - 1]
        upper_bound = block_offset_keys[position]
        if position == len(block_offset_keys):
            upper_bound = None
        if position == 0:
            lower_bound = None
        return lower_bound, upper_bound

    def _find_key_in_segment(
        self, key: str, segment: Tuple[str, dict]
    ) -> Optional[str]:
        """
        Finds the value of the key in the segment. If the key is not found, returns None.
        If the key is in the sparse index, then the value is read directly from the block.
        If not, get the block range that bounds the key and read the file between the lower and upper bound offsets and find the key.
        """

        segmant_name, block_offsets = segment
        if key in block_offsets:
            # if key is part of sparse index, read value directly from block
            value = self._read_value_from_segment(segment, block_offsets[key])[1]
            return value
        else:
            # if key is not part of sparse index, find the block range that bounds the key
            lower_bound, upper_bound = self._find_block_range_for_key(
                key, block_offsets
            )
            if lower_bound is None:
                # if lower_bound is None, then the key does not exist in the segment
                return None
            else:
                # else read the file bewteen the lower and upper bound offsets and find the key
                with open(segmant_name, "rb") as f:
                    current_block_offset = block_offsets[lower_bound]
                    while True:
                        f.seek(current_block_offset)
                        current_key, current_value = self._read_kay_and_value(f)
                        if key == current_key:
                            return current_value
                        elif upper_bound < current_key:
                            return None

                        current_block_offset = f.tell()

    def _read_kay_and_value(self, file: BinaryIO) -> Tuple[str, str]:
        """
        Reads a key and value from a file.
        The file pointer should be at the start of the key length.
        """
        key_length = struct.unpack(">i", file.read(4))[0]
        key = struct.unpack(f">{key_length}s", file.read(key_length))[0].decode("utf-8")
        value_length = struct.unpack(">i", file.read(4))[0]
        value = struct.unpack(f">{value_length}s", file.read(value_length))[0].decode(
            "utf-8"
        )
        return key, value


if __name__ == "__main__":
    tree = LSMTree()
    tree.put("key", "value")
    tree.put("key2", "value2")
    tree.put("key3", "value33")
    print(tree.get("key2"))
