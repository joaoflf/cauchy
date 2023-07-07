import bisect
import os
import struct
import threading
import time
from typing import BinaryIO, Optional, Tuple, Union

import sortedcontainers
from pympler import asizeof

U = Union[int, float, str]


class LSMTree:
    """
    A Log-Structured Merge Tree (LSM Tree) to serve as the storage engine for the database.

    Args:
        memtable_max_size (int): The maximum size of the memtable in megabytes.
        sstable_block_size (int): The size of each block in the SSTable in kilobytes.
    """

    def __init__(
        self,
        memtable_max_size: int = 64,
        sstable_block_size: int = 4,
        merge_interval: float = 3600,
    ):
        self._memtable: dict[str, U] = sortedcontainers.SortedDict()
        self._memtable_being_flushed: dict[str, U] = sortedcontainers.SortedDict()
        self._memtable_max_size = memtable_max_size * 1024 * 1024
        self._sstable_block_size = sstable_block_size * 1024
        self._merge_interval = merge_interval
        self._data_segments = []
        self._last_sstable_id = 0
        self._last_merged_sstable_id = 0
        if not os.path.exists("storage"):
            os.mkdir("storage")

        # call _merge_and_compact() every hour in a separate thread
        self._merge_scheduler: Optional[threading.Timer] = threading.Timer(
            self._merge_interval, self._merge_and_compact
        )
        self._merge_scheduler.start()

    def get(self, key: str) -> Optional[U]:
        if self._memtable.get(key) == "tombstone":
            return None

        memtable_result = self._memtable.get(key) or self._memtable_being_flushed.get(
            key
        )
        if memtable_result is not None:
            return memtable_result
        else:
            for segment in reversed(self._data_segments):
                segment_result = self._find_item_in_segment(key, segment)
                if segment_result is not None:
                    return segment_result[0]

    def put(self, key: str, value: U):
        self._memtable.update({key: value})
        if self._is_memtable_over_threshold():
            self._flush_memtable()

    def delete(self, key):
        if key in self._memtable:
            self._memtable.update({key: "tombstone"})
        else:
            segment_name = None
            item_offset = 0
            for segment in reversed(self._data_segments):
                segment_result = self._find_item_in_segment(key, segment)
                if segment_result is not None:
                    segment_name = segment[0]
                    item_offset = segment_result[1]
                    break
            if segment_name:
                self._mark_item_as_tombstoned(segment_name, item_offset)
            else:
                raise KeyError(f"Key {key} not found")

    def _is_memtable_over_threshold(self):
        return asizeof.asizeof(self._memtable) > self._memtable_max_size

    def _flush_memtable(self):
        """
        Flushes the memtable to disk and creates a new memtable.
        """

        # provide new memtable to receive new writes
        self.memtable_being_flushed = self._memtable
        self._memtable = sortedcontainers.SortedDict()

        self._last_sstable_id += 1
        data_segment_name = f"storage/segment_{self._last_sstable_id}"

        offsets = self._write_dict_to_data_segment(
            data_segment_name, self.memtable_being_flushed
        )
        self._data_segments.append((data_segment_name, offsets))
        self.memtable_being_flushed.clear()

    def _write_dict_to_data_segment(
        self, data_segment_name: str, dict_to_write: dict
    ) -> dict:
        """
        Writes a dictionary to a data segment and returns the a sparse index of the keys in the segment.
        """
        current_block_size = 0
        is_first_block = True
        offsets = {}
        with open(data_segment_name, "wb") as f:
            for key, value in dict_to_write.items():
                if value != "tombstone":
                    if is_first_block:
                        # if is_first_block, then we need to add the offset of the first block
                        offsets[key] = 0
                        is_first_block = False

                    format_string = self._generate_struct_format_string(str(key), value)

                    if (
                        current_block_size + struct.calcsize(format_string)
                        > self._sstable_block_size
                    ):
                        # if the current block size + the size of the new key value pair is greater than the block size,
                        # then we need to start a new block
                        f.flush()
                        offsets[key] = f.tell()
                        current_block_size = struct.calcsize(format_string)
                    else:
                        current_block_size += struct.calcsize(format_string)

                    encoded_key = str(key).encode("utf-8")
                    key_length = len(encoded_key)
                    type_char = (
                        "i"
                        if isinstance(value, int)
                        else "d"
                        if isinstance(value, float)
                        else "s"
                    )
                    if isinstance(value, int) or isinstance(value, float):
                        f.write(
                            struct.pack(
                                format_string,
                                key_length,
                                encoded_key,
                                False,
                                type_char.encode("utf-8"),
                                value,
                            )
                        )
                    else:
                        encoded_value = str(value).encode("utf-8")
                        value_length = len(encoded_value)
                        f.write(
                            struct.pack(
                                format_string,
                                key_length,
                                encoded_key,
                                False,
                                type_char.encode("utf-8"),
                                value_length,
                                encoded_value,
                            )
                        )
        return offsets

    def _generate_struct_format_string(self, key: str, value: U) -> str:
        """
        Builds the format string for the serialization of the key and value.
        Order is key_length, key, tombstone, value_type, value_length(if str), value.
        """
        key_length = len(str(key).encode("utf-8"))

        if isinstance(value, int):
            return ">i{}s?ci".format(key_length)
        elif isinstance(value, float):
            return ">i{}s?cd".format(key_length)
        elif isinstance(value, str):
            value_length = len(str(value).encode("utf-8"))
            return ">i{}s?ci{}s".format(key_length, value_length)
        else:
            raise (TypeError("Unsupported type for value"))

    def _find_block_range_for_key(
        self, key: str, block_offsets: dict
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns the offsets of the SSTable blocks that bound the key in the segment.
        """
        block_offset_keys = list(block_offsets.keys())
        position = bisect.bisect_left(block_offset_keys, key)

        lower_bound = (
            block_offset_keys[position - 1]
            if position in range(1, len(block_offset_keys) + 1)
            else block_offset_keys[position]
            if position == 0
            else None
        )
        upper_bound = (
            block_offset_keys[position] if position != len(block_offset_keys) else None
        )

        return lower_bound, upper_bound

    def _find_item_in_segment(
        self, key: str, segment: Tuple[str, dict]
    ) -> Optional[Tuple[U, int]]:
        """
        Finds an item in the given segment. If the key is not found, returns None.
        If the key is in the sparse index and not tombstoned, then the value is read directly from the block.
        If not, get the block range that bounds the key and read the file between the lower and upper bound offsets and find the key.
        Return value and offset of the item in the segment.
        """
        segment_name, block_offsets = segment
        if key in block_offsets:
            # if key is part of sparse index, navigate to the block offset and read the key and value
            with open(segment_name, "rb") as f:
                f.seek(block_offsets[key])
                key_read, value, is_tombstoned = self._read_item_from_disk(f)
                if not is_tombstoned:
                    return (value, block_offsets[key])

        lower_bound, upper_bound = self._find_block_range_for_key(key, block_offsets)
        if lower_bound is None:
            # if lower_bound is None, then the key does not exist in the segment
            return None
        else:
            with open(segment_name, "rb") as f:
                current_block_offset = block_offsets[lower_bound]
                while not self._is_EOF(f):
                    f.seek(current_block_offset)

                    (
                        current_key,
                        current_value,
                        is_tombstoned,
                    ) = self._read_item_from_disk(f)

                    if key == current_key and not is_tombstoned:
                        return (current_value, current_block_offset)
                    elif upper_bound is not None and upper_bound < current_key:
                        # if we passed the upper bound, then the key does not exist in the segment
                        return None
                    current_block_offset = f.tell()

    def _is_EOF(self, file: BinaryIO) -> bool:
        next_byte = file.read(1)
        if next_byte != b"":
            # if next_byte is not empty, restore the file pointer
            file.seek(-1, 1)
        return next_byte == b""

    def _read_item_from_disk(self, file: BinaryIO) -> Tuple[str, U, bool]:
        """
        Reads a key, value, and tombstone bit from the file.
        File pointer must be at the start of the key.
        """
        key_length = struct.unpack(">i", file.read(4))[0]
        key = struct.unpack(f">{key_length}s", file.read(key_length))[0].decode("utf-8")
        is_tombstoned = struct.unpack(">?", file.read(1))[0]
        value_type = struct.unpack(">c", file.read(1))[0].decode("utf-8")
        if value_type == "s":
            value_length = struct.unpack(">i", file.read(4))[0]
            value = str(
                struct.unpack(f">{value_length}s", file.read(value_length))[0].decode(
                    "utf-8"
                )
            )
        elif value_type == "i":
            value = int(struct.unpack(">i", file.read(4))[0])
        elif value_type == "d":
            value = float(struct.unpack(">d", file.read(8))[0])
        else:
            raise (TypeError("Unsupported type for value"))

        return str(key), value, bool(is_tombstoned)

    def _mark_item_as_tombstoned(self, segment_name: str, offset: int):
        """
        Marks the item at the given offset in the segment as tombstoned.
        """
        with open(segment_name, "r+b") as f:
            key_length = struct.unpack(">i", f.read(4))[0]
            f.seek(offset + 4 + key_length)
            f.write(struct.pack(">?", True))

    def _merge_and_compact(self):
        """
        Merges all data segments into a single segment and deletes the old segments.
        Duplicate keys are resolved by taking the value from the segment with the highest priority.
        Tombstoned keys are not added to the merged segment.
        """
        if len(self._data_segments) > 1:
            self._last_merged_sstable_id += 1
            merged_data_segment_name = (
                f"storage/merged_segment_{self._last_merged_sstable_id}"
            )
            merged_values = sortedcontainers.SortedDict()

            for segment in self._data_segments:
                with open(segment[0], "rb") as f:
                    while not self._is_EOF(f):
                        key, value, is_tombstoned = self._read_item_from_disk(f)
                        if not is_tombstoned:
                            merged_values.update({key: value})
                        elif key in merged_values:
                            merged_values.pop(key)
                os.remove(segment[0])

            offsets = self._write_dict_to_data_segment(
                merged_data_segment_name, merged_values
            )
            self._data_segments = [(merged_data_segment_name, offsets)]

    def _stop_merge_scheduler(self):
        self._merge_scheduler.cancel()
        self._merge_scheduler = None
