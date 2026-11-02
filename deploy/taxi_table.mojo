"""The taxi table as the benchmarks read it: `deploy/taxi.mojo` and
`deploy/shuffle.mojo` build with `-I deploy` and share this.

    TAXI_METADATA   the table's metadata location, as the table names it
    TAXI_PREFIX     the warehouse location the table was written at
    TAXI_ROOT       where that warehouse is mounted here (default: the same)
"""

from std.os import getenv

from iceberg import DataFile, FileIO, FileScanTask, TableMetadata
from iceberg.manifest import CONTENT_DATA
from iceberg.catalog.filesystem import read_metadata_file

from dista import Seq
from dista.store import node_name


@fieldwise_init
struct PlannedFile(Copyable, Movable):
    """What the driver planned for one data file, so the worker doesn't plan
    again. The taxi table has no delete files and no identity partitions,
    which is what lets this leave out the deletes and the partition values."""

    var path: String
    var format: String
    var size: Int64
    var records: Int64
    var residual: String
    var start: Int64
    var length: Int64
    var spec_id: Int
    var sequence: Int64
    var split_offsets: Seq[Int64]
    """Where each row group starts: what lets a task own only the row groups
    in its byte range. Without them a split task reads the whole file."""

    @staticmethod
    def of(t: FileScanTask) raises -> PlannedFile:
        if len(t.delete_files) > 0:
            raise Error(t.data_file.file_path, " has deletes")
        ref f = t.data_file
        return PlannedFile(
            f.file_path.copy(),
            f.file_format.copy(),
            f.file_size_in_bytes,
            f.record_count,
            t.residual.copy(),
            t.start,
            t.length,
            t.spec_id,
            t.data_sequence_number,
            Seq[Int64](f.split_offsets.copy()),
        )

    def task(self) -> FileScanTask:
        var offsets = List[Int64](capacity=len(self.split_offsets))
        for i in range(len(self.split_offsets)):
            offsets.append(self.split_offsets[i])
        var f = DataFile(
            CONTENT_DATA,
            self.path.copy(),
            self.format.copy(),
            [],
            self.records,
            self.size,
            [],
            [],
            [],
            False,
            offsets^,
            [],
            0,
            False,
            0,
            False,
            String(),
            False,
            0,
            False,
            0,
            False,
        )
        return FileScanTask(
            f^,
            [],
            self.residual.copy(),
            self.start,
            self.length,
            self.spec_id,
            self.sequence,
        )


def taxi_io() -> FileIO:
    var io = FileIO.local()
    var prefix = getenv("TAXI_PREFIX")
    var root = getenv("TAXI_ROOT", prefix)
    if prefix != "" and root != prefix:
        io.rebase(prefix, root)
    return io^


def taxi_table() raises -> TableMetadata:
    """The table's metadata, read once by the driver."""
    return read_metadata_file(taxi_io(), getenv("TAXI_METADATA"))


def worker_node() -> String:
    var n = getenv("NODE_NAME")
    return n if n != "" else node_name()
