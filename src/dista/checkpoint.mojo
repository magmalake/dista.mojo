"""Where actor checkpoints live: objects under `DISTA_CHECKPOINT_URI`.

The URI is a local directory, a `file://` one, or anything objectstore
writes to, such as `s3://bucket/prefix`. A local directory only outlives the
worker that wrote it if the node that restarts the actor sees it too, so a
cluster needs a shared volume or object storage.

Each checkpoint is its own object, named for its actor and the number of
calls it covers, `<uri>/<actor ID in hex>/<calls>`, so a new one never
overwrites the one the head would restore from. The worker hosting the
actor deletes the ones before that one, those its predecessor left
included.
"""

from std.os import getenv

from objectstore import FileIOResolver


def checkpoint_uri() raises -> String:
    """`DISTA_CHECKPOINT_URI`, without a trailing slash; raises if unset."""
    var uri = getenv("DISTA_CHECKPOINT_URI")
    if uri == "":
        raise Error("DISTA_CHECKPOINT_URI is not set: nowhere to checkpoint")
    while uri.endswith("/") and uri.byte_length() > 1:
        var trimmed = String(uri.removesuffix("/"))
        uri = trimmed^
    return uri^


def checkpoint_location(actor: UInt64, calls: Int) raises -> String:
    """Where actor `actor`'s checkpoint after `calls` calls lives."""
    return String(checkpoint_uri(), "/", hex(actor), "/", calls)


def list_checkpoints(actor: UInt64) raises -> List[Int]:
    """The calls covered by each checkpoint actor `actor` has, in any order."""
    var prefix = String(checkpoint_uri(), "/", hex(actor), "/")
    var out = List[Int]()
    var found: List[String]
    try:
        found = FileIOResolver().list(prefix)
    except:
        return out^  # nothing written there yet
    for ref loc in found:
        var cut = loc.rfind("/")
        try:
            out.append(Int(String(loc[byte = cut + 1 :])))
        except:
            pass  # not a checkpoint
    return out^


def write_checkpoint(location: String, data: Span[UInt8, _]) raises:
    FileIOResolver().new_output(location).overwrite(data)


def read_checkpoint(location: String) raises -> List[UInt8]:
    return FileIOResolver().new_input(location).read_all()


def delete_checkpoint(location: String):
    """Deletes a checkpoint nothing will restore from; a failure only leaves
    it behind."""
    try:
        FileIOResolver().delete(location)
    except e:
        print("dista worker: couldn't delete checkpoint", location, ":", e)
