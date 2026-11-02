"""Where actor checkpoints live: objects under `DISTA_CHECKPOINT_URI`.

The URI is a local directory, a `file://` one, or anything objectstore
writes to, such as `s3://bucket/prefix`. A local directory only outlives the
worker that wrote it if the node that restarts the actor sees it too, so a
cluster needs a shared volume or object storage.

Each checkpoint is its own object, named for its actor and the number of
calls it covers, `<uri>/<actor ID in hex>/<calls>`, so a new one never
overwrites the one the head would restore from. The worker hosting the
actor deletes the ones before that one, those its predecessor left
included. A head that restarts restores each actor from its newest.

A checkpoint's bytes are, for each caller whose calls it includes, that
caller's number and the newest call's object ID, then the actor's state:
count u64, that many pairs of u64s, the state through the codec. Calls
resent after a head restart that it already includes aren't run again.
"""

from std.ffi import c_int, external_call
from std.os import getenv

from objectstore import FileIOResolver, parse_uri


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


def newest_checkpoint(actor: UInt64) raises -> Int:
    """The calls covered by actor `actor`'s newest checkpoint, or -1."""
    var newest = -1
    for c in list_checkpoints(actor):
        newest = max(newest, c)
    return newest


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
    """Writes a whole checkpoint, never part of one: after a head restart the
    newest is restored without anyone having said it was finished. An object
    store's put is whole already; a local file is written under a temporary
    name and renamed into place."""
    var fio = FileIOResolver()
    var uri = parse_uri(location)
    if uri.scheme != "file":
        fio.new_output(location).overwrite(data)
        return
    var final = uri.local_path()
    var tmp = String(final, ".tmp")
    fio.new_output(tmp).overwrite(data)
    if (
        external_call["rename", c_int](
            tmp.as_c_string_span(), final.as_c_string_span()
        )
        != 0
    ):
        raise Error("could not move ", tmp, " into place")


def read_checkpoint(location: String) raises -> List[UInt8]:
    return FileIOResolver().new_input(location).read_all()


def delete_checkpoint(location: String):
    """Deletes a checkpoint nothing will restore from; a failure only leaves
    it behind."""
    try:
        FileIOResolver().delete(location)
    except e:
        print("dista worker: couldn't delete checkpoint", location, ":", e)
