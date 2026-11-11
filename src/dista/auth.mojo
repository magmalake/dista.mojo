"""A cluster token (#124): with `DISTA_TOKEN` set, every connection to a
head or to a worker's object server starts with an `auth` frame (kind 34)
carrying it, and one that doesn't, or carries another, is closed unanswered.
As Ray's token mode, it authenticates and doesn't encrypt: keep a cluster on
a private network, and the token stops anything else there from joining it,
submitting to it or fetching its results.

    DISTA_TOKEN=...            the token, or
    DISTA_TOKEN_FILE=path      its first line (a mounted secret)

Unset, nothing changes: no `auth` frame is sent, none is asked for, and one
that arrives anyway is ignored. Every process of a cluster gets the same
token, as they get the same build.
"""

from std.os import getenv

from dista.frame import KIND_AUTH, write_frame
from dista.socket import Stream

comptime PRE_AUTH_BYTES = 8 * 1024
"""The most a connection may send before its `auth` frame: a token's frame is
far smaller, and nothing larger is read from a peer not yet known."""


def cluster_token() -> String:
    """`DISTA_TOKEN`, else the first line of `DISTA_TOKEN_FILE`, else empty."""
    var t = getenv("DISTA_TOKEN")
    if t:
        return t
    var path = getenv("DISTA_TOKEN_FILE")
    if not path:
        return ""
    try:
        var text: String
        with open(path, "r") as f:
            text = f.read()
        var nl = text.find("\n")
        return String(text[byte=:nl]) if nl >= 0 else text
    except:
        print("dista: can't read DISTA_TOKEN_FILE", path)
        return ""


def send_auth(mut s: Stream) raises:
    """Sends this process's token on `s`, first, if it has one."""
    var t = cluster_token()
    if t:
        write_frame(s, KIND_AUTH, 0, 0, List[UInt8](t.as_bytes()))


def token_matches(given: Span[UInt8, _], want: String) -> Bool:
    """Whether `given` is `want`, in time that depends on `want`'s length
    only, not on where they first differ."""
    var w = want.as_bytes()
    var diff = UInt8(0) if len(given) == len(w) else UInt8(1)
    for i in range(len(w)):
        var g = given[i] if i < len(given) else UInt8(0)
        diff |= g ^ w[i]
    return diff == 0
