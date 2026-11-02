"""Prometheus metrics: counters and a histogram, and the `/metrics` page.

With `DISTA_METRICS_PORT` set, the head and every worker answer HTTP on that
port with Prometheus's text format, which Kubernetes monitoring scrapes as it
is. It's off by default, since in local mode the head and its workers share
a host.

The server is the smallest one that does: the event loop polls its listener,
and each connection is answered on the spot. The request is read for up to
`READ_TIMEOUT_MS` and only its first line matters; every path gets the
metrics. A scrape is a few kilobytes, so answering it blocks the loop for
well under a millisecond, but a client that connects and sends nothing holds
it for the timeout.
"""

from std.ffi import c_int
from std.os import getenv

from dista.socket import Listener, Stream, poll_readable, recv_some

comptime READ_TIMEOUT_MS = 200

comptime BUCKETS = 6
"""Task duration buckets: 1 ms, 10 ms, 100 ms, 1 s, 10 s and 60 s."""


def bucket_bound(i: Int) -> Float64:
    """Bucket `i`'s upper bound, in seconds."""
    if i == BUCKETS - 1:
        return 60.0
    return 0.001 * Float64(10**i)


struct Histogram(Copyable, Movable):
    """A Prometheus histogram of durations."""

    var counts: List[Int]
    """Per bucket, how many observations were at most its bound."""
    var count: Int
    var sum: Float64

    def __init__(out self):
        self.counts = List[Int](length=BUCKETS, fill=0)
        self.count = 0
        self.sum = 0.0

    def observe(mut self, seconds: Float64):
        for i in range(BUCKETS):
            if seconds <= bucket_bound(i):
                self.counts[i] += 1
        self.count += 1
        self.sum += seconds

    def write(self, mut out: String, name: String, help: String):
        out += String("# HELP ", name, " ", help, "\n")
        out += String("# TYPE ", name, " histogram\n")
        for i in range(BUCKETS):
            out += String(
                name,
                '_bucket{le="',
                bucket_bound(i),
                '"} ',
                self.counts[i],
                "\n",
            )
        out += String(name, '_bucket{le="+Inf"} ', self.count, "\n")
        out += String(name, "_sum ", self.sum, "\n")
        out += String(name, "_count ", self.count, "\n")


def write_metric(
    mut out: String, kind: String, name: String, help: String, value: Int
):
    """One counter or gauge, without labels."""
    out += String("# HELP ", name, " ", help, "\n")
    out += String("# TYPE ", name, " ", kind, "\n")
    out += String(name, " ", value, "\n")


def write_outcomes(
    mut out: String, name: String, help: String, ok: Int, err: Int
):
    """A counter split by outcome, ok and error."""
    out += String("# HELP ", name, " ", help, "\n")
    out += String("# TYPE ", name, " counter\n")
    out += String(name, '{outcome="ok"} ', ok, "\n")
    out += String(name, '{outcome="error"} ', err, "\n")


def metrics_listener() raises -> Optional[Listener]:
    """A listener on `DISTA_METRICS_PORT`, on every interface, if it's set."""
    var port = getenv("DISTA_METRICS_PORT")
    if port == "":
        return None
    var l = Listener(Int(port), any_interface=True)
    print("dista metrics on port", l.port)
    return l^


def answer_scrape(l: Listener, body: String):
    """Accepts one connection on `l` and answers it with `body`. A client
    that misbehaves only loses its answer."""
    try:
        var s = l.accept()
        var buf = List[UInt8](length=4096, fill=0)
        var got = 0
        while got < len(buf):
            if not poll_readable([s.fd], READ_TIMEOUT_MS)[0]:
                break
            var n = recv_some(
                s.fd, buf.unsafe_ptr().unsafe_offset(got), len(buf) - got
            )
            if n <= 0:
                break
            got += n
            if _ends_headers(buf, got):
                break
        var head = String(
            "HTTP/1.1 200 OK\r\n",
            "Content-Type: text/plain; version=0.0.4\r\n",
            "Content-Length: ",
            body.byte_length(),
            "\r\nConnection: close\r\n\r\n",
        )
        s.write_all(head.as_bytes())
        s.write_all(body.as_bytes())
        s.close()
    except e:
        print("dista metrics: a scrape failed:", e)


def _ends_headers(buf: List[UInt8], n: Int) -> Bool:
    if n < 4:
        return False
    return (
        buf[n - 4] == 13
        and buf[n - 3] == 10
        and buf[n - 2] == 13
        and buf[n - 1] == 10
    )
