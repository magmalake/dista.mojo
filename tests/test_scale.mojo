"""`dista.scale`: the autoscaling policy over tables of demand, and a job
simulated from a burst to idle."""

from std.testing import assert_equal, assert_true, TestSuite

from dista.scale import Demand, Policy, WorkerDemand, decide


def _workers(
    n: Int, idle: Float64 = 0, held: Int = 0, running: Int = 0
) -> List[WorkerDemand]:
    var out = List[WorkerDemand]()
    for i in range(n):
        out.append(
            WorkerDemand(
                String("10.0.0.", i + 1, ":7000"), running, idle, held, 0, False
            )
        )
    return out^


def test_from_zero_starts_at_once() raises:
    var d = Demand(tasks_ready=100, workers=_workers(0))
    var got = decide(d, Policy(min_workers=0, max_workers=6), now=10)
    assert_equal(got.want, 6)
    assert_equal(got.start, 4)  # max_step
    assert_equal(len(got.drain), 0)


def test_up_waits_up_after_and_counts_pending() raises:
    var policy = Policy(min_workers=0, max_workers=6)
    var d = Demand(tasks_ready=100, workers=_workers(0))
    # Four asked for, none joined: two more are wanted, after up_after.
    var got = decide(d, policy, now=10, pending=4)
    assert_equal(got.start, 0)
    assert_equal(got.short_since, 10)
    got = decide(d, policy, now=13, pending=4, short_since=got.short_since)
    assert_equal(got.start, 0)
    got = decide(d, policy, now=15, pending=4, short_since=got.short_since)
    assert_equal(got.start, 2)
    assert_equal(got.short_since, 0)
    # All six there: nothing.
    var full = Demand(tasks_ready=88, tasks_running=12, workers=_workers(6))
    got = decide(full, policy, now=20)
    assert_equal(got.start, 0)
    assert_equal(len(got.drain), 0)


def test_no_ready_tasks_no_start() raises:
    # Tasks waiting on inputs don't start workers: they can't run yet.
    var d = Demand(tasks_waiting=50, tasks_running=2, workers=_workers(1))
    var got = decide(d, Policy(min_workers=0, max_workers=6), now=100)
    assert_equal(got.start, 0)
    assert_equal(len(got.drain), 0)


def test_tasks_per_worker() raises:
    var d = Demand(tasks_ready=10, pipeline=2, workers=_workers(0))
    var got = decide(d, Policy(max_workers=20, tasks_per_worker=5), now=0)
    assert_equal(got.want, 2)
    got = decide(d, Policy(max_workers=20), now=0)
    assert_equal(got.want, 5)


def test_idle_drain_after_wait_fewest_bytes_first() raises:
    var policy = Policy(min_workers=1, max_workers=6)
    var w = _workers(4, idle=30)
    var d = Demand(workers=w.copy())
    assert_equal(len(decide(d, policy, now=0).drain), 0)
    w = _workers(4, idle=61)
    w[0].held_bytes = 500
    w[1].held_bytes = 0
    w[2].held_bytes = 100
    w[3].held_bytes = 900
    d = Demand(workers=w^)
    var got = decide(d, policy, now=0)
    assert_equal(len(got.drain), 3)
    assert_equal(got.drain[0], "10.0.0.2:7000")
    assert_equal(got.drain[1], "10.0.0.3:7000")
    assert_equal(got.drain[2], "10.0.0.1:7000")


def test_busy_workers_stay() raises:
    var w = _workers(3, idle=100)
    w[0].idle_seconds = 0
    w[0].running = 2
    var d = Demand(tasks_running=2, workers=w^)
    var got = decide(d, Policy(min_workers=0, max_workers=6), now=0)
    # One is wanted, for the running tasks; the two idle go.
    assert_equal(got.want, 1)
    assert_equal(len(got.drain), 2)
    for a in got.drain:
        assert_true(a != "10.0.0.1:7000")


def test_draining_workers_dont_count() raises:
    var w = _workers(3, idle=100)
    w[0].draining = True
    w[1].draining = True
    var d = Demand(workers=w^)
    var got = decide(d, Policy(min_workers=1, max_workers=6), now=0)
    assert_equal(len(got.drain), 0)


def test_actor_without_checkpoints_stays() raises:
    var w = _workers(2, idle=100)
    w[0].actor = 7
    var d = Demand(workers=w.copy())
    var got = decide(d, Policy(min_workers=0, max_workers=6), now=0)
    # The actor's worker is wanted, and couldn't move anyway; the other goes.
    assert_equal(len(got.drain), 1)
    assert_equal(got.drain[0], "10.0.0.2:7000")
    var d2 = Demand(checkpoints=True, workers=w^)
    got = decide(d2, Policy(min_workers=0, max_workers=6), now=0)
    assert_equal(len(got.drain), 1)


def test_down_to_zero() raises:
    var policy = Policy(min_workers=0, max_workers=6)
    var d = Demand(workers=_workers(1, idle=100))
    assert_equal(len(decide(d, policy, now=0).drain), 1)
    # Holding results the driver hasn't read: nowhere to move them.
    d = Demand(workers=_workers(1, idle=100, held=10))
    assert_equal(len(decide(d, policy, now=0).drain), 0)
    # With min_workers 1, never.
    d = Demand(workers=_workers(1, idle=100))
    assert_equal(len(decide(d, Policy(min_workers=1), now=0).drain), 0)
    # Two idle, one holding: the empty one goes, the holder stays.
    var w = _workers(2, idle=100)
    w[0].held_bytes = 10
    d = Demand(workers=w^)
    var got = decide(d, policy, now=0)
    assert_equal(len(got.drain), 1)
    assert_equal(got.drain[0], "10.0.0.2:7000")


def test_min_workers_started() raises:
    var d = Demand(workers=_workers(0))
    var got = decide(d, Policy(min_workers=2, max_workers=6), now=0)
    assert_equal(got.start, 2)


def test_parse_head_json() raises:
    var text = String(
        '{"tasks_ready":12,"tasks_waiting":3,"tasks_running":6,',
        '"actors_waiting":1,"tasks_done":40,"tasks_failed":0,"drivers":1,',
        '"pipeline":2,"checkpoints":true,"later":{"a":[1,"x",null]},',
        '"workers":[{"address":"127.0.0.1:5000","node":"n1",',
        '"registry":"0xab","running":2,"idle_seconds":0.0,',
        '"held_bytes":1024,"actor":0,"draining":false},',
        '{"address":"127.0.0.1:5001","node":"n1","registry":"0xab",',
        '"running":0,"idle_seconds":12.5,"held_bytes":0,"actor":9,',
        '"draining":true}]}\n',
    )
    var d = Demand.parse(text)
    assert_equal(d.tasks_ready, 12)
    assert_equal(d.tasks_waiting, 3)
    assert_equal(d.tasks_running, 6)
    assert_equal(d.actors_waiting, 1)
    assert_true(d.checkpoints)
    assert_equal(len(d.workers), 2)
    assert_equal(d.workers[0].address, "127.0.0.1:5000")
    assert_equal(d.workers[0].held_bytes, 1024)
    assert_equal(d.workers[1].idle_seconds, 12.5)
    assert_equal(Int(d.workers[1].actor), 9)
    assert_true(d.workers[1].draining)
    assert_equal(d.live(), 1)
    var empty = Demand.parse('{"workers":[]}')
    assert_equal(len(empty.workers), 0)


def test_simulated_burst_grows_and_shrinks() raises:
    """A job of 200 tasks of 2 s each, a tick a second: workers join 3 s
    after they're started and each runs 2 tasks at once. It must reach
    max_workers in steps of max_step, finish, and drain back to
    min_workers, never outside the range."""
    var policy = Policy(
        min_workers=0, max_workers=6, up_after=5, idle_before_down=10
    )
    var queued = 200
    var joining = List[Int]()  # ticks left until each started worker joins
    var w = List[WorkerDemand]()
    var busy_until = List[List[Int]]()  # per worker, its tasks' end ticks
    var short_since = Float64(0)
    var most = 0
    var biggest_step = 0
    var next_id = 0
    var done = 0
    var ended_at = -1
    for t in range(400):
        # Joins and finished tasks.
        var still = List[Int]()
        for j in joining:
            if j <= 1:
                w.append(WorkerDemand(String("w", next_id), 0, 0, 0, 0, False))
                busy_until.append(List[Int]())
                next_id += 1
            else:
                still.append(j - 1)
        joining = still^
        for i in range(len(w)):
            var left = List[Int]()
            for e in busy_until[i]:
                if e > t:
                    left.append(e)
                else:
                    done += 1
            busy_until[i] = left^
        # The head hands out tasks.
        var running = 0
        for i in range(len(w)):
            while queued > 0 and len(busy_until[i]) < 2:
                busy_until[i].append(t + 2)
                queued -= 1
            w[i].running = len(busy_until[i])
            running += w[i].running
            if w[i].running > 0:
                w[i].idle_seconds = 0
            else:
                w[i].idle_seconds += 1
        var d = Demand(
            tasks_ready=queued, tasks_running=running, workers=w.copy()
        )
        var got = decide(d, policy, Float64(t), len(joining), short_since)
        short_since = got.short_since
        assert_true(got.start <= policy.max_step)
        biggest_step = max(biggest_step, got.start)
        for _ in range(got.start):
            joining.append(3)
        # Drained workers leave at once: they're idle.
        for a in got.drain:
            for i in range(len(w)):
                if w[i].address == a:
                    assert_equal(w[i].running, 0)
                    _ = w.pop(i)
                    _ = busy_until.pop(i)
                    break
        most = max(most, len(w) + len(joining))
        assert_true(len(w) + len(joining) <= policy.max_workers)
        if done == 200 and ended_at < 0:
            ended_at = t
        if ended_at >= 0 and len(w) == 0:
            break
    assert_equal(done, 200)
    assert_equal(most, 6)
    assert_equal(biggest_step, 4)
    assert_equal(len(w), 0)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
