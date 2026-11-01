# Numbers for the write-up

Every measured number from the AWS runs, in one place, with the folder each
came from. All runs: us-east-2, c7g.4xlarge (16 Graviton3 vCPUs, 32 GiB),
k3s, 16 task slots per node. The table: NYC yellow taxi 2011–2024 as Iceberg
in S3, 1,438,321,685 rows in 416 Parquet files (19.5 GB), written by 168
appends, so 168 manifests. Eight taxibench queries per pass; "median pass" is
the median over passes of the eight queries' total.

Answers are checked: every run prints `ANSWER` lines, and row counts must
match exactly across dista, Rust and Ray, sums to floating-point rounding.

## 1. Distributed, 3 nodes: before and after the read path

Median pass, ms. Before: `gate-2026-11-01/` (dista#37). After:
`gate-2026-11-01-readpath/` (dista#41), the same gate on the same day.

| query | dista before | dista after | rust | ray |
|---|---:|---:|---:|---:|
| q1_scan_count | 10,427 | 2,439 | 6,630 | 6,822 |
| q2_month_range | 498 | 220 | 617 | 748 |
| q3_payment_sum | 9,628 | 1,723 | 5,269 | 6,274 |
| q4_tip_ratio | 11,292 | 2,072 | 5,014 | 7,684 |
| q5_top_zones | 10,578 | 1,889 | 4,426 | 6,344 |
| q6_zone_revenue | 912 | 251 | 616 | 896 |
| q7_wide | 1,216 | 1,122 | 1,150 | 1,281 |
| q8_selective | 12,139 | 2,293 | 5,022 | 8,520 |
| **total, median pass** | **56,690** | **12,008** | **28,744** | **38,570** |

Rust and Ray in the "after" run; in the "before" run they were 25,740 and
37,528 ms, so run-to-run variation is about ±10%.

Answers, after (dista vs Rust): every count identical, e.g. q1 1,426,837,047
rows; q3 sum 7,104,909,931.26 vs 7,104,909,931.20.

## 2. Where dista's time went (task timeline)

From `dista-3-timeline.json.gz` in each gate folder (Chrome trace, opens in
Perfetto), dista's head and worker stamps plus the driver's own spans (#40).

| | before | after |
|---|---:|---:|
| a file task (`fold_file`), median | 691 ms | 177 ms |
| a file task, p90 | – | 243 ms |
| planning one query (driver) | ~4,000 ms | 295 ms |
| submitting one query's 416 tasks | 800–1,100 ms | 3 ms |
| waiting for a free worker, median | 1,661 ms | – |

What the read path changed, per file task:

| requests, one-column read | before | after |
|---|---:|---:|
| table metadata (133 KB, 168 snapshots) | 1 | 0 (sent with the task) |
| footer | 2 (trailer, then footer) | 1 (512 KiB tail) |
| column chunks, 4 row groups | 4 in a row | 4 at once |

Decoding the same file from local disk: 12 ms (1 column) to 40 ms (4
columns), so before, about 95% of a file task was waiting on S3.

Planning, measured from a laptop over the internet (so latency is higher than
in-region): 17.9 s with the 168 manifests read one by one, 1.7 s read at once.
Four row groups of one column with 30 ms of latency added per request on
loopback: 145 ms one by one, 43 ms at once.

## 2b. Distributed, 3, 6 and 12 nodes, after the read path

`matrix-2026-11-01/`, 5 passes, the first run of `bench matrix`. Every answer
identical across the nine runs (three frameworks × three node counts).

| query | dista ×3 | rust ×3 | ray ×3 | dista ×6 | rust ×6 | ray ×6 | dista ×12 | rust ×12 | ray ×12 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| q1_scan_count | 1,981 | 2,306 | 4,128 | 804 | 1,116 | 1,751 | 817 | 620 | 1,330 |
| q2_month_range | 157 | 429 | 409 | 165 | 435 | 405 | 161 | 411 | 421 |
| q3_payment_sum | 1,599 | 3,347 | 4,168 | 1,016 | 1,678 | 1,810 | 832 | 914 | 1,395 |
| q4_tip_ratio | 1,980 | 3,538 | 3,528 | 1,279 | 1,826 | 2,083 | 933 | 1,080 | 1,584 |
| q5_top_zones | 1,889 | 3,481 | 3,483 | 1,184 | 1,761 | 2,072 | 904 | 1,007 | 1,507 |
| q6_zone_revenue | 247 | 468 | 493 | 244 | 456 | 481 | 243 | 479 | 496 |
| q7_wide | 1,001 | 1,074 | 1,219 | 1,012 | 1,069 | 1,172 | 1,000 | 1,062 | 1,174 |
| q8_selective | 2,169 | 3,756 | 3,603 | 1,418 | 1,887 | 2,297 | 1,060 | 1,071 | 1,907 |
| **total, median pass** | **11,040** | **18,377** | **21,275** | **7,184** | **10,267** | **12,737** | **5,941** | **6,682** | **15,703** |

- Rust's 3-node median (18.4 s) is faster than in the gate (28.7 s): its
  driver now plans with iceberg-rust's concurrency limit at 8, not 1
  (`TAXI_FILES_AT_ONCE`, dista#43), as dista fetches manifests at once.
- dista scales 11.0 → 7.2 → 5.9 s. At 12 nodes a file task's median is 155 ms
  (p90 249 ms), and planning each query takes 0.31 s on the driver: about
  2.5 s of the 5.9 s pass. iceberg-rust caches manifests between plans; dista
  reads them again for every query. That is the next limit.
- Ray is slower on 12 nodes (15.7 s) than on 6 (12.7 s).

## 2c. 12 nodes with the manifest cache

`matrix-2026-11-01-2/`, 5 passes, after the driver kept one `ManifestCache`
across its queries (iceberg#58, dista#46). Planning a query: 18 ms median
(the first one, which reads the 168 manifests, 351 ms), against 310 ms
before. Counts identical across the three frameworks.

| total, median pass (ms) | dista | rust | ray |
|---|---:|---:|---:|
| 12 nodes, before (`matrix-2026-11-01`) | 5,941 | 6,682 | 15,703 |
| **12 nodes, with the cache** | **4,446** | **6,745** | **9,955** |

Nothing changed for Ray between the two runs; its 12-node result moved by
37%, so Ray's 12-node numbers are noisy.

## 3. Distributed, 3 and 6 nodes, before the read path

`2026-11/`, 5 passes. The 12-node dista run failed its answer check, fixed
since (#29); not rerun yet.

| total, median pass (ms) | dista | rust | ray |
|---|---:|---:|---:|
| 3 nodes | 53,548 | 21,966 | 30,140 |
| 6 nodes | 35,613 | 10,264 | 16,140 |

## 4. One process, on the head node

Before the read path, one thread each (`2026-11/`):

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 218,843 | 218,199 | 173,036 |
| q2_month_range | 1,792 | 2,489 | 2,452 |
| q3_payment_sum | 288,083 | 282,516 | 241,964 |
| q4_tip_ratio | 375,410 | 287,253 | 307,729 |
| q5_top_zones | 311,999 | 299,812 | 241,986 |
| q6_zone_revenue | 9,726 | 9,880 | 10,581 |
| q7_wide | 1,355 | 1,147 | 1,236 |
| q8_selective | 425,834 | 308,175 | 360,886 |
| **total** | **1,633,042** | **1,409,471** | **1,339,870** |

### 4a. After the read path, 16 threads

`local-2026-11-01-best-t16/`. Every framework on the head node's 16 cores:
dista's `num_workers = 16`, Rust's multi-threaded tokio runtime, pyarrow's
default thread pools. Answers identical across the three.

| query | dista | rust | ray (PyIceberg + pyarrow) |
|---|---:|---:|---:|
| q1_scan_count | 9,095 | 34,582 | 9,125 |
| q2_month_range | 331 | 785 | 459 |
| q3_payment_sum | 10,309 | 38,275 | 14,959 |
| q4_tip_ratio | 11,138 | 39,172 | 22,783 |
| q5_top_zones | 26,788 | 44,131 | 16,699 |
| q6_zone_revenue | 1,067 | 1,324 | 1,236 |
| q7_wide | 880 | 1,114 | 649 |
| q8_selective | 12,048 | 35,681 | 29,609 |
| **total** | **71,656** | **195,064** | **95,519** |

### 4b. After the read path, one thread with the best settings

`local-2026-11-01-best-t1/`. One decoding thread each, with up to 8 files'
reads in flight: dista `files_at_once = 8`; Rust's current-thread runtime
with iceberg-rust's concurrency limits at 8; pyarrow with 1 CPU thread and
8 I/O threads. Answers identical across the three.

| query | dista | rust | ray (PyIceberg + pyarrow) |
|---|---:|---:|---:|
| q1_scan_count | 29,197 | 34,419 | 17,651 |
| q2_month_range | 424 | 880 | 543 |
| q3_payment_sum | 40,182 | 40,171 | 30,402 |
| q4_tip_ratio | 50,980 | 42,438 | 44,675 |
| q5_top_zones | 53,475 | 45,878 | 32,555 |
| q6_zone_revenue | 2,118 | 1,646 | 1,959 |
| q7_wide | 1,148 | 1,119 | 1,191 |
| q8_selective | 52,705 | 43,286 | 59,019 |
| **total** | **230,229** | **209,837** | **187,995** |

For comparison, one thread reading one file at a time
(`local-2026-11-01-t1-unequal/`, Rust and Ray only): Rust 1,384,366 ms, Ray
1,305,694 ms. Eight reads in flight is worth 6.6× to Rust and 6.9× to Ray.

Open questions these raise, for the next round:
- dista's q5 on 16 threads (26.8 s) is about 2.5× its q1, q3 and q4; on one
  thread q5 is in line with them. Something in its zone fold doesn't scale.
- On one thread dista is the slowest of the three (230 s against 210 and
  188), where its distributed runs lead: the single-thread decode is the next
  thing to profile.
- Rust on 16 threads (195 s) is barely faster than on one (210 s).

The first one-process session's 16-thread dista run crashed in curl
(`local-2026-11-01-t16-crash/`, fixed in objectstore#15).

## 5. Building the images

On the head node, cold (`build.tsv` in each folder):

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) |
|---|---:|---:|---:|---:|---:|
| dista | 116–122 | 35 | 51–54 | 92–93 | 32 |
| rust | 168–170 | 92–93 | 57–58 | 132 | 41 |
| ray | 44 | 17 | – | 607 | 204 |

dista's image is distroless with no libpq or sqlite (#27).

## 6. The runtime itself

- Head throughput (`bench/fanout.mojo`, local, forked workers): about 160,000
  empty tasks a second, flat from 4 to 192 workers (#33).
- Actors (#39), local: 1,000 ordered calls to one counter actor in about
  22 ms.
- Cost of the AWS gate: a head and 3 workers for about 13 minutes, about
  $1.50.
