## Distributed, median of passes (ms)

| query | dista ×3 | rust ×3 | ray ×3 | dista ×6 | rust ×6 | ray ×6 | dista ×12 |
|---|---:|---:|---:|---:|---:|---:|---:|
| q1_scan_count | 8,204 | 3,632 | 3,904 | 5,676 | 1,268 | 1,925 |  |
| q2_month_range | 392 | 438 | 583 | 381 | 433 | 559 |  |
| q3_payment_sum | 9,399 | 4,563 | 5,020 | 6,263 | 1,686 | 2,382 |  |
| q4_tip_ratio | 11,240 | 4,287 | 6,268 | 7,216 | 1,821 | 3,092 |  |
| q5_top_zones | 9,844 | 3,585 | 5,127 | 6,627 | 1,756 | 2,577 |  |
| q6_zone_revenue | 812 | 478 | 753 | 757 | 447 | 745 |  |
| q7_wide | 1,071 | 1,049 | 1,271 | 1,075 | 1,055 | 1,259 |  |
| q8_selective | 12,143 | 4,043 | 7,150 | 7,583 | 1,896 | 3,614 |  |
| total, median pass | **53,548** | **21,966** | **30,140** | **35,613** | **10,264** | **16,140** |  |
| total, slowest pass | 57,693 | 29,131 | 35,052 | 36,254 | 15,704 | 22,997 |  |
| answers match the reference | yes | yes | yes | yes | yes | yes | **failed** |

Slots per node: 16. Passes: 5.

## One process, on the head node (ms)

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
| total | **1,633,042** | **1,409,471** | **1,339,870** |

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| ray | 45 | 17.7 | - | 607 | 204 | c7g.4xlarge |
| rust | 167 | 91.7 | 58.1 | 132 | 41 | c7g.4xlarge |
| dista | 115 | 35.9 | 48.6 | 92 | 32 | c7g.4xlarge |

All answers agree with the one-process references (dista, rust, ray).
