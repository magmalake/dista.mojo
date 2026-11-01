## Distributed, median of passes (ms)

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
| total, median pass | **11,040** | **18,377** | **21,275** | **7,184** | **10,267** | **12,737** | **5,941** | **6,682** | **15,703** |
| total, slowest pass | 12,422 | 21,538 | 26,047 | 7,647 | 12,318 | 20,039 | 6,313 | 8,903 | 23,161 |
| answers match the reference | no reference | no reference | no reference | no reference | no reference | no reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 5.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 120 | 35.4 | 53.7 | 93 | 32 | c7g.4xlarge |
| ray | 44 | 17.8 | - | 607 | 204 | c7g.4xlarge |
| rust | 168 | 92.3 | 58.1 | 132 | 41 | c7g.4xlarge |

