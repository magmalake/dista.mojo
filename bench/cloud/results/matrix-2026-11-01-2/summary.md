## Distributed, median of passes (ms)

| query | dista ×12 | rust ×12 | ray ×12 |
|---|---:|---:|---:|
| q1_scan_count | 520 | 634 | 1,253 |
| q2_month_range | 114 | 435 | 421 |
| q3_payment_sum | 497 | 980 | 1,440 |
| q4_tip_ratio | 620 | 987 | 1,608 |
| q5_top_zones | 597 | 994 | 1,511 |
| q6_zone_revenue | 181 | 473 | 496 |
| q7_wide | 954 | 1,070 | 1,173 |
| q8_selective | 718 | 1,057 | 1,906 |
| total, median pass | **4,446** | **6,745** | **9,955** |
| total, slowest pass | 5,164 | 8,911 | 17,976 |
| answers match the reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 5.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 124 | 35.1 | 58.7 | 93 | 32 | c7g.4xlarge |
| ray | 46 | 18.8 | - | 607 | 204 | c7g.4xlarge |
| rust | 185 | 102.5 | 64.1 | 132 | 41 | c7g.4xlarge |

