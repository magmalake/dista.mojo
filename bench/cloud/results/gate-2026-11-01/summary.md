## Distributed, median of passes (ms)

| query | dista ×3 | rust ×3 | ray ×3 |
|---|---:|---:|---:|
| q1_scan_count | 10,427 | 6,211 | 6,868 |
| q2_month_range | 498 | 605 | 672 |
| q3_payment_sum | 9,628 | 4,818 | 5,868 |
| q4_tip_ratio | 11,292 | 4,418 | 7,546 |
| q5_top_zones | 10,578 | 3,794 | 6,170 |
| q6_zone_revenue | 912 | 550 | 984 |
| q7_wide | 1,216 | 1,122 | 1,316 |
| q8_selective | 12,139 | 4,222 | 8,101 |
| total, median pass | **56,690** | **25,740** | **37,528** |
| total, slowest pass | 59,417 | 29,173 | 40,244 |
| answers match the reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 2.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 116 | 35.6 | 50.6 | 92 | 32 | c7g.4xlarge |
| ray | 44 | 17.3 | - | 607 | 204 | c7g.4xlarge |
| rust | 168 | 92.3 | 57.4 | 132 | 41 | c7g.4xlarge |

