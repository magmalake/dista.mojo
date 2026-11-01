## Distributed, median of passes (ms)

| query | dista ×12 | rust ×12 | ray ×12 |
|---|---:|---:|---:|
| q1_scan_count | 611 | 666 | 1,272 |
| q2_month_range | 93 | 410 | 398 |
| q3_payment_sum | 509 | 974 | 1,420 |
| q4_tip_ratio | 696 | 1,005 | 1,757 |
| q5_top_zones | 540 | 967 | 1,490 |
| q6_zone_revenue | 111 | 472 | 496 |
| q7_wide | 316 | 1,054 | 1,172 |
| q8_selective | 675 | 1,119 | 6,928 |
| total, median pass | **3,751** | **6,691** | **15,395** |
| total, slowest pass | 8,721 | 9,203 | 29,624 |
| answers match the reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 5.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 122 | 35.8 | 54.2 | 93 | 32 | c7g.4xlarge |
| ray | 45 | 18.0 | - | 607 | 204 | c7g.4xlarge |
| rust | 168 | 91.7 | 58.1 | 132 | 41 | c7g.4xlarge |

