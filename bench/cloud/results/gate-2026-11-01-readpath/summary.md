## Distributed, median of passes (ms)

| query | dista ×3 | rust ×3 | ray ×3 |
|---|---:|---:|---:|
| q1_scan_count | 2,439 | 6,630 | 6,822 |
| q2_month_range | 220 | 617 | 748 |
| q3_payment_sum | 1,723 | 5,269 | 6,274 |
| q4_tip_ratio | 2,072 | 5,014 | 7,684 |
| q5_top_zones | 1,889 | 4,426 | 6,344 |
| q6_zone_revenue | 251 | 616 | 896 |
| q7_wide | 1,122 | 1,150 | 1,281 |
| q8_selective | 2,293 | 5,022 | 8,520 |
| total, median pass | **12,008** | **28,744** | **38,570** |
| total, slowest pass | 12,839 | 32,253 | 40,978 |
| answers match the reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 2.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 122 | 35.1 | 54.0 | 93 | 32 | c7g.4xlarge |
| ray | 44 | 17.5 | - | 607 | 204 | c7g.4xlarge |
| rust | 170 | 93.4 | 57.9 | 132 | 41 | c7g.4xlarge |

