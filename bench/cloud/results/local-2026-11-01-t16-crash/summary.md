## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 8,906 | 33,917 | 9,590 |
| q2_month_range | 351 | 794 | 525 |
| q3_payment_sum |  | 38,507 | 15,699 |
| q4_tip_ratio |  | 39,289 | 22,992 |
| q5_top_zones |  | 43,557 | 17,558 |
| q6_zone_revenue |  | 1,405 | 1,264 |
| q7_wide |  | 1,096 | 715 |
| q8_selective |  | 35,736 | 28,973 |
| total | **9,257** | **194,301** | **97,316** |

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 129 | 37.3 | 54.2 | 93 | 32 | c7g.4xlarge |
| ray | 45 | 17.8 | - | 607 | 204 | c7g.4xlarge |
| rust | 171 | 94.2 | 58.7 | 132 | 41 | c7g.4xlarge |

## Problems

- dista's reference run didn't pass
