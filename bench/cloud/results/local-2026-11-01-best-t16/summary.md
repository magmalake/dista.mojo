## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 9,095 | 34,582 | 9,125 |
| q2_month_range | 331 | 785 | 459 |
| q3_payment_sum | 10,309 | 38,275 | 14,959 |
| q4_tip_ratio | 11,138 | 39,172 | 22,783 |
| q5_top_zones | 26,788 | 44,131 | 16,699 |
| q6_zone_revenue | 1,067 | 1,324 | 1,236 |
| q7_wide | 880 | 1,114 | 649 |
| q8_selective | 12,048 | 35,681 | 29,609 |
| total | **71,656** | **195,064** | **95,519** |

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 124 | 35.3 | 54.0 | 93 | 32 | c7g.4xlarge |
| ray | 45 | 17.6 | - | 607 | 204 | c7g.4xlarge |
| rust | 172 | 93.7 | 58.9 | 132 | 41 | c7g.4xlarge |

All answers agree with the one-process references (dista, rust, ray).
