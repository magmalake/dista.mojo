## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 28,022 | 34,318 | 17,537 |
| q2_month_range | 391 | 834 | 533 |
| q3_payment_sum | 38,329 | 39,690 | 29,969 |
| q4_tip_ratio | 50,365 | 42,206 | 44,095 |
| q5_top_zones | 41,525 | 46,057 | 31,709 |
| q6_zone_revenue | 1,718 | 1,696 | 1,945 |
| q7_wide | 1,132 | 1,097 | 1,211 |
| q8_selective | 50,405 | 42,443 | 57,328 |
| total | **211,887** | **208,341** | **184,327** |

All answers agree with the one-process references (dista, rust, ray).
