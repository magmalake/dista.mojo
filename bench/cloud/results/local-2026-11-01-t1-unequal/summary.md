## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 28,695 | 204,843 | 159,595 |
| q2_month_range | 416 | 2,676 | 2,305 |
| q3_payment_sum | 39,680 | 279,566 | 233,990 |
| q4_tip_ratio | 50,445 | 278,104 | 312,178 |
| q5_top_zones | 52,376 | 296,348 | 235,335 |
| q6_zone_revenue | 2,145 | 9,712 | 10,628 |
| q7_wide | 1,126 | 1,191 | 1,433 |
| q8_selective | 51,878 | 311,926 | 350,230 |
| total | **226,761** | **1,384,366** | **1,305,694** |

All answers agree with the one-process references (dista, rust, ray).
