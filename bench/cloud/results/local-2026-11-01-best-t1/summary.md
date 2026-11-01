## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 29,197 | 34,419 | 17,651 |
| q2_month_range | 424 | 880 | 543 |
| q3_payment_sum | 40,182 | 40,171 | 30,402 |
| q4_tip_ratio | 50,980 | 42,438 | 44,675 |
| q5_top_zones | 53,475 | 45,878 | 32,555 |
| q6_zone_revenue | 2,118 | 1,646 | 1,959 |
| q7_wide | 1,148 | 1,119 | 1,191 |
| q8_selective | 52,705 | 43,286 | 59,019 |
| total | **230,229** | **209,837** | **187,995** |

All answers agree with the one-process references (dista, rust, ray).
