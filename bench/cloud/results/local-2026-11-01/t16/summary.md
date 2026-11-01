## One process, on the head node (ms)

| query | dista | rust | ray |
|---|---:|---:|---:|
| q1_scan_count | 7,828 |  | 9,192 |
| q2_month_range | 315 |  | 446 |
| q3_payment_sum | 9,229 |  | 14,937 |
| q4_tip_ratio | 10,663 |  | 22,689 |
| q5_top_zones | 15,309 |  | 16,625 |
| q6_zone_revenue | 747 |  | 1,233 |
| q7_wide | 888 |  | 706 |
| q8_selective | 11,401 |  | 28,676 |
| total | **56,380** | **0** | **94,504** |

## Problems

- rust's reference run didn't pass
