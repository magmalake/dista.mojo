## Distributed, median of passes (ms)

| query | dista ×3 | ray ×3 | dista ×6 | ray ×6 | dista ×12 | ray ×12 |
|---|---:|---:|---:|---:|---:|---:|
| zone_hours | 6,291 | 14,537 | 3,795 | 11,407 | 7,822 | 10,634 |
| total, median pass | **6,291** | **14,537** | **3,795** | **11,407** | **7,822** | **10,634** |
| total, slowest pass | 7,516 | 18,442 | 5,315 | 15,494 | 9,753 | 19,814 |
| answers match the reference | no reference | no reference | no reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 5.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 192 | 39.2 | 123.2 | 95 | 33 | c7g.4xlarge |
| ray | 48 | 19.5 | - | 652 | 217 | c7g.4xlarge |
| rust | 168 | 91.8 | 58.0 | 132 | 41 | c7g.4xlarge |

