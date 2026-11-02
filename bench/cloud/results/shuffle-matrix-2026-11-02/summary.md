## Distributed, median of passes (ms)

| query | dista ×3 | ray ×3 | dista ×6 | ray ×6 | dista ×12 | ray ×12 |
|---|---:|---:|---:|---:|---:|---:|
| zone_hours | 8,462 | 14,169 | 5,139 | 10,955 | 7,282 | 11,439 |
| total, median pass | **8,462** | **14,169** | **5,139** | **10,955** | **7,282** | **11,439** |
| total, slowest pass | 9,321 | 18,228 | 6,299 | 14,535 | 8,754 | 18,399 |
| answers match the reference | no reference | no reference | no reference | no reference | no reference | no reference |

Slots per node: 16. Passes: 5.

## Building the images

| image | whole build (s) | dependencies (s) | compile (s) | unpacked (MB) | gzip (MB) | built on |
|---|---:|---:|---:|---:|---:|---|
| dista | 191 | 35.6 | 123.7 | 95 | 33 | c7g.4xlarge |
| ray | 47 | 19.0 | - | 652 | 217 | c7g.4xlarge |
| rust | 169 | 92.8 | 58.6 | 132 | 41 | c7g.4xlarge |

