# Notes for coding agents

## Writing fast data code

dista runs data jobs whose tasks read Iceberg tables and transform their rows. Before writing or speeding up code that loops over record batches,
or that filters, groups or aggregates rows, use the
[`mojo-data-performance`](https://github.com/magmalake/skills/tree/main/mojo-data-performance)
skill. It explains how to time the decoding apart from the work, without a
second implementation to compare against, and gives the loop and pipeline
rules magmalake has measured. Install it with Modular's `mojo-syntax`,
which keeps the Mojo current:

```text
/plugin marketplace add magmalake/skills     # Claude Code
/plugin install skills@magmalake
npx skills add magmalake/skills              # other agents
npx skills add modular/skills --skill mojo-syntax
```
