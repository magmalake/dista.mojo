# dista

A distributed task framework for Mojo: mark a function remote, call it, and the work runs across a cluster. Every node runs the same compiled binary, and data moves between tasks as Arrow.

```mojo
from dista import ObjectRef, Runtime, Task

def square(x: Int) -> Int:
    return x * x

comptime Square = Task[Int, Int, square]

def main() raises:
    var rt = Runtime[Square](workers=4)
    var refs = List[ObjectRef[Int]]()
    for i in range(1000):
        refs.append(rt.remote[Square](i))
    var total = 0
    for r in refs:
        total += rt.get(r)
    print(total)  # 332833500
```

**Status: phase 1.** Tasks run on forked worker processes on one machine, over loopback TCP. Arguments and results can be scalars, `String`, any plain struct of those, or a type that implements `Codec`. A task that raises re-raises in `get`. 1,000 small tasks across 4 workers take about 11 ms. Multi-machine clusters, Arrow results and actors come in later phases.

- [VISION.md](VISION.md): what dista is for, its principles and phases.
- [DESIGN.md](DESIGN.md): how it works, and what's verified on Mojo 1.2.0.

```
pixi run test               # codec and runtime tests, Mojo 1.2.0
pixi run -e nightly test    # the same on the 1.3 nightly
pixi run -e fmt fmt         # mojo format
```

Part of [magmalake](https://github.com/magmalake). Apache-2.0.
