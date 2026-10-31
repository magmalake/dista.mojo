"""dista: distributed tasks for Mojo.

Declare a remote function as a task, start a runtime with it registered, and
call it:

```mojo
from dista import Runtime, Task

def square(x: Int) -> Int:
    return x * x

comptime Square = Task[Int, Int, square]

def main() raises:
    var rt = Runtime[Square](workers=4)
    var r = rt.remote[Square](7)
    print(rt.get(r))  # 49
```

See DESIGN.md for how it works.
"""

from dista.codec import Codec, decode, encode
from dista.runtime import ObjectRef, Runtime
from dista.task import Task, TaskLike
