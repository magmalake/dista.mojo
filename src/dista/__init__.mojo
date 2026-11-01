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

An actor keeps state between calls on a worker of its own:

```mojo
comptime CounterActor = Actor[Counter, Int, new_counter]
comptime Add = Method[CounterActor, Int, Int, add]

var rt = Runtime[CounterActor, Add](workers=2)
var c = rt.create[CounterActor](0)
var total = rt.call[Add](c, 5)  # an ObjectRef[Int]
rt.kill(c)
```

See DESIGN.md for how it works.
"""

from dista.codec import Codec, decode, encode
from dista.client import (
    ActorHandle,
    Client,
    CtxMethod,
    CtxMethodLike,
    CtxTask,
    CtxTaskLike,
    HeadStats,
    ObjectRef,
    TaskContext,
    TaskSpan,
)
from dista.runtime import Runtime
from dista.seq import Seq
from dista.task import Actor, ActorLike, Method, MethodLike, Task, TaskLike
