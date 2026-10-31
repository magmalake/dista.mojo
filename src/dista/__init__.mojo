"""dista: distributed tasks for Mojo.

A remote function is declared once as a task, with its argument and result
types:

```mojo
from dista import Task

def square(x: Int) -> Int:
    return x * x

comptime Square = Task[Int, Int, square]
```

See DESIGN.md for how it works.
"""

from dista.codec import Codec, decode, encode
from dista.task import Task, TaskLike
