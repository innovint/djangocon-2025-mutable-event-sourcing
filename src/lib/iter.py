from typing import Any
from typing import Generator
from typing import Iterable


def chunk[T](generator: Generator[T, Any, Any], size: int = 1000) -> Generator[Iterable[T], Any, Any]:
    lst = []
    for item in generator:
        lst.append(item)
        if len(lst) == size:
            yield lst
            lst = []
    if lst:  # Don't forget the last partial chunk
        yield lst
