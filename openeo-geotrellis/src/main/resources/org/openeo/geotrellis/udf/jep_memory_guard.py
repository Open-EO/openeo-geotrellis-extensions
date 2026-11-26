"""
Soft memory limit for UDFs that run in the (shared) JEP interpreter of a Spark executor.

`resource.setrlimit` can't be used: JEP runs inside the executor JVM, so it would limit the whole JVM.
Instead, Python and numpy allocations are traced with `tracemalloc`, and a watchdog thread raises
`UdfMemoryLimitExceeded` (a `MemoryError`) in the UDF threads when the traced memory exceeds the limit.
All UDFs of the executor share the interpreter, so the limit applies to their combined memory usage,
like `spark.executor.pyspark.memory` does for regular PySpark workers.

The exception is raised asynchronously: in Python code at the next bytecode boundary,
not during a single long-running native call.
"""
import ctypes
import gc
import sys
import threading
import time
import tracemalloc

CHECK_INTERVAL_SECONDS = 0.1


class UdfMemoryLimitExceeded(MemoryError):
    """The UDFs on this executor use more memory than allowed by spark.executor.pyspark.memory."""


_lock = threading.Lock()
_active_threads = {}
_signalled_threads = set()
_limit_bytes = 0
_watchdog = None


def _set_async_exc(ident, exc):
    # Passing None (NULL) clears a pending asynchronous exception.
    return ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_ulong(ident), ctypes.py_object(exc) if exc else None)


def _check():
    if _limit_bytes <= 0 or not tracemalloc.is_tracing():
        return
    current, _ = tracemalloc.get_traced_memory()
    if current <= _limit_bytes:
        with _lock:
            _signalled_threads.clear()
        return
    gc.collect()
    current, _ = tracemalloc.get_traced_memory()
    if current <= _limit_bytes:
        return
    with _lock:
        to_signal = [ident for ident in _active_threads if ident not in _signalled_threads]
        for ident in to_signal:
            _signalled_threads.add(ident)
            _set_async_exc(ident, UdfMemoryLimitExceeded)
    if to_signal:
        print(
            f"UDF memory usage ({current} bytes) exceeds the limit of {_limit_bytes} bytes "
            f"(spark.executor.pyspark.memory), interrupting {len(to_signal)} UDF(s).",
            file=sys.stderr,
        )


def _watch():
    while True:
        time.sleep(CHECK_INTERVAL_SECONDS)
        try:
            _check()
        except Exception as e:
            print(f"UDF memory watchdog failed: {e!r}", file=sys.stderr)


def enter(limit_bytes):
    """Registers the current thread as running a UDF, limited to `limit_bytes` (<= 0: no limit)."""
    global _limit_bytes, _watchdog
    if limit_bytes <= 0:
        return
    with _lock:
        _limit_bytes = limit_bytes
        if not tracemalloc.is_tracing():
            tracemalloc.start()
        if _watchdog is None:
            _watchdog = threading.Thread(target=_watch, name="udf-memory-watchdog", daemon=True)
            _watchdog.start()
        ident = threading.get_ident()
        _active_threads[ident] = _active_threads.get(ident, 0) + 1


def leave():
    """Unregisters the current thread and clears a pending UdfMemoryLimitExceeded, if any."""
    ident = threading.get_ident()
    with _lock:
        count = _active_threads.get(ident, 0) - 1
        if count > 0:
            _active_threads[ident] = count
        else:
            _active_threads.pop(ident, None)
            _signalled_threads.discard(ident)
            _set_async_exc(ident, None)
