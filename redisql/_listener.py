import logging
import threading

log = logging.getLogger(__name__)


class Listener:
    """
    Call on_change() each time wait() returns, on a background thread.

    wait() blocks until something may have changed or a timeout passes, so on_change() also
    runs periodically. If either raises, on_error(exc) is called and the loop retries after
    `retry` seconds.
    """

    def __init__(self, wait, on_change, on_error, retry=1):
        self._wait, self._on_change, self._on_error, self._retry = wait, on_change, on_error, retry
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self, timeout):
        self._stop.set()
        self._thread.join(timeout)

    @property
    def alive(self):
        return self._thread.is_alive()

    def _run(self):
        while not self._stop.is_set():
            try:
                self._wait()
                self._on_change()
            except Exception as e:
                log.exception("redisql listener error, retrying")
                self._on_error(e)
                self._stop.wait(self._retry)
