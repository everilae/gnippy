# -*- coding: utf-8 -*-

import threading
import requests
from gnippy import config
from contextlib import closing


class PowerTrackClient():
    """
        PowerTrackClient allows you to connect to the GNIP
        power track stream and fetch data.
    """

    def __init__(self, callback, **kwargs):
        """
        :param callback: On data callback for :class:`Worker`
        :param url: stream url
        :param auth: stream authentication, ``(account, password)`` tuple
        """
        c = config.resolve(kwargs)

        self.callback = callback
        self.url = c['url']
        self.auth = c['auth']
        self.worker = None

    def connect(self):
        """
        Create a :class:`Worker` daemon and start consuming :attr:`url`.

        :raises RuntimeError: if called more than once per client.
        """
        if self.worker:
            raise RuntimeError(
                "Cannot connect: PowerTrackClient is not re-entrant")

        self.worker = Worker(self.url, self.auth, self.callback)
        self.worker.daemon = True
        self.worker.start()

    def wait(self, timeout=None):
        """
        Wait on :attr:`worker` for ``timeout`` seconds or indefinitely if None
        or not provided.

        :returns: True if :attr:`worker` is alive, False otherwise.
        """
        self.worker.join(timeout=timeout)
        return self.worker.is_alive()

    def disconnect(self, timeout=None):
        """
        Kindly ask :attr:`worker` to stop and :meth:`wait`. :class:`Worker`
        may block for long(ish) periods before stopping.
        """
        self.worker.stop()
        return self.wait(timeout=timeout)

    def load_config_from_file(self, url, auth, config_file_path):
        """ Attempt to load the config from a file. """
        conf = config.get_config(config_file_path=config_file_path)

        if url is None:
            conf_url = conf['PowerTrack']['url']
            if conf_url:
                self.url = conf['PowerTrack']['url']
        else:
            self.url = url

        if auth is None:
            conf_uname = conf['Credentials']['username']
            conf_pwd = conf['Credentials']['password']
            self.auth = (conf_uname, conf_pwd)
        else:
            self.auth = auth


class Worker(threading.Thread):
    """ Background worker to fetch data without blocking """
    def __init__(self, url, auth, callback):
        super(Worker, self).__init__()
        self.url = url
        self.auth = auth
        self.on_data = callback
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def stream(self, response):
        for line in response.iter_lines():
            if line:
                self.on_data(line)

            if self.stopped():
                break

    def run(self):
        with closing(requests.get(self.url, auth=self.auth, stream=True)) as r:
            # Let user know if something went wrong
            r.raise_for_status()
            self.stream(r)
