# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import json
import requests
from gnippy import config
from gnippy.errors import BadArgumentException, HistoricalJobStatusException
from gnippy.compat import text_type
from datetime import datetime


def iso8601_to_dt(s):
    """
    Helper function for a very naive conversion of very specific ISO8601
    strings to python ``datetime`` objects.
    """
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


class PowerTrackJob(object):
    """
    Historical PowerTrack job.

    Args:
        publisher (str): The data publisher you want the historical job to use.
            Currently ``"twitter"`` only.

        stream_type (str): Type of "stream" used for the job. Currently only
            PowerTrack (``"track"``) is available.

        data_format (str): The data format to use for the job. Defaults to
            ``"activity-stream"``. To request data in the Publisher's
            original/native format use ``"original"``.

        from_date (datetime): A naive datetime indicating the start time of the
            period of interest with minute granularity. This date is inclusive,
            meaning the minute specified will be included in the job.

        to_date (datetime): A naive datetime indicating the end time of the
            period of interest with minute granularity. This is **NOT**
            inclusive, so a time of 00:00 will return data through 23:59 of
            previous day. I.e. the specified minute will not be included in the
            job, but the minute immediately preceeding it will in the UTC time
            zone.

        title (str): A title for the historical job. This must be unique and
            jobs with duplicate titles will be rejected.

        rules (list): The rules which will determine what data is returned by
            your job. Historical PowerTrack jobs support up to 1000 PowerTrack
            rules. For information on PowerTrack rules, see our documentation
            here, and be sure to consider the caveats listed here.
            For example::

                [
                    {
                        "value": "cow"
                    },
                    {
                        "value": "dog",
                        "tag": "pets"
                    }
                ]

    Raises:
        BadArgumentException: for invalid arguments.

    """

    VALID_DATA_FORMATS = {"activity-stream", "original"}

    def __init__(self,
                 title=None,
                 from_date=None,
                 to_date=None,
                 rules=None,
                 publisher="twitter",
                 stream_type="track",
                 data_format="activity-stream"):
        self.publisher = publisher
        self.stream_type = stream_type
        self.data_format = data_format
        self.from_date = from_date
        self.to_date = to_date
        self.title = title
        self.rules = rules

        self._account = None
        self._requested_by = None
        self._requested_at = None
        self._status = None
        self._status_message = None
        self._job_url = None
        self._quote = None
        self._accepted_by = None
        self._accepted_at = None
        self._percent_complete = None
        self._results = None

        self._validate()

    def _update(self, data):
        self._account = data['account']
        self._job_url = data['jobUrl']
        self._requested_by = data['requestedBy']
        self._requested_at = iso8601_to_dt(data['requestedAt'])
        self._status = data['status']
        self._status_message = data['statusMessage']
        self._quote = data.get('quote')
        self._accepted_by = data.get('acceptedBy')
        if 'acceptedAt' in data:
            self._accepted_at = iso8601_to_dt(data['acceptedAt'])
        self._percent_complete = data.get('percentComplete')
        self._results = data.get('results')

    @staticmethod
    def _get(job_url, auth):
        r = requests.get(job_url, auth=auth)
        r.raise_for_status()
        return r.json()

    @classmethod
    def get(cls, job_url, auth):
        """
        Get a monitor to an existing job. Will not include original
        :attr:`rules`.

        See :meth:`monitor`.

        Args:
            job_url: job instance url
            auth: authentication, ``("account", "password")`` tuple
        """
        data = cls._get(job_url, auth)
        obj = cls(
            title=data['title'],
            from_date=iso8601_to_dt(data['fromDate']),
            to_date=iso8601_to_dt(data['toDate']),
            rules=data.get('rules', []),  # unfortunate
            publisher=data['publisher'],
            stream_type=data['streamType'],
            data_format=data['dataFormat']
        )
        cls._update(obj, data)

        return obj

    def monitor(self, auth):
        """
        Monitors the status of a historical job.

        After a job is created, you can use this request to monitor the current
        status of the specific job.

        When the job is in the process of generating an estimate of expected
        order of magnitude of activity volume and time required, this request
        provides insight into the progress in the estimating process. Once this
        estimate is complete, the response will indicate the volume and time
        estimates referenced.

        After the job has been accepted, the response can be used to track its
        progress as the data files are generated.

        Args:
            auth: authentication, ``("account", "password")`` tuple
        """
        data = self._get(self._job_url, auth)
        self._update(data)

    def _accept_or_reject(self, verb, auth):
        if self._status != "quoted":
            raise HistoricalJobStatusException

        r = requests.put(self._job_url, auth=auth,
                         data=json.dumps(dict(status=verb)))
        r.raise_for_status()
        data = r.json()
        self._status = data['status']
        self._status_message = data['statusMessage']
        self._accepted_by = data['acceptedBy']
        self._accepted_at = iso8601_to_dt(data['acceptedAt'])

    def accept(self, auth):
        """
        Accepts or rejects a historical job in the "quoted" stage.
        Accepted jobs will be run by Gnip's system, and cannot be stopped
        after acceptance.

        Args:
            auth: authentication, ``("account", "password")`` tuple.

        Raises:
            HistoricalJobStatusException: if job is not in status "quoted".
        """
        self._accept_or_reject("accept", auth)

    def reject(self, auth):
        """
        Rejects a historical job in the "quoted" stage.
        Rejected jobs cannot be recovered.

        Args:
            auth: authentication, ``("account", "password")`` tuple.

        Raises:
            HistoricalJobStatusException: if job is not in status "quoted".
        """
        self._accept_or_reject("reject", auth)

    @property
    def account(self):
        return self._account

    @property
    def requested_by(self):
        return self._requested_by

    @property
    def requsted_at(self):
        return self._requested_at

    @property
    def status(self):
        return self._status

    @property
    def status_message(self):
        return self._status_message

    @property
    def job_url(self):
        return self._job_url

    @property
    def quote(self):
        return self._quote

    @property
    def accepted_by(self):
        return self._accepted_by

    @property
    def accepted_at(self):
        return self._accepted_at

    @property
    def percent_complete(self):
        return self._percent_complete

    @property
    def results(self):
        return self._results

    def _validate(self):
        if self.publisher != "twitter":
            raise BadArgumentException("publisher must be 'twitter'")

        if self.stream_type != "track":
            raise BadArgumentException("stream_type must be 'track'")

        if self.data_format not in self.VALID_DATA_FORMATS:
            raise BadArgumentException(
                "data_format must be one of %r, got %r" % (
                self.VALID_DATA_FORMATS, self.data_format))

        if not isinstance(self.from_date, datetime):
            raise BadArgumentException(
                "from_date must be of type 'datetime', got %r" %
                type(self.from_date))

        if not isinstance(self.to_date, datetime):
            raise BadArgumentException(
                "to_date must be of type 'datetime', got %r" %
                type(self.to_date))

        if self.to_date <= self.from_date:
            raise BadArgumentException(
                "from_date is larger or equal to to_date")

        if not (self.title and isinstance(self.title, text_type)):
            raise BadArgumentException(
                "title must be a non-empty string, got %r" % self.title)

        if not isinstance(self.rules, list):
            raise BadArgumentException("rules must be of type 'list', got %r" %
                                       type(self.rules))

        if len(self.rules) > 1000:
            raise BadArgumentException("too many rules")


class HistoricalPowerTrackClient(object):
    """
    Historical PowerTrack client.

    Args:
        auth: authentication, ``("account", "password")`` tuple
    """

    def __init__(self, **kwargs):
        c = config.resolve(kwargs)

        self.auth = c['auth']
