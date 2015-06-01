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


class PowerTrackJobResults(object):
    """
    Historical PowerTrack job result.

    Args:
        completed_at:
        activity_count:
        file_count:
        file_size_mb:
        data_url:
        expires_at:
        client:
    """

    def __init__(self,
                 completed_at=None,
                 activity_count=None,
                 file_count=None,
                 file_size_mb=None,
                 data_url=None,
                 expires_at=None,
                 client=None):
        self.completed_at = completed_at
        self.activity_count = activity_count
        self.file_count = file_count
        self.file_size_mb = file_size_mb
        self.data_url = data_url
        self.expires_at = expires_at

        self._client = client

        self._url_count = None
        self._url_list = None
        self._total_file_size_bytes = None
        self._suspect_minutes_url = None
        self._expires_at = None

    def get(self):
        """
        Retrieves information about a completed Historical PowerTrack job,
        including a list of URLs which correspond to the data files generated
        for a completed historical job. These URLs will be used to download the
        Twitter data files.
        """
        r = self._client._get(self.data_url)
        r.raise_for_status()
        data = r.json()
        self._url_count = data['urlCount']
        self._url_list = data['urlList']
        self._total_file_size_bytes = data['totalFileSizeBytes']
        self._suspect_minutes_url = data.get('suspectMinutesUrl')
        self._expires_at = iso8601_to_dt(data['expires_at'])



class PowerTrackJob(object):
    """
    Historical PowerTrack job.

    Args:
        publisher (str): The data publisher you want the historical job to use.
            Currently ``"twitter"`` only.

        stream_type (str): Type of "stream" used for the job. Currently only
            PowerTrack (``"track"``) is available.

        data_format (str): The data format to use for the job. Defaults to
            ``"activity-streams"``. To request data in the Publisher's
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

        client (HistoricalPowerTrackClient): The client instance that created
            this job.

    Raises:
        BadArgumentException: for invalid arguments.

    """

    VALID_DATA_FORMATS = {"activity-streams", "original"}

    def __init__(self,
                 title=None,
                 from_date=None,
                 to_date=None,
                 rules=None,
                 publisher="twitter",
                 stream_type="track",
                 data_format="activity-streams",
                 client=None):
        self.publisher = publisher
        self.stream_type = stream_type
        self.data_format = data_format
        self.from_date = from_date
        self.to_date = to_date
        self.title = title
        self.rules = rules

        self._client = client

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
        self._job_url = data['jobURL']
        self._requested_by = data['requestedBy']
        self._requested_at = iso8601_to_dt(data['requestedAt'])
        self._status = data['status']
        self._status_message = data['statusMessage']
        self._quote = data.get('quote')
        self._accepted_by = data.get('acceptedBy')

        if 'acceptedAt' in data:
            self._accepted_at = iso8601_to_dt(data['acceptedAt'])

        else:
            self._accepted_at = None

        self._percent_complete = data.get('percentComplete')

        if 'results' in data:
            results = data['results']
            self._results = PowerTrackJobResults(
                completed_at=iso8601_to_dt(results['completedAt']),
                activity_count=results['activityCount'],
                file_count=results['fileCount'],
                file_size_mb=results['fileSizeMb'],
                data_url=results['dataUrl'],
                expires_at=iso8601_to_dt(results['expiresAt']),
                client=self._client
            )

        else:
            self._results = None

    @staticmethod
    def _get(job_url, client):
        r = client._get(job_url)
        r.raise_for_status()
        return r.json()

    @classmethod
    def get(cls, job_url, client):
        """
        Get a monitor to an existing job. Will not include original
        :attr:`rules`.

        See :meth:`monitor`.

        Args:
            job_url (str): job instance url
            client (HistoricalPowerTrackClient): client

        Returns:
            PowerTrackJob:
            Job instance for given url.

        """
        data = cls._get(job_url, client)
        obj = cls(
            title=data['title'],
            from_date=iso8601_to_dt(data['fromDate']),
            to_date=iso8601_to_dt(data['toDate']),
            rules=data.get('rules', []),  # unfortunate
            publisher=data['publisher'],
            stream_type=data['streamType'],
            data_format=data['dataFormat'],
            client=client
        )
        cls._update(obj, data)

        return obj

    @staticmethod
    def _format_date(dt):
        return dt.strftime("%Y%m%d%H%M")

    def post(self):
        """
        Creates a new Historical PowerTrack job from this instance.
        """
        data = json.dumps(dict(
            publisher=self.publisher,
            streamType=self.stream_type,
            dataFormat=self.data_format,
            fromDate=self._format_date(self.from_date),
            toDate=self._format_date(self.to_date),
            title=self.title,
            rules=self.rules
        ))
        r = self._client._post(self._client.api_url, data=data)
        r.raise_for_status()
        self._update(r.json())

    def monitor(self):
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
        """
        self._update(self._get(self._job_url, self._client))

    def _accept_or_reject(self, verb):
        if self._status != "quoted":
            raise HistoricalJobStatusException

        r = self._client._put(self._job_url, data=json.dumps(dict(status=verb)))
        r.raise_for_status()
        data = r.json()
        self._status = data['status']
        self._status_message = data['statusMessage']
        self._accepted_by = data['acceptedBy']
        self._accepted_at = iso8601_to_dt(data['acceptedAt'])

    def accept(self):
        """
        Accepts a historical job in the "quoted" stage.
        Accepted jobs will be run by Gnip's system, and cannot be stopped
        after acceptance.

        Raises:
            HistoricalJobStatusException: if job is not in status "quoted".

        """
        self._accept_or_reject("accept")

    def reject(self):
        """
        Rejects a historical job in the "quoted" stage.
        Rejected jobs cannot be recovered.

        Raises:
            HistoricalJobStatusException: if job is not in status "quoted".

        """
        self._accept_or_reject("reject")

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
    Historical PowerTrack client allows you to create historical jobs and fetch
    results as they become available.

    Args:
        api_url: historical powertrack api url
        auth: authentication, ``("account", "password")`` tuple

    """

    def __init__(self, **kwargs):
        c = config.resolve(kwargs)

        # For the time being handle this as kwarg only, add it to the
        # configuration machinery later.
        self.api_url = kwargs['api_url']
        self.auth = c['auth']
        self._session = requests.Session()
        self._session.auth = self.auth
        self._session.headers.update({'Content-Type': 'application/json'})

    def _get(self, *args, **kwgs):
        return self._session.get(*args, **kwgs)

    def _post(self, *args, **kwgs):
        return self._session.post(*args, **kwgs)

    def _put(self, *args, **kwgs):
        return self._session.put(*args, **kwgs)

    def create_job(self, **kwgs):
        """
        Create a new :class:`PowerTrackJob` bound to this client.

        Args:
            publisher (str): The data publisher you want the historical job to use.
                Currently ``"twitter"`` only.

            stream_type (str): Type of "stream" used for the job. Currently only
                PowerTrack (``"track"``) is available.

            data_format (str): The data format to use for the job. Defaults to
                ``"activity-streams"``. To request data in the Publisher's
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

        Returns:
            PowerTrackJob:
            New historical powertrack job.

        """
        job = PowerTrackJob(client=self, **kwgs)
        #job.post()
        return job

    def get_job(self, job_url):
        """
        Get a monitor to an existing job. See :meth:`PowerTrackJob.get`.

        Returns:
            PowerTrackJob:
            PowerTrack job for url.

        """
        return PowerTrackJob.get(job_url, self)
