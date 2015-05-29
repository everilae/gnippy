# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import requests
from gnippy import config
from gnippy.errors import BadArgumentException
from gnippy.compat import text_type
from datetime import datetime


class PowerTrackJob(object):
    """
    Historical PowerTrack job.

    Args:
        publisher (str): The data publisher you want the historical job to use.
            Currently ``"twitter"`` only.

        stream_type (str): Type of "stream" used for the job. Currently only
            PowerTrack (``"track"``) is available.

        data_format (str): The data format to use for the job. Defaults to
            ``"activity-stream"`` To request data in the Publisher's
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

        self._validate()

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
    """

    def __init__(self, **kwargs):
        c = config.resolve(kwargs)

        self.url = c['url']
        self.auth = c['auth']
