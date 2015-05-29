# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function

import sys

PY2 = sys.version_info < (3,)
PY3 = not PY2

if PY2:
    text_type = unicode
    string_types = basestring

else:
    text_type = str
    string_types = str
