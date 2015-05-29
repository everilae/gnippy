# -*- coding: utf-8 -*-


class GnippyException(Exception):
    """ Base class for all gnippy exceptions. """
    pass


class ConfigFileNotFoundException(GnippyException):
    """ Raised when an invalid config_file_path argument was passed. """
    pass


class IncompleteConfigurationException(GnippyException):
    """ Raised when no .gnippy file is found. """
    pass


class BadArgumentException(GnippyException):
    """ Raised when an invalid argument is detected. """
    pass


class RuleAddFailedException(GnippyException):
    """ Raised when a rule add fails. """
    pass


class RulesListFormatException(GnippyException):
    """ Raised when rules_list is not in the correct format. """
    pass


class RulesGetFailedException(GnippyException):
    """ Raised when listing the current rule set fails. """
    pass


class BadPowerTrackUrlException(GnippyException):
    """ Raised when the PowerTrack URL looks like its incorrect. """
    pass


class RuleDeleteFailedException(GnippyException):
    """ Raised when a rule delete fails. """
    pass


class HistoricalJobStatusException(GnippyException):
    """
    Raised when an action on a historical job was performed while in
    incorrect status.
    """
    pass
