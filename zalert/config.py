# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""config
=========

The configuration is a JSON-formatted file.  See ``_config_example``.

"""
import os
import sbsearch.config

__all__ = ['Config']

_config_example = '''
{
  "database": "/path/to/zalert.db",
  "log": "/path/to/zalert.log"
}
'''


class Config(sbsearch.config.Config):
    """ZAlert configuration.

    Controls database location, log file location.  Parameters are
    stored as object keys, e.g., `Config['log']`.

    Parameters
    ----------
    **kwargs
        Override parameters with these values.

    **kwargs
      Additional or updated configuration parameters and values.

    """

    DEFAULT_FILE = os.path.expanduser('~/.config/zalert.config')

    def __init__(self, **kwargs):
        import json
        config = json.loads(_config_example)
        config.update(location='I41', **kwargs)
        super().__init__(**config)
