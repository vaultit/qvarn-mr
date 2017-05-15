import logging.config
import configparser


_config = None
_config_filename = None


def read_config(config):
    """Read configuration from a file path or a dict.

    Args:
        config (Union[str, dict]): configuration to be read
    """
    parser = configparser.RawConfigParser()
    if isinstance(config, str):
        parser.read(config)
    else:
        parser.read_dict(config)
    # Configure logging only if configuration is provided.
    if parser.has_section('loggers'):
        # We can't disable existing loggers, because loggers are created at module level, before
        # this line.
        logging.config.fileConfig(parser, disable_existing_loggers=False)
    return parser


def set_config(config):
    global _config, _config_filename
    if isinstance(config, str):
        _config_filename = config
    else:
        _config_filename = '<dict>'
    _config = read_config(config)


def get_config():
    global _config
    # Read configuration
    if _config is None:
        raise RuntimeError('You need to call set_config() first!')
    return _config


def get_config_filename():
    global _config_filename
    return _config_filename
