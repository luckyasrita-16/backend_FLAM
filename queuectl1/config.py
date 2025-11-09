from queuectl1.storage import set_config, get_config

def set_config_value(key, value):
    """
    Set configuration key to the given value.
    """
    set_config(key, str(value))

def get_config_value(key, default=None):
    """
    Get the configuration value for the given key.
    Returns default if key is not set.
    """
    value = get_config(key)
    if value is None:
        return default
    return value
