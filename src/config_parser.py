from configparser import ConfigParser, NoSectionError
import os

class ConfigValidationError(Exception):
    """Custom exception for configuration validation errors."""
    def __init__(self, errors):
        super().__init__(self, "Configuration validation failed.")
        self.errors = errors

    def __str__(self):
        return f"{self.message}\n" + "\n".join(self.errors)

def validateConfig(config: ConfigParser):
    if 'file_paths' not in config.sections():
        raise NoSectionError(f'file_paths section not found in configuration file')
    if 'lsh_params' not in config.sections():
        raise NoSectionError(f'lsh_params section not found in configuration file')
    if 'similarity_params' not in config.sections():
        raise NoSectionError(f'similarity_params section not found in configuration file')
    if 'min_hash_params' not in config.sections():
        raise NoSectionError(f'min_hash_params section not found in configuration file')
    if config.getint('min_hash_params', 'signature_size') != config.getint('lsh_params', 'bands') * config.getint('lsh_params', 'rows_per_band'):
        raise ConfigValidationError('Signature size should be band * number of rows per band')
    if config.getfloat('similarity_params', 'similarity_threshold') < 0 or config.getfloat('similarity_params', 'similarity_threshold') > 1:
        raise ConfigValidationError('Similarity threshold should be between 0 and 1')

def loadConfig() -> ConfigParser:
    config = ConfigParser()
    configFilepath = os.environ['CONFIG_FILEPATH']

    if not configFilepath or not os.path.isfile(configFilepath):
        raise FileNotFoundError(f"The configuration file was not found")

    config.read(configFilepath)
    validateConfig(config)
    return config

config = loadConfig()