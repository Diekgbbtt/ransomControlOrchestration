import pytest
from json import load as json_load

# pytest discovers tests configuration defined in this file, all fixtures deinfed in thsi file will be shared across all tests

@pytest.fixture
def mock_control_data():
    with open('config.json', 'r') as cfg:
        cfg_dict = json_load(cfg)
    return cfg_dict.get("Controls")[0]

@pytest.fixture
def mock_cfg_data():
    with open('config.json', 'r') as cfg:
        cfg_dict = json_load(cfg)
    return cfg_dict