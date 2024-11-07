#  run test with : pytest tests/test_orchestration.py -v --cov=orchestration --cov-report=term-missing

import pytest
import sys
import os
from json import load as json_load
from unittest.mock import Mock, patch
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(_file_))))
from orchestration import controlFactory, ransomCheck, DelphixEngine
from utils import decrypt_value


@pytest.fixture
def control_factory():
    return controlFactory("ransomCheck")


def test_instance_control_valid_ransomcheck(control_factory, mock_control_data, mock_cfg_data):
    with patch('orchestration.ransomCheck') as mock_ransom:
        # Mock the initialize_objs method to do nothing
        mock_ransom.return_value.initialize_objs = Mock(return_value=None)
        result = control_factory.instance_control(mock_control_data, mock_cfg_data)
        mock_ransom.assert_called_once_with(control_data=mock_control_data, cfg_data=mock_cfg_data)
    assert result == mock_ransom.return_value

def test_instance_control_invalid_control():
    factory = controlFactory(controlName="invalidControl")
    with pytest.raises(Exception) as exc_info:
        factory.instance_control({}, {})
    assert "Control invalidControl not supported" in str(exc_info.value)


@patch('orchestration.DelphixEngine')
def test_instance_control_engine_initialization_failure(mock_delphix, control_factory, mock_control_data, mock_cfg_data):
    mock_delphix.return_value = mock_delphix
    mock_delphix._init_.side_effect = RuntimeError("Failed to initialize Delphix Engine")
    with pytest.raises(RuntimeError) as exc_info:
        control_factory.instance_control(mock_control_data, mock_cfg_data)
    assert str(exc_info.value) == "Failed to initialize Delphix Engine"


def test_instance_control_missing_required_data(control_factory):
    incomplete_data = {'sourceEngineRef': 'engine1'}  # Missing required fields
    with pytest.raises(Exception) as exc_info:
        control_factory.instance_control(incomplete_data, {})
    assert "Invalid config, missing parameter in control data" in str(exc_info.value)

@patch('utils.decrypt_value')
def test_instance_control_decryption_failure(mock_decrypt, control_factory, mock_control_data, mock_cfg_data):
    decrypt_value.side_effect = Exception("Decryption failed")
    with pytest.raises(Exception) as exc_info:
        control_factory.instance_control(mock_control_data, mock_cfg_data)
    assert "env variable ENCRYPTION_KEY with encryption key not found, impossible to decrypt values." or "Decryption failed" in str(exc_info.value)