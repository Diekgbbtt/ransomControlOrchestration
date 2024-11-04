#  run test with : pytest tests/test_orchestration.py -v --cov=orchestration --cov-report=term-missing

import pytest
import sys
import os
from json import load as json_load
from unittest.mock import Mock, patch
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from ..orchestration import controlFactory, ransomCheck, DelphixEngine
from ..utils import decrypt_value

@patch.object(DelphixEngine, 'login_data', return_value=True)
@patch.object(DelphixEngine, 'login_compliance', return_value=True) 
@patch.object(DelphixEngine, 'create_session', return_value={'session': 'mock_session'})
class TestControlFactory:


    @pytest.fixture
    def control_factory(self):
        return controlFactory("ransomCheck")

    @pytest.fixture
    def mock_control_data(self):
        with open('config.json', 'r') as cfg:
            cfg_dict = json_load(cfg)
        return cfg_dict.get("Controls")[0]

    @pytest.fixture
    def mock_cfg_data(self):
        with open('config.json', 'r') as cfg:
            cfg_dict = json_load(cfg)
        return cfg_dict

    def test_instance_control_valid_ransomcheck(control_factory, mock_control_data, mock_cfg_data):
        with patch('orchestration.ransomCheck') as mock_ransom:
            result = control_factory.instance_control(mock_control_data, mock_cfg_data)
            mock_ransom.assert_called_once_with(mock_control_data, mock_cfg_data)
            assert result == mock_ransom.return_value

    def test_instance_control_invalid_control():
        factory = controlFactory(controlName="invalidControl")
        with pytest.raises(Exception) as exc_info:
            factory.instance_control({}, {})
        assert "Control invalidControl not supported " in str(exc_info.value)

    @patch('orchestration.DelphixEngine')
    def test_instance_control_engine_initialization_failure(self, mock_delphix, control_factory, mock_control_data, mock_cfg_data):
        mock_delphix.login_data.side_effect = Exception("Engine initialization failed")
        with pytest.raises(Exception) as exc_info:
            control_factory.instance_control(mock_control_data, mock_cfg_data)
        assert "Engine initialization failed" in str(exc_info.value)

    def test_instance_control_missing_required_data(self, control_factory):
        incomplete_data = {'sourceEngineRef': 'engine1'}  # Missing required fields
        with pytest.raises(Exception) as exc_info:
            control_factory.instance_control(incomplete_data, {})
        assert "Error creating control instance" in str(exc_info.value)

    @patch('utils.decrypt_value')
    def test_instance_control_decryption_failure(self, mock_decrypt, control_factory, mock_control_data, mock_cfg_data):
        decrypt_value.side_effect = Exception("Decryption failed")
        with pytest.raises(Exception) as exc_info:
            control_factory.instance_control(mock_control_data, mock_cfg_data)
        assert "Decryption failed" in str(exc_info.value)



