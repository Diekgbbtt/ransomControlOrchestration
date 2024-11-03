#  run test with : pytest tests/test_orchestration.py -v --cov=orchestration --cov-report=term-missing


import pytest
from unittest.mock import Mock, patch
from ..orchestration import controlFactory, ransomCheck

@pytest.fixture
def control_factory():
    return controlFactory("ransomCheck")

@pytest.fixture
def mock_control_data():
    return {
        'sourceEngineRef': 'engine1',
        'vaultEngineRef': 'engine13',
        'discEngineRef': 'engine_compl',
        'replicationSpec': 'rep1',
        'vdbContainerRef': 'vdb1',
        'dSourceContainer_ref': 'ds1',
        'jobId': 'job1'
    }

@pytest.fixture
def mock_cfg_data():
    return {
        'dpx_engines': {
            'source_engines': {'engine1': {'host': 'enc_host1', 'apiVersion': '1.0.0', 'usr': 'enc_usr1', 'pwd': 'enc_pwd1'}},
            'vault_engines': {'engine13': {'host': 'enc_host13', 'apiVersion': '1.0.0', 'usr': 'enc_usr13', 'pwd': 'enc_pwd13'}},
            'discovery_engines': {'engine_compl': {'usr': 'enc_usr_compl', 'pwd': 'enc_pwd_compl'}}
        },
        'vdbs_control': {'vdb1': {'host': 'enc_vdb_host', 'port': 'enc_port', 'usr': 'enc_usr', 'pwd': 'enc_pwd', 'sid': 'enc_sid'}},
        'mail': {'smtpServer': 'enc_smtp', 'usr': 'enc_mail_usr', 'pwd': 'enc_mail_pwd'}
    }

def test_instance_control_valid_ransomcheck(control_factory, mock_control_data, mock_cfg_data):
    with patch('orchestration.ransomCheck') as mock_ransom:
        result = control_factory.instance_control(mock_control_data, mock_cfg_data)
        mock_ransom.assert_called_once_with(mock_control_data, mock_cfg_data)
        assert result == mock_ransom.return_value

def test_instance_control_invalid_control():
    factory = controlFactory("invalidControl")
    with pytest.raises(Exception) as exc_info:
        factory.instance_control({}, {})
    assert "Control invalidControl not supported " in str(exc_info.value)

def test_instance_control_none_control():
    factory = controlFactory(None)
    with pytest.raises(Exception) as exc_info:
        factory.instance_control({}, {})
    assert "Unsupported control type: None" in str(exc_info.value)

@patch('orchestration.DelphixEngine')
def test_instance_control_engine_initialization_failure(mock_delphix, control_factory, mock_control_data, mock_cfg_data):
    mock_delphix.side_effect = Exception("Engine initialization failed")
    with pytest.raises(Exception) as exc_info:
        control_factory.instance_control(mock_control_data, mock_cfg_data)
    assert "Engine initialization failed" in str(exc_info.value)

def test_instance_control_missing_required_data(control_factory):
    incomplete_data = {'sourceEngineRef': 'engine1'}  # Missing required fields
    with pytest.raises(Exception) as exc_info:
        control_factory.instance_control(incomplete_data, {})
    assert "Error creating control instance" in str(exc_info.value)

@patch('orchestration.decrypt_value')
def test_instance_control_decryption_failure(mock_decrypt, control_factory, mock_control_data, mock_cfg_data):
    mock_decrypt.side_effect = Exception("Decryption failed")
    with pytest.raises(Exception) as exc_info:
        control_factory.instance_control(mock_control_data, mock_cfg_data)
    assert "Decryption failed" in str(exc_info.value)

def test_instance_control_another_check(mock_control_data, mock_cfg_data):
    factory = controlFactory("anotherCheck")
    result = factory.instance_control(mock_control_data, mock_cfg_data)
    assert result is None

@pytest.mark.parametrize("control_name", [
    "",
    "RANSOMCHECK",
    "ransom_check",
    123,
    True,
    {},
    []
])
def test_instance_control_invalid_control_names(control_name):
    factory = controlFactory(control_name)
    with pytest.raises(Exception) as exc_info:
        factory.instance_control({}, {})
    assert f"Unsupported control type: {control_name}" in str(exc_info.value)
