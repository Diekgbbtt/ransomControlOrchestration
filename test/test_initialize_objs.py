import pytest
from unittest.mock import patch, Mock
from integrity_control import ransomCheck



@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_success(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_decrypt.return_value = 'decrypted_value'
    mock_engine_instance = Mock()
    mock_engine.return_value = mock_engine_instance
    
    ransom_check = ransomCheck(mock_control_data, mock_cfg_data)
    assert ransom_check.source_engine == mock_engine_instance
    assert ransom_check.vault_engine == mock_engine_instance
    assert ransom_check.disc_engine == mock_engine_instance

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_invalid_config(mock_decrypt, mock_engine, mock_control_data):
    invalid_cfg = {'dpx_engines': {'source_engines': []}}
    with pytest.raises(Exception, match="Engines are not configured properly in config file"):
        ransomCheck(mock_control_data, invalid_cfg)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_decrypt_failure(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_decrypt.side_effect = Exception("Decryption failed")
    with pytest.raises(Exception, match="Error initializing engines"):
        ransomCheck(mock_control_data, mock_cfg_data)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_create_session_failure(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_engine_instance = Mock()
    mock_engine_instance.create_session.side_effect = Exception("Session creation failed")
    mock_engine.return_value = mock_engine_instance
    
    with pytest.raises(Exception, match="Error initializing engines"):
        ransomCheck(mock_control_data, mock_cfg_data)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_login_data_failure(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_engine_instance = Mock()
    mock_engine_instance.login_data.side_effect = Exception("Login failed")
    mock_engine.return_value = mock_engine_instance
    
    with pytest.raises(Exception, match="Error initializing engines"):
        ransomCheck(mock_control_data, mock_cfg_data)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_login_compliance_failure(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_engine_instance = Mock()
    mock_engine_instance.login_compliance.side_effect = Exception("Compliance login failed")
    mock_engine.return_value = mock_engine_instance
    
    with pytest.raises(Exception, match="Error initializing engines"):
        ransomCheck(mock_control_data, mock_cfg_data)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_missing_vdb_details(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_cfg_data['vdbs_control']['vdb1'] = 'invalid'
    with pytest.raises(Exception, match="Error in config data: missing vdb control details"):
        ransomCheck(mock_control_data, mock_cfg_data)

@patch('orchestration.DelphixEngine')
@patch('utils.decrypt_value')
def test_initialize_objs_missing_mail_details(mock_decrypt, mock_engine, mock_control_data, mock_cfg_data):
    mock_cfg_data['mail'] = 'invalid'
    with pytest.raises(Exception, match="Error in config data: missing mail details"):
        ransomCheck(mock_control_data, mock_cfg_data)