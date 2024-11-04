import pytest
from unittest.mock import patch, MagicMock
from cryptography.exceptions import InvalidKey, InternalError, InvalidTag
from base64 import b64decode
import os
from utils import decrypt_value
from json import load as json_load

class TestDecryptValue:
    @pytest.fixture
    def valid_env_key(self):
        return os.environ.get('ENCRYPTION_KEY')

    @pytest.fixture
    def valid_ciphertext(self):
        with open ('../config.json', 'r') as config:
            cfg_data = json_load(config)
            return cfg_data.get('mail').get('smtpServer')

    def test_successful_decryption(self, valid_env_key, valid_ciphertext):
        with patch.dict(os.environ, {'ENCRYPTION_KEY': valid_env_key}):
            result = decrypt_value(valid_ciphertext)
            assert result == "outlook.office365.com"


    def test_missing_env_key(self, valid_ciphertext):
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(Exception) as exc_info:
                decrypt_value(valid_ciphertext)
            assert "env variable" in str(exc_info.value)

    def test_invalid_base64_input(self):
        with patch.dict(os.environ, {'ENCRYPTION_KEY': "valid_key"}):
            with pytest.raises(Exception) as exc_info:
                decrypt_value("invalid base64!")
            assert "Error decrypting value" in str(exc_info.value)

    def test_internal_error(self, valid_ciphertext):
        with patch('cryptography.hazmat.primitives.ciphers.Cipher') as mock_cipher:
            mock_cipher.side_effect = InternalError(err_code=100, err_text="Internal error")
            with pytest.raises(Exception) as exc_info:
                decrypt_value(valid_ciphertext)
            assert "Internal Error while decrypting" in str(exc_info.value)

    def test_invalid_tag(self, valid_ciphertext):
        with patch('cryptography.hazmat.primitives.ciphers.Cipher') as mock_cipher:
            mock_cipher.side_effect = InvalidTag()
            with pytest.raises(Exception) as exc_info:
                decrypt_value(valid_ciphertext)
            assert "invalid key or authentication tag" in str(exc_info.value)

    def test_invalid_key_length(self):
        with patch.dict(os.environ, {'ENCRYPTION_KEY': "short_key"}):
            with pytest.raises(Exception):
                decrypt_value("KSWXTqBvcH9EVaGaCgSJZ0avZjHxoWX2QRN78R4f22md61oSr4WaakRwdrw7Uwjk")

    def test_corrupted_ciphertext(self, valid_env_key):
        with patch.dict(os.environ, {'ENCRYPTION_KEY': valid_env_key}):
            corrupted_ciphertext = "KSWXTqBvcH9EVaGaCgSJZ0avZjHxoWX2QRN78R4f22md61oSr4WaakRwdrw="
            with pytest.raises(Exception):
                decrypt_value(corrupted_ciphertext)

    @patch('cryptography.hazmat.primitives.padding.PKCS7')
    def test_padding_error(self, mock_pkcs7, valid_env_key, valid_ciphertext):
        mock_unpadder = MagicMock()
        mock_unpadder.update.side_effect = ValueError("Invalid padding")
        mock_pkcs7.return_value.unpadder.return_value = mock_unpadder
        
        with patch.dict(os.environ, {'ENCRYPTION_KEY': valid_env_key}):
            with pytest.raises(Exception) as exc_info:
                decrypt_value(valid_ciphertext)
            assert "Error decrypting value" in str(exc_info.value)