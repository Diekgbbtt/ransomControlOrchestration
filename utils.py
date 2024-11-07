# /bin/env python3.12

from binascii import Error as binascii_Error
import json
import os
import sys
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.exceptions import InvalidTag, InternalError
from base64 import b64encode, b64decode
import secrets


ENCRYPT_KEYS = []
ENV_KEY_NAME = 'ENCRYPTION_KEY'  # Name of the environment variable for the encryption key

# Get or generate the encryption key
def get_encryption_key():
    key = os.environ.get(ENV_KEY_NAME)
    if key:
        return key.encode()[:32]  # Ensure the key is 32 bytes (AES-256 requires a 256-bit key)
    else:
        # Generate a new key if it doesn't exist
        os.environ[ENV_KEY_NAME] = secrets.token_hex(32)[:32]  # Set it as an environment variable for current session
        print(f"Environment variable '{ENV_KEY_NAME}' was not set. A new key has been generated and set.")
        return key.encode()

# Generate a random initialization vector (IV)
def generate_iv():
    return secrets.token_bytes(16)  # 16 bytes for AES-CBC

# Encrypt a plaintext string with AES-256-CBC
def encrypt_value(plaintext, key):
    iv = generate_iv()
    cipher = Cipher(algorithms.AES(bytes(key)), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Pad plaintext to AES block size (16 bytes)
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(plaintext.encode()) + padder.finalize()

    encrypted = encryptor.update(padded_data) + encryptor.finalize()
    return b64encode(iv + encrypted).decode()  # Encode to base64 for storage

# Process JSON file and encrypt specified keys
def encrypt_json_file(filepath):
    # Load JSON data
    with open(filepath, 'r') as file:
        data = json.load(file)

    # Fetch or generate the encryption key
    key_enc = get_encryption_key()

    # Recursively encrypt the specified keys in the JSON structure
    def encrypt_keys(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ENCRYPT_KEYS and isinstance(value, str):  # Only encrypt strings
                    obj[key] = encrypt_value(value, key_enc)
                elif isinstance(value, (dict, list)):
                    encrypt_keys(value)
        elif isinstance(obj, list):
            for item in obj:
                encrypt_keys(item)

    # Encrypt specified keys in JSON data
    encrypt_keys(data)

    # Save encrypted JSON data back to file
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"File '{filepath}' has been encrypted successfully.")

def decrypt_value(encrypted_value):

    try:    
        try:
            # Decode the base64 encoded data
            encrypted_data = b64decode(encrypted_value)
        except binascii_Error or Exception as e :
            raise Exception(f"Error decoding ecnrypted value {encrypted_value} from base64: \n {e}")
    
        # Extract the IV (first 16 bytes) and the actual ciphertext
        iv = encrypted_data[:16]
        ciphertext = encrypted_data[16:]

        _key = os.environ.get(ENV_KEY_NAME)  # Ensure the key is 32 bytes (AES-256 requires a 256-bit key)
        if _key == None:
            raise Exception(f"env variable {ENV_KEY_NAME} with encryption key not found, impossible to decrypt values")
        key = _key.encode()[:32]
        
        # Create the cipher object for decryption
        cipher = Cipher(algorithms.AES(bytes(key)), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        try:
            # Decrypt and then remove padding
            padded_data = decryptor.update(ciphertext) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded_data) + unpadder.finalize()
        except ValueError or TypeError as e:
            # TypeError - If the input data type is not bytes, or anyway incorrect format for padding
            # ValueError - padding is invalid or corrupted
            raise Exception(f"Error decrypting or unpadding decrypted value. \n Error : {e}")
        
        return plaintext.decode()
    except InternalError as e:
        raise Exception(f"Internal Error while decrypting value. \n Error : {str(e) if str(e) else e} \n Error code : {e.err_code if e.hasatttr('err_code') else ''}")
    except InvalidTag as e:
        raise Exception(f"invalid key or authentication tag : \n {e}")
    except Exception as e:
        raise Exception(f"Error decrypting value. \n Error : {e}")


if __name__ == "__main__":
    
    filepath = sys.argv[1]
    
    for key in sys.argv[2:]:
        ENCRYPT_KEYS.append(key)
    
    encrypt_json_file(filepath)