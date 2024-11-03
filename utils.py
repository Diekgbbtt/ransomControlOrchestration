# /bin/env python3.12

from json import load as json_load, dump
from orchestration import controlClass
from progress.bar import IncrementalBar
from datetime import datetime
import os
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding
from cryptography.exceptions import InvalidTag, InternalError
from base64 import b64encode, b64decode
import secrets



# Keys we want to encrypt
ENCRYPT_KEYS = ['host', 'usr', 'pwd']
ENV_KEY_NAME = 'ENCRYPTION_KEY'  # Name of the environment variable for the encryption key

# Get or generate the encryption key
def get_encryption_key():
    key = os.environ.get(ENV_KEY_NAME)
    if key:
        return key.encode()[:32]  # Ensure the key is 32 bytes (AES-256 requires a 256-bit key)
    else:
        # Generate a new key if it doesn't exist
        key = secrets.token_hex(32)[:32]  # Generate a 32-character key
        os.environ[ENV_KEY_NAME] = key  # Set it as an environment variable for current session
        print(f"Environment variable '{ENV_KEY_NAME}' was not set. A new key has been generated and set.")
        return key.encode()

# Generate a random initialization vector (IV)
def generate_iv():
    return secrets.token_bytes(16)  # 16 bytes for AES-CBC

# Encrypt a plaintext string with AES-256-CBC
def encrypt_value(plaintext, key):
    iv = generate_iv()
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    encryptor = cipher.encryptor()

    # Pad plaintext to AES block size (16 bytes)
    padder = padding.PKCS7(128).padder()
    padded_data = padder.update(plaintext.encode()) + padder.finalize()

    encrypted = encryptor.update(padded_data) + encryptor.finalize()
    return b64encode(iv + encrypted).decode()  # Encode to base64 for storage


def decrypt_value(encrypted_value):

    try:    
        # Decode the base64 encoded data
        encrypted_data = b64decode(encrypted_value)
        
        # Extract the IV (first 16 bytes) and the actual ciphertext
        iv = encrypted_data[:16]
        ciphertext = encrypted_data[16:]

        key = os.environ.get(ENV_KEY_NAME).encode()[:32]  # Ensure the key is 32 bytes (AES-256 requires a 256-bit key)
        if key == None:
            raise Exception(msg=f"env variable {ENV_KEY_NAME} with encryption key not found, impossible to decrypt values")

        
        # Create the cipher object for decryption
        cipher = Cipher(algorithms.AES(bytes(key)), modes.CBC(iv), backend=default_backend())
        decryptor = cipher.decryptor()
        
        # Decrypt and then remove padding
        padded_data = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_data) + unpadder.finalize()
        
        return plaintext.decode()
    except InternalError as e:
        raise Exception(msg=f"Internal Error while decrypting value. \n Error : {e.msg if e.hasattr('msg') else e} \n Error code : {e.err_code if e.hasatttr('err_code') else ''}")
    except InvalidTag as e:
        raise Exception(msg=f"invalid key or authentication tag : \n {e}")
    except Exception as e:
        raise Exception(msg=f"Error decrypting value. \n Error : {e}")


# Process JSON file and encrypt specified keys
def encrypt_json_file(filepath):
    # Load JSON data
    with open(filepath, 'r') as file:
        data = json_load(file)

    # Fetch or generate the encryption key
    key = get_encryption_key()

    # Recursively encrypt the specified keys in the JSON structure
    def encrypt_keys(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key in ENCRYPT_KEYS and isinstance(value, str):  # Only encrypt strings
                    obj[key] = encrypt_value(value, key)
                elif isinstance(value, (dict, list)):
                    encrypt_keys(value)
        elif isinstance(obj, list):
            for item in obj:
                encrypt_keys(item)

    # Encrypt specified keys in JSON data
    encrypt_keys(data)

    # Save encrypted JSON data back to file
    with open(filepath, 'w') as file:
        dump(data, file, indent=4)
    print(f"File '{filepath}' has been encrypted successfully.")


def load_config():
    try:
        with open('config.json', 'r') as cfg:
                cfg_dict = json_load(cfg)
        return cfg_dict

    except Exception as e:
            print(f"Error opening config: {str(e)}")


def controlDatabase(check: controlClass):

    print(f"Starting control {check.name} ")
    s_time = datetime.now()
    try :
        check.start()
        f_time = datetime.now()
        os.system('clear')
        print(f"Finished control {check.name} in {f_time - s_time} seconds")
    except Exception as e:
        raise Exception(msg=(e.msg if e.hasattr('msg') else f"Error executing control {check.name}: \n Error : {e}"))

# display a bar that shows the progress of the process
# def print_process_status():
  #       bar = IncrementalBar(suffix='%(index)d/%(max)d [%(elapsed)d / %(eta)d / %(eta_td)s] (%(iter_value)s)', color='blue', max=100)
    #     for i in bar.iter(range(200)):
      #      time.sleep(0.01)



# Example usage
if __name__ == "__main__":
    filepath = 'config.json'  # Replace with the path to your JSON file
    encrypt_json_file(filepath)


