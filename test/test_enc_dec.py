import os
import sys 

from utils import decrypt_value, encrypt_value


def test_enc_decryption(plaintext):

    encryypted_value = encrypt_value(plaintext, os.environ.get().encode()[:32])
    decrypted_value = decrypt_value(encryypted_value)
    print(f"Decrypted value: {decrypted_value}")


