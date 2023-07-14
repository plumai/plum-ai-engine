from Crypto.Cipher import AES
from hashlib import pbkdf2_hmac


def _un_pad(byte_array):
    return byte_array[:-ord(byte_array[-1:])]


def decrypt(password, filename, file_encrypted_data_bytes):
    dk = pbkdf2_hmac('sha256', password.encode(encoding="utf-8"), password.encode(encoding="utf-8"), 65536)
    cipher = AES.new(key=dk, mode=AES.MODE_CBC, IV=filename.encode(encoding="utf-8")[0:16])
    decrypted_data = _un_pad(cipher.decrypt(file_encrypted_data_bytes))
    return decrypted_data
