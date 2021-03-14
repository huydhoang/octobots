# AES 256 encryption/decryption using pycrypto library
 
import base64
from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Protocol.KDF import PBKDF2
from _SECRETS import PASSWORD, SALT
 

BLOCK_SIZE = 16
pad = lambda s: s + (BLOCK_SIZE - len(s) % BLOCK_SIZE) * chr(BLOCK_SIZE - len(s) % BLOCK_SIZE)
unpad = lambda s: s[:-ord(s[len(s) - 1:])]

def get_private_key():
    password = PASSWORD
    salt = SALT
    kdf = PBKDF2(password, salt, 64, 1000)
    key = kdf[:32]
    return key


def encrypt(raw):
    private_key = get_private_key()
    raw = pad(raw)
    iv = Random.new().read(AES.block_size)
    cipher = AES.new(private_key, AES.MODE_CBC, iv)
    return bytes.decode(base64.b64encode(iv + cipher.encrypt(raw)))


def decrypt(enc):
    private_key = get_private_key()
    enc = base64.b64decode(enc)
    iv = enc[:16]
    cipher = AES.new(private_key, AES.MODE_CBC, iv)
    return bytes.decode(unpad(cipher.decrypt(enc[16:])))
 
 
# # First let us encrypt secret message
# encrypted = encrypt("some secret message")
# print(encrypted)
 
# # Let us decrypt using our original password
# decrypted = decrypt(encrypted)
# print(bytes.decode(decrypted))

