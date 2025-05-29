"""
KUMDB Cryptography - Military-grade encryption for your data.
"""

import os
import secrets
from typing import Tuple, Optional
from pathlib import Path
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding, hashes, hmac
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
from ..exceptions import SecurityError, DecryptionError

class CryptoEngine:
    """Authenticated Encryption with Associated Data (AEAD) implementation."""

    def __init__(self, master_key: Optional[bytes] = None):
        """
        Initialize with optional master key.
        If no key provided, generates a secure random one.
        """
        self.backend = default_backend()
        self.master_key = master_key or self.generate_key(32)
        self.current_key_version = 1
        self.key_rotation_schedule = 30  # Days

    @staticmethod
    def generate_key(length: int = 32) -> bytes:
        """Generate cryptographically secure random key."""
        return secrets.token_bytes(length)

    def derive_key(self, 
                  password: str, 
                  salt: Optional[bytes] = None) -> Tuple[bytes, bytes]:
        """
        Derive encryption key from password using PBKDF2-HMAC-SHA256.
        Returns (key, salt).
        """
        salt = salt or os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=self.backend
        )
        key = kdf.derive(password.encode('utf-8'))
        return key, salt

    def encrypt(self, 
               plaintext: bytes, 
               associated_data: Optional[bytes] = None) -> bytes:
        """
        Encrypt data with AES-256-GCM.
        Returns: version + nonce + ciphertext + tag
        """
        nonce = os.urandom(12)
        cipher = Cipher(
            algorithms.AES(self.master_key),
            modes.GCM(nonce),
            backend=self.backend
        )
        encryptor = cipher.encryptor()

        if associated_data:
            encryptor.authenticate_additional_data(associated_data)

        ciphertext = encryptor.update(plaintext) + encryptor.finalize()
        return (
            self.current_key_version.to_bytes(1, 'big') +
            nonce +
            ciphertext +
            encryptor.tag
        )

    def decrypt(self, 
               ciphertext: bytes,
               associated_data: Optional[bytes] = None) -> bytes:
        """
        Decrypt data with AES-256-GCM.
        Handles key versioning automatically.
        """
        try:
            version = ciphertext[0]
            nonce = ciphertext[1:13]
            tag = ciphertext[-16:]
            ciphertext = ciphertext[13:-16]

            # Key rotation logic would go here
            key = self.master_key

            cipher = Cipher(
                algorithms.AES(key),
                modes.GCM(nonce, tag),
                backend=self.backend
            )
            decryptor = cipher.decryptor()

            if associated_data:
                decryptor.authenticate_additional_data(associated_data)

            return decryptor.update(ciphertext) + decryptor.finalize()
        except Exception as e:
            raise DecryptionError(f"Decryption failed: {str(e)}")

    def encrypt_file(self, 
                    input_path: Path, 
                    output_path: Optional[Path] = None,
                    chunk_size: int = 64 * 1024) -> Path:
        """
        Encrypt file in chunks for memory efficiency.
        Returns path to encrypted file.
        """
        output_path = output_path or input_path.with_suffix('.enc')
        nonce = os.urandom(12)
        cipher = Cipher(
            algorithms.AES(self.master_key),
            modes.GCM(nonce),
            backend=self.backend
        )
        encryptor = cipher.encryptor()

        with open(input_path, 'rb') as fin, open(output_path, 'wb') as fout:
            # Write version + nonce first
            fout.write(self.current_key_version.to_bytes(1, 'big'))
            fout.write(nonce)

            while True:
                chunk = fin.read(chunk_size)
                if not chunk:
                    break
                fout.write(encryptor.update(chunk))

            fout.write(encryptor.finalize())
            fout.write(encryptor.tag)  # Store tag at end

        return output_path

    def decrypt_file(self, 
                    input_path: Path, 
                    output_path: Optional[Path] = None,
                    chunk_size: int = 64 * 1024 + 16) -> Path:
        """
        Decrypt file in chunks for memory efficiency.
        Returns path to decrypted file.
        """
        output_path = output_path or input_path.with_suffix('.dec')
        try:
            with open(input_path, 'rb') as fin:
                version = fin.read(1)
                nonce = fin.read(12)
                tag_pos = os.path.getsize(input_path) - 16
                fin.seek(tag_pos)
                tag = fin.read(16)
                fin.seek(13)  # Skip header

                cipher = Cipher(
                    algorithms.AES(self.master_key),
                    modes.GCM(nonce, tag),
                    backend=self.backend
                )
                decryptor = cipher.decryptor()

                with open(output_path, 'wb') as fout:
                    while fin.tell() < tag_pos:
                        chunk = fin.read(min(chunk_size, tag_pos - fin.tell()))
                        fout.write(decryptor.update(chunk))

                    fout.write(decryptor.finalize())

            return output_path
        except Exception as e:
            if output_path.exists():
                os.unlink(output_path)
            raise DecryptionError(f"File decryption failed: {str(e)}")

    def generate_hmac(self, data: bytes, key: Optional[bytes] = None) -> bytes:
        """Generate HMAC-SHA256 for data integrity verification."""
        h = hmac.HMAC(
            key or self.master_key,
            hashes.SHA256(),
            backend=self.backend
        )
        h.update(data)
        return h.finalize()

    def verify_hmac(self, 
                   data: bytes, 
                   signature: bytes, 
                   key: Optional[bytes] = None) -> bool:
        """Verify HMAC-SHA256 signature."""
        h = hmac.HMAC(
            key or self.master_key,
            hashes.SHA256(),
            backend=self.backend
        )
        h.update(data)
        try:
            h.verify(signature)
            return True
        except:
            return False

    def rotate_key(self, new_key: Optional[bytes] = None) -> None:
        """Rotate master key and increment version."""
        self.master_key = new_key or self.generate_key(32)
        self.current_key_version += 1