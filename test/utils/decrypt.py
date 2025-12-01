#!/usr/bin/env python3

import os
import subprocess
import sys

def decrypt_file(key_file, encrypted_file, output_file):
    """
    Decrypt a file using AES-256-CBC.
    Extracts IV from first 16 bytes of encrypted file and removes it.

    Args:
        key_file: Path to file containing the encryption key
        encrypted_file: Path to encrypted input file (will be modified)
        output_file: Path to decrypted output file
    """
    try:
        with open(key_file, 'rb') as f:
            key = f.read()

        # Read encrypted file
        with open(encrypted_file, 'rb') as f:
            data = f.read()

        if len(data) < 16:
            print("Error: Encrypted file is too small to contain IV")
            return False

        # Extract IV from first 16 bytes
        iv = data[:16]
        encrypted_data = data[16:]

        # Write encrypted_data to a temp file
        tmp_file = encrypted_file + '.tmp'
        with open(tmp_file, 'wb') as f:
            f.write(encrypted_data)

        print(f"Removed IV from {tmp_file}")

        print("KEY:", key.hex())

       # Call openssl enc subprocess
        subprocess.run([
            'openssl', 'enc', '-aes-256-cbc', '-d',
            '-K', key.hex(),
            '-iv', iv.hex(),
            '-in', tmp_file,
            '-out', output_file,
            '-nosalt'
        ], check=True)

        # Delete tmp_file
        os.remove(tmp_file)

        print(f"Decryption successful: {output_file}")
        return True
    except Exception as e:
        print(f"Error: {e}")
        return False

def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <encrypted_file.aes>")
        sys.exit(1)

    encrypted_file = sys.argv[1]
    key_file = encrypted_file + ".key"

    if not os.path.isfile(key_file):
        print(f"Error: Key file not found: {key_file}")
        sys.exit(1)

    if not os.path.isfile(encrypted_file):
        print(f"Error: Encrypted file not found: {encrypted_file}")
        sys.exit(1)

    if not encrypted_file.endswith('.aes'):
        print("Error: Encrypted file must have a .aes extension")
        sys.exit(1)

    output_file = encrypted_file[:-4]  # Remove .aes extension for output

    if decrypt_file(key_file, encrypted_file, output_file):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
