"""Analyze X-Codex-Turn-State samples: decode base64, inspect the Fernet
layout and extract the embedded metadata (e.g. the timestamp).

Both samples are synthetic tokens with the same byte layout as the production
values; no captured data is contained in this file.
"""

import base64
import struct
from datetime import datetime, timezone

VALUES = {
    "sample A (synthetic)": "gAAAAABmghDAxGKf1Sdzgjn12nAPCycdxfMcxibA5Z_oOPJv54SBDALTLi1Mca2D_6kI2oh-XKV1F_qy5G31xyGw3uHSu5jkSXCYpnxzOR0pITyVdhXH9fGRyIfR114r-wTxrEnfsZ_srOsLj_JtnzYgr4e7HYL-biRaGfjHr0VwnNrMYwZ80Is5cmab6VptKJJDPKzHA5jkYUy9ZhCeOIeCk0j0PxhAj2I2akAl4CIoqICctbVjtQQpUINSwaKo6UPWlWWQxMptnyybojsrsHVhqap07KlsCg==",
    "sample B (synthetic)": "gAAAAABmghGBmYaclH8OS9klHHtxV66BFRs90YCwVvyhGekc94ZKb8ykKmVcL0Oi5t7l6rXCV4UZuSTEIJtZDdMLLxSrYWCICmEC7Ak1NZJgVQM4cKr_GrmMMH6PZ_vdOzagz_s6UfFH-uHuW3DMgmFtS2GeFdISSKPc9_mrGjv0ivTrd-TCZMSn1wO1p_Hf9S4lF76BVWe2Si70_Zd5QQvn2bazHvUfKc8AIF6HEndjCJNwec577I6HhQ0z2VmRsLkjvZehVq3lo5rbmXJiaE_Sz3XFeYsndA==",
}

for name, value in VALUES.items():
    raw = base64.urlsafe_b64decode(value)
    version = raw[0]
    timestamp = struct.unpack(">Q", raw[1:9])[0]
    generated = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    iv = raw[9:25]
    mac = raw[-32:]
    cipher = raw[25:-32]
    printable = sum(1 for byte in cipher if 32 <= byte < 127)
    print(f"== {name}")
    print(f"   encoded length: {len(value)} chars | decoded length: {len(raw)} bytes")
    print(f"   version byte: 0x{version:02x}")
    print(f"   timestamp: {timestamp} -> {generated.isoformat()}")
    print(f"   iv (16B): {iv.hex()}")
    print(f"   ciphertext: {len(cipher)} bytes | printable ratio: {printable/len(cipher):.3f}")
    print(f"   hmac: {len(mac)} bytes")
    print(f"   ciphertext head: {cipher[:32].hex()}")
    print()
