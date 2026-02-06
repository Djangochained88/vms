"""
Zeta-frame pipeline: video management system for adaptive bitrate compression.
Profiles, tiers, and encode jobs are managed locally; content is keyed by hash.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any, Iterator, Optional


# ---------------------------------------------------------------------------
# Constants and configuration (unique to this module)
# ---------------------------------------------------------------------------

DEFAULT_MAX_PROFILES = 64
DEFAULT_JOB_SLOTS_PER_TIER = 89
DEFAULT_COOLDOWN_SECONDS = 1123
DEFAULT_MAX_TIERS = 12
CODEC_H264_ID = 1
CODEC_H265_ID = 2
CODEC_VP9_ID = 3
CODEC_AV1_ID = 4
MIN_BITRATE_KBPS = 256
MAX_BITRATE_KBPS = 25000
DEFAULT_KEYFRAME_INTERVAL = 48


# ---------------------------------------------------------------------------
# Enums and data structures
# ---------------------------------------------------------------------------

class CodecKind(IntEnum):
    H264 = CODEC_H264_ID
    H265 = CODEC_H265_ID
    VP9 = CODEC_VP9_ID
    AV1 = CODEC_AV1_ID


@dataclass
class CompressionProfile:
    """Single compression profile: bitrate, keyframe interval, codec."""
    profile_id: str
    max_bitrate_kbps: int
    keyframe_interval: int
    codec_id: int
    active: bool = True
    created_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        return {
            "profile_id": self.profile_id,
            "max_bitrate_kbps": self.max_bitrate_kbps,
            "keyframe_interval": self.keyframe_interval,
            "codec_id": self.codec_id,
            "active": self.active,
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CompressionProfile:
        return cls(
            profile_id=data["profile_id"],
            max_bitrate_kbps=data["max_bitrate_kbps"],
            keyframe_interval=data["keyframe_interval"],
            codec_id=data["codec_id"],
            active=data.get("active", True),
            created_at=data.get("created_at", time.time()),
        )


@dataclass
class EncodeJob:
    """Encode job keyed by content hash and tier."""
    job_id: str
    content_hash: str
    tier_index: int
    scheduled_at: float
    nonce: int
    fulfilled: bool = False
    fulfilled_at: Optional[float] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "content_hash": self.content_hash,
            "tier_index": self.tier_index,
            "scheduled_at": self.scheduled_at,
            "nonce": self.nonce,
            "fulfilled": self.fulfilled,
            "fulfilled_at": self.fulfilled_at,
        }


# ---------------------------------------------------------------------------
# Content hashing
# ---------------------------------------------------------------------------

def content_hash_from_bytes(data: bytes) -> str:
    """Produce a deterministic content hash (hex) for raw bytes."""
    return hashlib.sha256(data).hexdigest()


def content_hash_from_string(s: str) -> str:
    """Produce content hash for a string (e.g. path or identifier)."""
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Codec registry
# ---------------------------------------------------------------------------

class CodecRegistry:
    """Maps codec IDs to names and default settings."""

    def __init__(self) -> None:
        self._entries: dict[int, dict[str, Any]] = {
            CODEC_H264_ID: {"name": "H.264", "default_keyframe": 48, "max_kbps": 20000},
            CODEC_H265_ID: {"name": "H.265", "default_keyframe": 96, "max_kbps": 25000},
            CODEC_VP9_ID: {"name": "VP9", "default_keyframe": 72, "max_kbps": 18000},
            CODEC_AV1_ID: {"name": "AV1", "default_keyframe": 120, "max_kbps": 22000},
        }

    def get_name(self, codec_id: int) -> str:
        return self._entries.get(codec_id, {}).get("name", "unknown")

    def get_default_keyframe_interval(self, codec_id: int) -> int:
        return self._entries.get(codec_id, {}).get("default_keyframe", 48)

    def get_max_kbps(self, codec_id: int) -> int:
        return self._entries.get(codec_id, {}).get("max_kbps", 25000)

