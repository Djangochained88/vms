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

    def register(self, codec_id: int, name: str, default_keyframe: int, max_kbps: int) -> None:
        self._entries[codec_id] = {
            "name": name,
            "default_keyframe": default_keyframe,
            "max_kbps": max_kbps,
        }

    def list_codecs(self) -> list[tuple[int, str]]:
        return [(cid, info["name"]) for cid, info in self._entries.items()]


# ---------------------------------------------------------------------------
# Tier manager (bitrate tiers)
# ---------------------------------------------------------------------------

class TierManager:
    """Manages bitrate tiers for adaptive streaming."""

    def __init__(self, max_tiers: int = DEFAULT_MAX_TIERS) -> None:
        self._max_tiers = max_tiers
        self._tier_bitrate_kbps: list[int] = []
        self._rebuild_default_tiers()

    def _rebuild_default_tiers(self) -> None:
        base = [400, 800, 1200, 2400, 4800, 8000, 12000, 18000]
        self._tier_bitrate_kbps = (base + [22000, 25000])[: self._max_tiers]

    def get_tier_count(self) -> int:
        return len(self._tier_bitrate_kbps)

    def get_bitrate_for_tier(self, tier_index: int) -> int:
        if 0 <= tier_index < len(self._tier_bitrate_kbps):
            return self._tier_bitrate_kbps[tier_index]
        return 0

    def set_tier_bitrate(self, tier_index: int, kbps: int) -> bool:
        if tier_index < 0 or tier_index >= self._max_tiers:
            return False
        if kbps < MIN_BITRATE_KBPS or kbps > MAX_BITRATE_KBPS:
            return False
        while len(self._tier_bitrate_kbps) <= tier_index:
            self._tier_bitrate_kbps.append(MIN_BITRATE_KBPS)
        self._tier_bitrate_kbps[tier_index] = kbps
        return True

    def iter_tiers(self) -> Iterator[tuple[int, int]]:
        for i, kbps in enumerate(self._tier_bitrate_kbps):
            yield i, kbps


# ---------------------------------------------------------------------------
# Profile store
# ---------------------------------------------------------------------------

class ProfileStore:
    """In-memory store of compression profiles keyed by profile hash."""

    def __init__(self, max_profiles: int = DEFAULT_MAX_PROFILES) -> None:
        self._max_profiles = max_profiles
        self._by_hash: dict[str, CompressionProfile] = {}
        self._order: list[str] = []

    def add(self, profile: CompressionProfile) -> bool:
        key = self._profile_key(profile)
        if key in self._by_hash:
            return False
        if len(self._by_hash) >= self._max_profiles:
            return False
        self._by_hash[key] = profile
        self._order.append(key)
        return True

    def _profile_key(self, profile: CompressionProfile) -> str:
        raw = f"{profile.max_bitrate_kbps}:{profile.keyframe_interval}:{profile.codec_id}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def get(self, profile_hash: str) -> Optional[CompressionProfile]:
        return self._by_hash.get(profile_hash)

    def deactivate(self, profile_hash: str) -> bool:
        p = self._by_hash.get(profile_hash)
        if p is None or not p.active:
            return False
        p.active = False
        return True

    def active_count(self) -> int:
        return sum(1 for p in self._by_hash.values() if p.active)

    def list_active(self) -> list[CompressionProfile]:
        return [p for p in self._by_hash.values() if p.active]


# ---------------------------------------------------------------------------
# Job queue
# ---------------------------------------------------------------------------

class JobQueue:
    """Queue of encode jobs with cooldown and content-hash deduplication."""

    def __init__(
        self,
        job_slots_per_tier: int = DEFAULT_JOB_SLOTS_PER_TIER,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        max_tiers: int = DEFAULT_MAX_TIERS,
    ) -> None:
        self._job_slots = job_slots_per_tier
        self._cooldown = cooldown_seconds
        self._max_tiers = max_tiers
        self._jobs: dict[str, EncodeJob] = {}
        self._content_processed: set[str] = set()
        self._last_request_by_caller: dict[str, float] = {}
        self._slot_counter = 0

    def _next_slot_id(self) -> str:
        self._slot_counter += 1
        return f"slot_{self._slot_counter}_{uuid.uuid4().hex[:8]}"

    def can_schedule(self, caller_id: str, content_hash: str) -> tuple[bool, str]:
        if content_hash in self._content_processed:
            return False, "content_already_processed"
        last = self._last_request_by_caller.get(caller_id, 0.0)
        if time.time() - last < self._cooldown:
            return False, "cooldown_active"
        return True, ""

    def schedule(self, content_hash: str, tier_index: int, caller_id: str) -> Optional[EncodeJob]:
        ok, reason = self.can_schedule(caller_id, content_hash)
        if not ok:
            return None
        if tier_index < 0 or tier_index >= self._max_tiers:
            return None

        job_id = self._next_slot_id()
        job = EncodeJob(
            job_id=job_id,
            content_hash=content_hash,
            tier_index=tier_index,
            scheduled_at=time.time(),
            nonce=self._slot_counter,
            fulfilled=False,
        )
        self._jobs[job_id] = job
        self._last_request_by_caller[caller_id] = time.time()
        return job

    def fulfill(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.fulfilled:
            return False
        job.fulfilled = True
        job.fulfilled_at = time.time()
        self._content_processed.add(job.content_hash)
        return True

    def get_job(self, job_id: str) -> Optional[EncodeJob]:
        return self._jobs.get(job_id)

    def is_content_processed(self, content_hash: str) -> bool:
        return content_hash in self._content_processed

    def next_slot_number(self) -> int:
        return self._slot_counter + 1


# ---------------------------------------------------------------------------
# Main VMS compression engine
# ---------------------------------------------------------------------------

class VMSCompressionEngine:
    """
    Video management system: ties together profiles, tiers, and job queue
    for compression workflows.
    """

    def __init__(
        self,
        max_profiles: int = DEFAULT_MAX_PROFILES,
        job_slots_per_tier: int = DEFAULT_JOB_SLOTS_PER_TIER,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        max_tiers: int = DEFAULT_MAX_TIERS,
    ) -> None:
        self._codec_registry = CodecRegistry()
        self._tier_manager = TierManager(max_tiers=max_tiers)
        self._profile_store = ProfileStore(max_profiles=max_profiles)
        self._job_queue = JobQueue(
            job_slots_per_tier=job_slots_per_tier,
            cooldown_seconds=cooldown_seconds,
            max_tiers=max_tiers,
        )

    @property
    def codec_registry(self) -> CodecRegistry:
        return self._codec_registry

    @property
    def tier_manager(self) -> TierManager:
        return self._tier_manager

    @property
    def profile_store(self) -> ProfileStore:
        return self._profile_store

    @property
    def job_queue(self) -> JobQueue:
        return self._job_queue

    def register_profile(
        self,
        max_bitrate_kbps: int,
        keyframe_interval: int,
        codec_id: int,
    ) -> Optional[str]:
        profile = CompressionProfile(
            profile_id=uuid.uuid4().hex,
            max_bitrate_kbps=max_bitrate_kbps,
            keyframe_interval=keyframe_interval,
            codec_id=codec_id,
            active=True,
        )
        if not self._profile_store.add(profile):
            return None
        return self._profile_key(profile)

    def _profile_key(self, profile: CompressionProfile) -> str:
        raw = f"{profile.max_bitrate_kbps}:{profile.keyframe_interval}:{profile.codec_id}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def schedule_encode_job(
        self,
        content_hash: str,
        tier_index: int,
        caller_id: str = "default",
    ) -> Optional[EncodeJob]:
        return self._job_queue.schedule(content_hash, tier_index, caller_id)

    def fulfill_job(self, job_id: str) -> bool:
        return self._job_queue.fulfill(job_id)

    def deactivate_profile(self, profile_hash: str) -> bool:
        return self._profile_store.deactivate(profile_hash)

    def get_profile(self, profile_hash: str) -> Optional[dict[str, Any]]:
        p = self._profile_store.get(profile_hash)
        return p.to_dict() if p else None

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        j = self._job_queue.get_job(job_id)
        return j.to_dict() if j else None

    def list_tiers(self) -> list[tuple[int, int]]:
        return list(self._tier_manager.iter_tiers())

    def list_codecs(self) -> list[tuple[int, str]]:
        return self._codec_registry.list_codecs()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_bitrate_kbps(kbps: int) -> bool:
    return MIN_BITRATE_KBPS <= kbps <= MAX_BITRATE_KBPS


def validate_keyframe_interval(interval: int) -> bool:
    return 1 <= interval <= 600


def validate_tier_index(index: int, max_tiers: int) -> bool:
    return 0 <= index < max_tiers

