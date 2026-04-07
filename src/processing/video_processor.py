"""Download source video + re-encode with hash-breaking modifications.

Pipeline:
  1. yt-dlp -> storage/downloaded/{candidate_id}.mp4
  2. videohash of original (dedupe check vs video_hashes table)
  3. ffmpeg filter chain -> storage/processed/{candidate_id}.mp4
     - metadata strip
     - tiny speed change (video + audio)
     - 2px crop + rescale
     - mild color tweak
     - optional credit text overlay
  4. videohash of processed; write both rows to DB
"""
from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.models import Candidate, CandidateStatus, VideoHash


@dataclass
class ProcessedVideo:
    candidate_id: int
    source_path: Path
    processed_path: Path
    source_hash: str
    processed_hash: str


def _ensure_dirs() -> None:
    settings.download_dir.mkdir(parents=True, exist_ok=True)
    settings.processed_dir.mkdir(parents=True, exist_ok=True)


def download(candidate: Candidate) -> Path:
    _ensure_dirs()
    out = settings.download_dir / f"{candidate.id}.mp4"
    if out.exists():
        return out
    cmd = [
        "yt-dlp",
        "-f", "mp4/bestvideo*+bestaudio/best",
        "--merge-output-format", "mp4",
        "-o", str(out),
        candidate.source_url,
    ]
    logger.info("yt-dlp: {}", " ".join(cmd))
    subprocess.run(cmd, check=True)
    return out


def _hash_file(path: Path) -> str:
    """Perceptual video hash. Falls back to sha256 if videohash fails."""
    try:
        from videohash import VideoHash  # type: ignore

        vh = VideoHash(path=str(path))
        return vh.hash_hex
    except Exception as exc:
        logger.warning("videohash failed, falling back to sha256: {}", exc)
        import hashlib

        h = hashlib.sha256()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()


def modify(src: Path, dst: Path, credit: str | None = None) -> None:
    """Run ffmpeg with a hash-breaking filter chain."""
    vf_parts = [
        "crop=in_w-4:in_h-4",
        "scale=in_w:in_h",
        "eq=brightness=0.02:saturation=1.05",
        "setpts=0.97*PTS",
    ]
    if credit:
        safe = credit.replace("'", "").replace(":", "")
        vf_parts.append(
            f"drawtext=text='{safe}':fontcolor=white:fontsize=18:"
            f"box=1:boxcolor=black@0.4:boxborderw=6:x=(w-text_w)/2:y=h-60"
        )
    vf = ",".join(vf_parts)

    cmd = [
        "ffmpeg", "-y",
        "-i", str(src),
        "-map_metadata", "-1",
        "-vf", vf,
        "-af", "atempo=1.03",
        "-c:v", "libx264", "-crf", "23", "-preset", "medium",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        str(dst),
    ]
    logger.info("ffmpeg: {}", " ".join(cmd))
    subprocess.run(cmd, check=True)


def process_candidate(session: Session, candidate: Candidate) -> ProcessedVideo:
    _ensure_dirs()
    src = download(candidate)
    src_hash = _hash_file(src)

    existing = session.execute(
        select(VideoHash).where(VideoHash.hash == src_hash)
    ).scalar_one_or_none()
    if existing and existing.candidate_id != candidate.id:
        raise RuntimeError(f"duplicate of candidate {existing.candidate_id}")

    dst = settings.processed_dir / f"{candidate.id}.mp4"
    credit = f"credit @{candidate.author}" if candidate.author else None
    modify(src, dst, credit=credit)

    processed_hash = _hash_file(dst)

    session.add(VideoHash(hash=src_hash, candidate_id=candidate.id))
    if processed_hash != src_hash:
        session.add(VideoHash(hash=processed_hash, candidate_id=candidate.id))
    candidate.status = CandidateStatus.PROCESSED
    session.commit()

    return ProcessedVideo(
        candidate_id=candidate.id,
        source_path=src,
        processed_path=dst,
        source_hash=src_hash,
        processed_hash=processed_hash,
    )
