"""Reelhouse cloud-side video import service. Only use for authorized, unprotected media."""
from __future__ import annotations

import ipaddress
import json
import os
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import boto3
from boto3.s3.transfer import TransferConfig
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

SERVICE_TOKEN = os.getenv("SERVICE_TOKEN", "")
ALLOW_HOSTS = [x.strip().lower().lstrip(".") for x in os.getenv("ALLOWED_SOURCE_HOSTS", "").split(",") if x.strip()]
ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
BUCKET = os.getenv("R2_BUCKET", "")
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DB = DATA_DIR / "imports.sqlite3"
MAX_GB = float(os.getenv("MAX_VIDEO_GB", "10"))
R2_PREFIX = os.getenv("R2_PREFIX", "videos/imported").strip("/")
EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="video-import")
LOCK = threading.RLock()
app = FastAPI(title="Reelhouse Import Processor", docs_url=None, redoc_url=None)


class ScanInput(BaseModel):
    url: str = Field(min_length=10, max_length=2048)


class MovieMetadata(BaseModel):
    title: str = Field(min_length=1, max_length=250)
    categories: list[str] = Field(default_factory=list, max_length=20)
    year: int | None = Field(default=None, ge=1888, le=2100)
    description: str = Field(default="", max_length=5000)
    poster: str = Field(default="", max_length=2048)


class ImportInput(BaseModel):
    scan_id: str
    choice_id: str
    metadata: MovieMetadata


def authorized(request: Request):
    if not SERVICE_TOKEN or not secrets.compare_digest(request.headers.get("x-service-token", ""), SERVICE_TOKEN):
        raise HTTPException(401, "Unauthorized")


def is_public_ip(ip: str) -> bool:
    obj = ipaddress.ip_address(ip)
    return obj.is_global


def validate_url(source: str):
    try:
        u = urlparse(source.strip())
        host = (u.hostname or "").lower().rstrip(".")
        port = u.port
    except ValueError:
        raise HTTPException(400, "Invalid URL")
    if u.scheme != "https" or not host or u.username or u.password or (port not in (None, 443)):
        raise HTTPException(400, "Only public HTTPS links on the normal port are allowed")
    if not ALLOW_HOSTS or not any(host == h or host.endswith("." + h) for h in ALLOW_HOSTS):
        raise HTTPException(400, "This source domain is not on the server's allowlist")
    if host == "localhost" or host.endswith((".local", ".internal", ".localhost")):
        raise HTTPException(400, "Private hosts are not allowed")
    try:
        if not is_public_ip(host):
            raise HTTPException(400, "IP addresses are not supported")
    except ValueError:
        pass
    # Check the initial host. Follow-on CDN hosts must additionally be isolated by an
    # outbound network firewall when deploying: yt-dlp is an external-network client.
    try:
        addresses = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        if not addresses or any(not is_public_ip(x[4][0]) for x in addresses):
            raise HTTPException(400, "Source hostname does not resolve to public IPs")
    except OSError:
        raise HTTPException(400, "Could not resolve the source hostname")
    return source.strip()


def connect():
    conn = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def initialize():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with connect() as c:
        c.execute("CREATE TABLE IF NOT EXISTS scans (id TEXT PRIMARY KEY, url TEXT NOT NULL, choices TEXT NOT NULL, title TEXT, poster TEXT, created REAL NOT NULL)")
        c.execute("CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, state TEXT NOT NULL, progress REAL NOT NULL, message TEXT, r2_key TEXT, metadata TEXT, created REAL NOT NULL, updated REAL NOT NULL)")
        c.execute("UPDATE jobs SET state='failed',message='Processor restarted; retry this import',updated=? WHERE state IN ('queued','downloading','processing','uploading')", (time.time(),))


initialize()


def set_job(job_id: str, **updates):
    updates["updated"] = time.time()
    with LOCK, connect() as c:
        values = list(updates.values())
        c.execute("UPDATE jobs SET " + ",".join(k + "=?" for k in updates) + " WHERE id=?", values + [job_id])


def get_job(job_id: str):
    with connect() as c:
        row = c.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    return {"job_id": row["id"], "state": row["state"], "progress": row["progress"], "message": row["message"], "r2Key": row["r2_key"], "metadata": json.loads(row["metadata"] or "{}")}


def ydl_opts():
    return {
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "socket_timeout": 25, "retries": 2, "fragment_retries": 2,
        "skip_download": True,
        "ignoreerrors": False,
        "extract_flat": False,
        "cachedir": False,
        "max_filesize": int(MAX_GB * 1024**3),
    }


def inspect_url(url: str):
    import yt_dlp
    with yt_dlp.YoutubeDL(ydl_opts()) as ydl:
        data = ydl.extract_info(url, download=False)
    if not data:
        raise ValueError("No downloadable media detected")
    if data.get("is_live") or data.get("live_status") == "is_live":
        raise ValueError("Live streams are not supported")
    if data.get("_type") in ("playlist", "multi_video"):
        raise ValueError("Paste a single video page, not a playlist")
    return data


def pick_choices(data: dict):
    formats = data.get("formats") or ([data] if data.get("url") else [])
    groups: dict[str, list[dict]] = {}
    for f in formats:
        if f.get("vcodec") == "none" or f.get("has_drm") or not f.get("format_id"):
            continue
        height = f.get("height")
        if isinstance(height, int) and height > 0:
            label = f"{height}p"
        else:
            label = "Source (resolution unknown)"
        groups.setdefault(label, []).append(f)
    choices = []
    for label, pool in groups.items():
        # Favor a H.264 rendition at that resolution, otherwise use the best available.
        pool.sort(key=lambda f: (
            str(f.get("vcodec", "")).startswith(("avc1", "h264")),
            f.get("ext") == "mp4", f.get("tbr") or 0,
        ), reverse=True)
        selected = pool[0]
        audio_present = selected.get("acodec") not in (None, "none")
        choices.append({
            "id": selected["format_id"], "label": label,
            "height": selected.get("height") or 0,
            "extension": selected.get("ext") or "unknown",
            "hasAudio": audio_present,
            "codec": (selected.get("vcodec") or "unknown")[:48],
            "estimatedMB": round(float(selected.get("filesize") or selected.get("filesize_approx") or 0) / 1024**2) or None,
        })
    choices.sort(key=lambda x: x["height"], reverse=True)
    return choices


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/scan", dependencies=[Depends(authorized)])
def scan(data: ScanInput):
    url = validate_url(data.url)
    try:
        media = inspect_url(url)
        choices = pick_choices(media)
    except Exception as ex:
        raise HTTPException(422, f"Scanning failed: {str(ex)[:350]}")
    if not choices:
        raise HTTPException(422, "The webpage did not expose any supported downloadable video quality")
    sid = str(uuid.uuid4())
    title = str(media.get("title") or "Untitled")[:250]
    poster = str(media.get("thumbnail") or "")[:2048]
    with LOCK, connect() as c:
        c.execute("DELETE FROM scans WHERE created<?", (time.time() - 3600,))
        c.execute("INSERT INTO scans VALUES(?,?,?,?,?,?)", (sid, url, json.dumps(choices), title, poster, time.time()))
    return {"scanId": sid, "title": title, "poster": poster, "description": str(media.get("description") or "")[:5000], "suggestedCategories": (media.get("categories") or [])[:20], "qualities": choices}


@app.post("/import", dependencies=[Depends(authorized)])
def start_import(data: ImportInput):
    if not (SERVICE_TOKEN and ACCOUNT_ID and BUCKET and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY")):
        raise HTTPException(503, "R2 credentials are not configured")
    with connect() as c:
        scan_row = c.execute("SELECT * FROM scans WHERE id=? AND created>?", (data.scan_id, time.time() - 3600)).fetchone()
    if not scan_row:
        raise HTTPException(404, "Scan expired; scan the webpage again")
    choices = json.loads(scan_row["choices"])
    if data.choice_id not in [x["id"] for x in choices]:
        raise HTTPException(400, "Choose a resolution returned by the scan")
    job_id = str(uuid.uuid4())
    meta = data.metadata.model_dump()
    with LOCK, connect() as c:
        c.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)", (
            job_id, "queued", 0.0, "In queue", None, json.dumps(meta), time.time(), time.time(),
        ))
    EXECUTOR.submit(process_job, job_id, scan_row["url"], data.choice_id)
    return {"jobId": job_id, "state": "queued"}


@app.get("/jobs/{job_id}", dependencies=[Depends(authorized)])
def status(job_id: str):
    return get_job(job_id)


def process_job(job_id: str, url: str, fmt_id: str):
    """Download and package cloud-side, then upload via R2 multipart."""
    import yt_dlp
    workdir = Path(tempfile.mkdtemp(prefix="reelhouse-", dir=str(DATA_DIR)))
    key = f"{R2_PREFIX}/{job_id}.mp4" if R2_PREFIX else f"{job_id}.mp4"
    try:
        set_job(job_id, state="downloading", message="Downloading selected quality", progress=1)

        def progress(d):
            if d.get("status") == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes") or 0
                if total:
                    set_job(job_id, progress=round(min(70, 70 * downloaded / total), 1), message="Downloading selected quality")

        opts = ydl_opts()
        opts.update({
            "skip_download": False,
            # A video-only 1080p rendition is combined with the best available audio.
            "format": f"{fmt_id}+bestaudio/{fmt_id}",
            "merge_output_format": "mkv",
            "outtmpl": str(workdir / "source.%(ext)s"),
            "progress_hooks": [progress],
            "nopart": False,
            "overwrites": True,
        })
        with yt_dlp.YoutubeDL(opts) as ydl:
            ydl.download([url])
        source_files = [f for f in workdir.iterdir() if f.is_file() and not f.name.endswith((".part", ".ytdl"))]
        if not source_files:
            raise RuntimeError("No file resulted from the selected download")
        source = max(source_files, key=lambda x: x.stat().st_size)
        if source.stat().st_size > MAX_GB * 1024**3:
            raise RuntimeError("File exceeds configured size limit")
        set_job(job_id, state="processing", message="Preparing browser-compatible MP4", progress=72)
        final = workdir / "final.mp4"
        probe = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "stream=codec_type,codec_name", "-of", "json", str(source),
        ], capture_output=True, text=True, timeout=60, check=True)
        streams = json.loads(probe.stdout).get("streams", [])
        vcodec = next((s["codec_name"] for s in streams if s["codec_type"] == "video"), "")
        acodec = next((s["codec_name"] for s in streams if s["codec_type"] == "audio"), "")
        if not vcodec:
            raise RuntimeError("The downloaded file does not contain video")
        cmd = ["ffmpeg", "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", str(source),
               "-map", "0:v:0", "-map", "0:a:0?", "-c:v", "copy" if vcodec == "h264" else "libx264"]
        if vcodec != "h264":
            cmd += ["-preset", "veryfast", "-crf", "22", "-pix_fmt", "yuv420p"]
        cmd += ["-c:a", "copy" if not acodec or acodec == "aac" else "aac", "-movflags", "+faststart", str(final)]
        subprocess.run(cmd, check=True, timeout=60 * 60 * 6)
        if final.stat().st_size > MAX_GB * 1024**3:
            raise RuntimeError("Processed file exceeds configured size limit")
        set_job(job_id, state="uploading", message="Uploading into Cloudflare R2", progress=81)
        s3 = boto3.client(
            "s3", endpoint_url=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"], region_name="auto",
        )
        size = final.stat().st_size
        uploaded = 0
        update_guard = threading.Lock()

        def on_upload(n):
            nonlocal uploaded
            with update_guard:
                uploaded += n
                set_job(job_id, progress=round(81 + 18 * min(1, uploaded / max(size, 1)), 1), message="Uploading into Cloudflare R2")

        s3.upload_file(str(final), BUCKET, key,
                       ExtraArgs={"ContentType": "video/mp4"}, Callback=on_upload,
                       Config=TransferConfig(multipart_threshold=32 * 1024**2, multipart_chunksize=32 * 1024**2, max_concurrency=4))
        with connect() as c:
            rec = c.execute("SELECT metadata FROM jobs WHERE id=?", (job_id,)).fetchone()
        s3.put_object(Bucket=BUCKET, Key=key[:-4] + '.import.json', Body=json.dumps({
            'videoIds': [key], 'r2Key': key, 'metadata': json.loads(rec['metadata']), 'sourcePage': url
        }).encode('utf-8'), ContentType='application/json')
        set_job(job_id, state="complete", message="Video imported into R2; attach to catalog", progress=100, r2_key=key)
    except Exception as ex:
        set_job(job_id, state="failed", message=f"Import failed: {str(ex)[:380]}")
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
