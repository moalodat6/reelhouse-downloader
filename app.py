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
from urllib.parse import urlparse, urljoin

import boto3
from boto3.s3.transfer import TransferConfig
from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from starlette.middleware.trustedhost import TrustedHostMiddleware

SERVICE_TOKEN = os.getenv("SERVICE_TOKEN", "")
ALLOW_HOSTS = [x.strip().lower().lstrip(".") for x in os.getenv("ALLOWED_SOURCE_HOSTS", "").split(",") if x.strip()]
MEDIA_HOSTS = [x.strip().lower().lstrip(".") for x in os.getenv("ALLOWED_MEDIA_HOSTS", "").split(",") if x.strip()] or ALLOW_HOSTS
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


def validate_url(source: str, allowed_hosts=None):
    try:
        u = urlparse(source.strip())
        host = (u.hostname or "").lower().rstrip(".")
        port = u.port
    except ValueError:
        raise HTTPException(400, "Invalid URL")
    if u.scheme != "https" or not host or u.username or u.password or (port not in (None, 443)):
        raise HTTPException(400, "Only public HTTPS links on the normal port are allowed")
    allowed = ALLOW_HOSTS if allowed_hosts is None else allowed_hosts
    if not allowed or not any(host == h or host.endswith("." + h) for h in allowed):
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


def inspect_url(url: str, referer: str | None = None):
    import yt_dlp
    opts = ydl_opts()
    if referer:
        opts["http_headers"] = {"Referer": referer}
    with yt_dlp.YoutubeDL(opts) as ydl:
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


# Browser-based discovery is only a FALLBACK for sources that yt-dlp cannot
# inspect directly. It does not defeat DRM, bot controls, logins or IP locks.
MEDIA_EXT = re.compile(r"\.(?:m3u8|mpd|mp4|webm)(?:$|[?#])", re.I)
MEDIA_TYPES = ("application/vnd.apple.mpegurl", "application/x-mpegurl", "application/dash+xml", "video/mp4", "video/webm")


# Controlled interaction: explicitly marked Play controls and video-server tabs only.
# Never click arbitrary page buttons, login/paywall controls, or offsite download links.
PLAY_CONTROLS = (
    'button[aria-label*="play" i]',
    '[role="button"][aria-label*="play" i]',
    'button[title*="play" i]',
    '.vjs-big-play-button',
    '.plyr__control--overlaid',
    '.jw-display-icon-container',
    '.jw-icon-display',
    '.dplayer-play-icon',
    '.fp-play',
    '.art-state',
    '.play-button',
    '[class~="play-icon"]',
    'button:has-text("Play Movie")',
    'button:has-text("Play Video")',
    'button:has-text("Play Now")',
    'button:has-text("Play")',
    '[role="button"]:has-text("Play")',
)
SERVER_CONTROLS = (
    '[role="tab"][data-server]',
    'button[data-server]',
    'button[data-embed]',
    'button[data-source]',
    '[class*="server" i] button',
    '[class*="server" i] [role="tab"]',
    '[class*="player-list" i] button',
    '[class*="source-list" i] button',
    '[class*="server" i] [role="button"]',
)


def _safe_click(frame, selectors, clicked: set[str]) -> bool:
    """Click one media control; never follow links that navigate off-page."""
    for selector in selectors:
        try:
            loc = frame.locator(selector)
            for index in range(min(loc.count(), 5)):
                item = loc.nth(index)
                key = f"{frame.url}|{selector}|{index}"
                if key in clicked or not item.is_visible(timeout=650):
                    continue
                # Do not follow outbound links, hidden ads, or clickable page overlays.
                meta = item.evaluate("""el => ({
                    tag: el.tagName.toLowerCase(),
                    aria: (el.getAttribute('aria-label') || '').toLowerCase(),
                    text: (el.innerText || '').trim().slice(0, 90).toLowerCase(),
                    href: el.getAttribute('href') || '',
                    disabled: Boolean(el.disabled),
                })""")
                if meta['disabled']:
                    continue
                if meta['tag'] == 'a' and not (
                    not meta['href'] or meta['href'].startswith('#')
                    or meta['href'].lower().replace(' ', '') in ('javascript:void(0)', 'javascript:;')
                ):
                    continue
                if any(x in meta['text'] for x in ('sign in', 'login', 'subscribe', 'pay now', 'report broken')):
                    continue
                rect = item.bounding_box(timeout=650)
                if not rect or rect['width'] < 12 or rect['height'] < 12:
                    continue
                clicked.add(key)
                try:
                    item.click(timeout=1800)
                    return True
                except Exception:
                    # Mark attempted elements so a deceptive overlay can't loop forever.
                    continue
        except Exception:
            continue
    return False


def _activate_video_elements(frame):
    """Start already-created media elements without requiring user gestures."""
    try:
        frame.evaluate("""() => document.querySelectorAll('video').forEach(v => {
            v.muted = true;
            v.play().catch(() => {});
        })""")
    except Exception:
        pass


def _dom_video_urls(frame) -> list[str]:
    """Read only real media/source elements, not arbitrary script text."""
    try:
        sources = frame.evaluate("""() => Array.from(document.querySelectorAll('video,source'))
            .flatMap(el => [el.currentSrc, el.src, el.getAttribute('src'),
                            el.getAttribute('data-src')])
            .filter(Boolean)""")
        return [urljoin(frame.url, s) for s in sources if isinstance(s, str)]
    except Exception:
        return []


def browser_discover(page_url: str) -> list[dict]:
    """Watch media traffic after loading, activating Play, and switching player tabs.

    Only finds accessible, unprotected HTTP media. Does not bypass DRM, login,
    CAPTCHA, IP checks, or access restrictions. Never triggers download links.
    """
    from playwright.sync_api import sync_playwright

    found: dict[str, str] = {}
    host_cache = {}
    clicked_play: set[str] = set()
    clicked_server: set[str] = set()

    def remember(url, content_type='', referer=''):
        if len(found) >= 24 or not isinstance(url, str) or not url.startswith('https://'):
            return
        if MEDIA_EXT.search(urlparse(url).path) or any(t in content_type.lower() for t in MEDIA_TYPES):
            if url not in found or (referer and not found[url]):
                found[url] = referer if isinstance(referer, str) and referer.startswith('https://') else ''

    def check_public(url):
        try:
            u = urlparse(url)
            if u.scheme in ('data', 'blob', 'about'):
                return True
            if u.scheme != 'https' or not u.hostname or u.username or u.password or u.port not in (None, 443):
                return False
            host = u.hostname.lower().rstrip('.')
            if host in host_cache:
                return host_cache[host]
            try:
                good = is_public_ip(host)
            except ValueError:
                good = host not in ('localhost',) and not host.endswith(('.local','.internal','.localhost'))
                if good:
                    addrs = socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
                    good = bool(addrs) and all(is_public_ip(a[4][0]) for a in addrs)
            host_cache[host] = good
            return good
        except (ValueError, OSError):
            return False

    with sync_playwright() as play:
        browser = play.chromium.launch(headless=True, args=['--disable-dev-shm-usage'])
        try:
            context = browser.new_context(
                service_workers='block', ignore_https_errors=False,
                accept_downloads=False, java_script_enabled=True,
            )
            context.set_default_timeout(1800)

            def safe_route(route):
                try:
                    if check_public(route.request.url):
                        route.continue_()
                    else:
                        route.abort()
                except Exception:
                    try:
                        route.abort()
                    except Exception:
                        pass

            context.route('**/*', safe_route)
            # Collect stream URLs from network traffic in the page and its frames.
            context.on('request', lambda req: remember(
                req.url, referer=req.headers.get('referer', '')
            ))
            context.on('response', lambda res: remember(
                res.url, res.headers.get('content-type', '') if res.status in (200,206) else '',
                res.request.headers.get('referer', '')
            ))
            page = context.new_page()
            page.on('dialog', lambda dialog: dialog.dismiss())

            # Close advertising pop-ups; only the requested webpage is scanned.
            def close_popup(popup):
                if popup != page:
                    try:
                        popup.close()
                    except Exception:
                        pass

            context.on('page', close_popup)
            try:
                page.goto(page_url, wait_until='domcontentloaded', timeout=25000)
            except Exception:
                pass  # Partially loaded pages can still have working players.

            def frames():
                return page.frames[:12]

            def collect():
                for frame in frames():
                    for src in _dom_video_urls(frame):
                        remember(src, referer=frame.url)
                    _activate_video_elements(frame)

            collect()
            page.wait_for_timeout(1800)
            collect()

            # Stage 1: click explicit Play buttons on the page and within embeds.
            for frame in frames():
                if _safe_click(frame, PLAY_CONTROLS, clicked_play):
                    page.wait_for_timeout(1200)
                    collect()

            # Stage 2: activate up to four clearly identified source/server tabs.
            # Tabs can create new iframes asynchronously, so refresh frames each pass.
            if not found:
                switches = 0
                for _ in range(4):
                    changed = False
                    for frame in frames():
                        if switches >= 4:
                            break
                        if _safe_click(frame, SERVER_CONTROLS, clicked_server):
                            changed = True
                            switches += 1
                            page.wait_for_timeout(900)
                            collect()
                            clicked_play.clear()  # new server may have a new Play overlay
                            for subframe in frames():
                                if _safe_click(subframe, PLAY_CONTROLS, clicked_play):
                                    page.wait_for_timeout(900)
                                    collect()
                    if not changed or found:
                        break

            # Final window for player initialization and asynchronous manifests.
            page.wait_for_timeout(2200)
            collect()
            context.close()
        finally:
            browser.close()
    return [{"url": url, "referer": ref or page_url} for url, ref in found.items()]


def browser_choices(page_url: str) -> list[dict]:
    candidates = browser_discover(page_url)
    if not candidates:
        raise ValueError("No accessible MP4/HLS/DASH video streams were detected in this webpage or its frames")

    accepted = []
    unapproved = set()
    for candidate in candidates:
        media_url = candidate["url"] if isinstance(candidate, dict) else candidate
        referer = candidate.get("referer", page_url) if isinstance(candidate, dict) else page_url
        host = urlparse(media_url).hostname or ""
        if not any(host == h or host.endswith("." + h) for h in MEDIA_HOSTS):
            unapproved.add(host)
            continue
        try:
            validate_url(media_url, MEDIA_HOSTS)
        except HTTPException:
            continue
        accepted.append((media_url, referer))
    if not accepted:
        hosts = ", ".join(sorted(unapproved)[:3])
        raise ValueError("Video streams were detected on other host(s): " + hosts + ". Add only trusted, authorized hosts to ALLOWED_MEDIA_HOSTS")

    all_choices = []
    for media_url, referer in accepted[:4]:
        try:
            info = inspect_url(media_url, referer=referer)
            for choice in pick_choices(info):
                choice = dict(choice)
                # IDs are opaque; the actual source and format are stored server-side.
                choice["_download_url"] = media_url
                choice["_download_format"] = choice["id"]
                choice["_referer"] = referer
                choice["id"] = uuid.uuid4().hex
                all_choices.append(choice)
        except Exception:
            continue
    if not all_choices:
        raise ValueError("Detected video streams could not be inspected. They may be expired, blocked or protected")
    all_choices.sort(key=lambda x: x["height"], reverse=True)
    # Show one usable stream per resolution, retaining the best quality.
    seen_heights = set()
    unique = []
    for item in all_choices:
        key = (item["height"], item["label"])
        if key not in seen_heights:
            unique.append(item)
            seen_heights.add(key)
    return unique


@app.get("/health")
def health():
    return {"ok": True, "scanner": "player-activation-v3"}


@app.post("/scan", dependencies=[Depends(authorized)])
def scan(data: ScanInput):
    url = validate_url(data.url)
    media = {}
    direct_error = ""
    try:
        media = inspect_url(url)
        choices = pick_choices(media)
    except Exception as ex:
        direct_error = str(ex)[:180]
        choices = []
    if not choices:
        try:
            choices = browser_choices(url)
        except Exception as ex:
            raise HTTPException(422, f"Neither direct extraction nor browser scanning worked. Browser: {str(ex)[:250]}. Direct: {direct_error[:80]}")
    sid = str(uuid.uuid4())
    title = str(media.get("title") or "Untitled")[:250]
    poster = str(media.get("thumbnail") or "")[:2048]
    with LOCK, connect() as c:
        c.execute("DELETE FROM scans WHERE created<?", (time.time() - 3600,))
        c.execute("INSERT INTO scans VALUES(?,?,?,?,?,?)", (sid, url, json.dumps(choices), title, poster, time.time()))
    # Never return discovered media URLs or any internal backend fields to the browser.
    public_choices = [{k: v for k, v in row.items() if not k.startswith("_")} for row in choices]
    return {"scanId": sid, "title": title, "poster": poster, "description": str(media.get("description") or "")[:5000], "suggestedCategories": (media.get("categories") or [])[:20], "qualities": public_choices}


@app.post("/import", dependencies=[Depends(authorized)])
def start_import(data: ImportInput):
    if not (SERVICE_TOKEN and ACCOUNT_ID and BUCKET and os.getenv("R2_ACCESS_KEY_ID") and os.getenv("R2_SECRET_ACCESS_KEY")):
        raise HTTPException(503, "R2 credentials are not configured")
    with connect() as c:
        scan_row = c.execute("SELECT * FROM scans WHERE id=? AND created>?", (data.scan_id, time.time() - 3600)).fetchone()
    if not scan_row:
        raise HTTPException(404, "Scan expired; scan the webpage again")
    choices = json.loads(scan_row["choices"])
    selected = next((x for x in choices if x["id"] == data.choice_id), None)
    if selected is None:
        raise HTTPException(400, "Choose a resolution returned by the scan")
    target_url = selected.get("_download_url", scan_row["url"])
    format_id = selected.get("_download_format", data.choice_id)
    if selected.get("_download_url"):
        validate_url(target_url, MEDIA_HOSTS)
    job_id = str(uuid.uuid4())
    meta = data.metadata.model_dump()
    with LOCK, connect() as c:
        c.execute("INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?)", (
            job_id, "queued", 0.0, "In queue", None, json.dumps(meta), time.time(), time.time(),
        ))
    EXECUTOR.submit(process_job, job_id, target_url, format_id, selected.get("_referer"))
    return {"jobId": job_id, "state": "queued"}


@app.get("/jobs/{job_id}", dependencies=[Depends(authorized)])
def status(job_id: str):
    return get_job(job_id)


def process_job(job_id: str, url: str, fmt_id: str, referer: str | None = None):
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
        if referer:
            opts["http_headers"] = {"Referer": referer}
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
