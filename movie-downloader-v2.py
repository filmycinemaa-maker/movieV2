#!/usr/bin/env python3
"""
BEAM Movie Downloader v2 (NO SPLIT) — GitHub Actions Pipeline
=================================================================
Same Queue/Archive sheet layout as the split version, same subtitle
pipeline (extract English tracks -> host on Internet Archive -> attach to
Vidara via API) — the ONLY thing this version does differently is that it
does NOT split multi-audio files by language. Each link produces exactly
ONE uploaded file containing every audio track already in it, and the
filename records every language found, e.g.:

    Dark Knight (2008) 1080p Eng Tel Hin Tam.mkv

For every row, for every quality (1080p/720p/480p):
    - Cell contains one or more links (one per line).
    - For each link, in order:
        - Download the file (source URLs have no extension — the real
          container format is detected AFTER download via MediaInfo's
          General track, not guessed from the URL).
        - Detect every audio track's language (never guessed — unidentified
          stays "Unknown") and every subtitle track (unknown -> English).
        - Extract + host every English subtitle track on Internet Archive,
          then upload the file AS-IS (no re-encode, no re-mux) to Vidara,
          then attach each hosted subtitle to that single filecode.
        - beam_upsert() records the FULL list of languages found.
        - Delete the local file immediately after upload.
    - No cross-link duplicate-language memory exists in this version —
      splitting is gone, so there's nothing to skip. Each link is fully
      independent.
    - Sheet writes happen only at checkpoints, same as the split version:
        quality start   -> DOWNLOAD_STATUS_xxxx = Running
        quality success -> DOWNLOAD_STATUS_xxxx = Done, ERROR_xxxx cleared
                            (or a subtitle-warning note if a caption failed
                            to attach, without failing the row)
        quality failure -> DOWNLOAD_STATUS_xxxx = Failed, ERROR_xxxx = details
"""

import os
import re
import json
import shutil
import requests
import subprocess
import time
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from pymediainfo import MediaInfo
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

# ============================================================================
# CONFIGURATION
# ============================================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

VIDARA_API_KEY = os.environ.get("VIDARA_API_KEY", "").strip()

# Internet Archive S3-style credentials, used to host extracted English
# subtitles so Vidara can fetch them by direct URL.
# SECURITY NOTE: swap these for a GitHub Secret (IA_ACCESS_KEY /
# IA_SECRET_KEY, same pattern as VIDARA_API_KEY above) before running this
# long-term — anyone with read access to this file/repo gets full write
# access to your IA account with these sitting here in plain text.
IA_ACCESS_KEY = os.environ.get("IA_ACCESS_KEY", "EQ6XJ3AACbxfK4n7").strip()
IA_SECRET_KEY = os.environ.get("IA_SECRET_KEY", "BlzN7vT0uJo7g3n2").strip()

RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()

BEAM_WORKER_URL = "https://beamplay.beam-api.workers.dev"
ADMIN_EMAIL = os.environ.get("BEAM_ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.environ.get("BEAM_ADMIN_PASSWORD", "")

TEMP_FOLDER = "./temp_downloads"
os.makedirs(TEMP_FOLDER, exist_ok=True)

LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
}

UNKNOWN_TOKENS = {"", "und", "unknown", "unk", "n/a", "none"}

# Container format (MediaInfo's General->Format field) -> file extension.
# Detected AFTER download since source URLs carry no extension.
FORMAT_TO_EXTENSION = {
    "Matroska": "mkv",
    "MPEG-4": "mp4",
    "AVI": "avi",
    "QuickTime": "mov",
    "WebM": "webm",
    "Flash Video": "flv",
    "MPEG-PS": "mpg",
    "MPEG-TS": "ts",
}
DEFAULT_EXTENSION = "mkv"  # matches this pipeline's overwhelming majority

CONTENT_TYPE_BY_EXT = {
    "mkv": "video/x-matroska",
    "mp4": "video/mp4",
    "avi": "video/x-msvideo",
    "mov": "video/quicktime",
    "webm": "video/webm",
    "flv": "video/x-flv",
    "ts": "video/mp2t",
    "mpg": "video/mpeg",
}


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_audio_lang(raw_code, raw_name=None):
    """Audio must NEVER guess. Unrecognized/blank stays 'Unknown'."""
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "Unknown"


def normalize_subtitle_lang(raw_code, raw_name=None):
    """Subtitles: unknown/blank/und collapses to English."""
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "English"


# ============================================================================
# MEDIAINFO
# ============================================================================

def inspect_tracks(file_path):
    """
    Returns:
        audio_tracks    = [ { "stream_index": int, "language": "English" }, ... ]
        subtitle_tracks = [ { "stream_index": int, "language": "English" }, ... ]
    """
    media = MediaInfo.parse(str(file_path))
    audio_tracks, subtitle_tracks = [], []
    audio_pos, sub_pos = 0, 0

    for track in media.tracks:
        if track.track_type == "Audio":
            lang = normalize_audio_lang(track.language, getattr(track, "language_full", None))
            audio_tracks.append({"stream_index": audio_pos, "language": lang})
            audio_pos += 1
        elif track.track_type == "Text":
            lang = normalize_subtitle_lang(track.language, getattr(track, "language_full", None))
            subtitle_tracks.append({"stream_index": sub_pos, "language": lang})
            sub_pos += 1

    if not audio_tracks:
        audio_tracks = [{"stream_index": 0, "language": "Unknown"}]

    return audio_tracks, subtitle_tracks


def detect_container_extension(file_path):
    """
    Reads the ACTUAL container format from the file's bytes (MediaInfo's
    General track), since the source URL has no extension to go by.
    Falls back to .mkv (the overwhelming majority in this pipeline) for
    anything unrecognized, rather than failing the whole file over it.
    """
    try:
        media = MediaInfo.parse(str(file_path))
        for track in media.tracks:
            if track.track_type == "General":
                fmt = (track.format or "").strip()
                ext = FORMAT_TO_EXTENSION.get(fmt)
                if ext:
                    return ext, fmt
                print(f"    [WARN] Unrecognized container format '{fmt}', defaulting to .{DEFAULT_EXTENSION}")
                return DEFAULT_EXTENSION, fmt
    except Exception as e:
        print(f"    [WARN] Could not detect container format ({e}), defaulting to .{DEFAULT_EXTENSION}")
    return DEFAULT_EXTENSION, "Unknown"


def dedupe_languages(audio_tracks):
    """Unique languages, preserving the order MediaInfo reported them in."""
    seen = set()
    langs = []
    for t in audio_tracks:
        if t["language"] not in seen:
            seen.add(t["language"])
            langs.append(t["language"])
    return langs or ["Unknown"]


# ============================================================================
# NAMING / VIDARA / BEAM / DOWNLOAD
# ============================================================================

def clean_string_for_vidara(text):
    if not text:
        return ""
    text = text.replace(".", "")
    text = text.replace("/", "-")
    text = re.sub(r'[:*?"<>|]', "", text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def build_filename(tmdb_name, year, quality, languages, ext):
    clean_title = clean_string_for_vidara(tmdb_name)
    lang_str = " ".join(l[:3] for l in languages)
    if year:
        return f"{clean_title} ({year}) {quality} {lang_str}.{ext}"
    return f"{clean_title} {quality} {lang_str}.{ext}"


def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or data.get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except Exception as e:
        print(f"    [WARN] Vidara server fetch failed: {e}")
        return "https://api.vidara.so/v1/upload/server"


def extract_vidara_urls(data):
    """
    Returns (full_url, bare_filecode).

    full_url: whatever URL Vidara actually returned in `url` (or
    result.url), stored AS-IS into BEAM — Vidara's embed domain has changed
    more than once (vidara.so -> vidaraa.cc -> vidara.to), so reconstructing
    or hardcoding a domain is fragile. Store exactly what they give back.

    bare_filecode: just the last path segment, needed ONLY internally for
    the subtitle-attach API, which requires the bare code rather than a URL.
    """
    full_url = data.get("url") or data.get("result", {}).get("url")
    filecode = data.get("filecode") or data.get("result", {}).get("filecode")

    if not full_url and not filecode:
        raise Exception(f"Vidara upload returned no url/filecode: {data}")

    if not full_url:
        full_url = filecode  # fallback, shouldn't happen with current API behavior

    if not filecode:
        filecode = full_url.rstrip("/").split("/")[-1]

    return full_url, filecode


def upload_to_vidara(file_path, custom_name, content_type="video/x-matroska"):
    upload_server = fetch_vidara_upload_server()
    print(f"    Uploading to Vidara: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    with open(file_path, "rb") as fh:
        encoder = MultipartEncoder(fields={
            "api_key": VIDARA_API_KEY,
            "file": (custom_name, fh, content_type)
        })
        monitor = MultipartEncoderMonitor(encoder)
        response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)

    if response.status_code == 200:
        data = response.json()
        return extract_vidara_urls(data)  # (full_url, filecode)
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")


# ============================================================================
# SUBTITLES — extract English tracks only, host them permanently and freely
# on Internet Archive, then tell Vidara to attach that URL to the uploaded
# file's filecode. Extracting straight from the same source file guarantees
# the subtitle timing matches — no separate re-sync possible.
# ============================================================================

def extract_subtitle_to_srt(source_path, subtitle_stream_index, output_srt_path):
    """
    Pulls ONE subtitle stream out of the source file as a standalone .srt.
    Text-based subtitle codecs (srt/ass/webvtt/etc.) convert cleanly via
    -c:s srt. Image-based tracks (PGS/VobSub) can't convert this way and
    will fail — handled by the caller as a skip, not a hard error.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", f"0:s:{subtitle_stream_index}",
        "-c:s", "srt",
        str(output_srt_path)
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_srt_path) or os.path.getsize(output_srt_path) < 10:
        raise Exception(f"ffmpeg subtitle extraction failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True


def slugify_for_ia(text, max_len=80):
    text = re.sub(r'[^a-zA-Z0-9\-_.]', '-', text or "")
    text = re.sub(r'-+', '-', text).strip('-_.')
    return (text.lower() or "item")[:max_len]


def upload_to_archive_org(file_path, bucket_hint, key_hint, content_type="application/x-subrip", wait_seconds=60):
    """
    Uploads via Internet Archive's S3-compatible endpoint. Storage is free
    and permanent. IA can take a few seconds to a couple minutes before a
    freshly uploaded file is publicly fetchable, so this polls briefly
    before handing the URL back — Vidara needs to fetch it right away.
    """
    bucket = slugify_for_ia(f"beamplay-subs-{bucket_hint}")
    key = slugify_for_ia(key_hint) + ".srt"
    upload_url = f"https://s3.us.archive.org/{bucket}/{key}"

    headers = {
        "authorization": f"LOW {IA_ACCESS_KEY}:{IA_SECRET_KEY}",
        "x-amz-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "texts",
        "x-archive-meta-collection": "opensource",
        "x-archive-ignore-preexisting-bucket": "1",
        "Content-Type": content_type,
    }

    with open(file_path, "rb") as fh:
        data = fh.read()

    response = requests.put(upload_url, data=data, headers=headers, timeout=60)
    if response.status_code not in (200, 201):
        raise Exception(f"Archive.org upload failed: {response.status_code} {response.text[:200]}")

    direct_url = f"https://archive.org/download/{bucket}/{key}"

    attempts = max(1, wait_seconds // 5)
    for _ in range(attempts):
        try:
            check = requests.head(direct_url, timeout=10, allow_redirects=True)
            if check.status_code == 200:
                return direct_url
        except Exception:
            pass
        time.sleep(5)

    print(f"       [WARN] Archive.org file not confirmed reachable after {wait_seconds}s, proceeding anyway: {direct_url}")
    return direct_url


def attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English"):
    res = requests.get(
        "https://api.vidara.so/v1/upload/sub",
        params={"api_key": VIDARA_API_KEY, "filecode": filecode, "sub_lang": sub_lang, "sub_url": sub_url},
        timeout=30
    )
    res.raise_for_status()
    data = res.json()
    if data.get("status") != 200:
        raise Exception(f"Vidara subtitle attach failed: {data}")
    return True


def prepare_english_subtitle_urls(source_path, subtitle_tracks, bucket_hint, tmp_prefix):
    """
    Extracts every subtitle track normalized to 'English', uploads each to
    Internet Archive (one IA "item" per bucket_hint, reused across every
    subtitle for that movie), and returns (urls, failure_reasons).
    """
    urls = []
    failures = []
    english_tracks = [s for s in subtitle_tracks if s["language"] == "English"]
    if not english_tracks:
        return urls, failures

    for idx, sub in enumerate(english_tracks):
        srt_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.srt")
        try:
            extract_subtitle_to_srt(source_path, sub["stream_index"], srt_path)
            url = upload_to_archive_org(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
            urls.append(url)
            print(f"       [SUB] English subtitle #{idx+1} hosted -> {url}")
        except Exception as e:
            failures.append(f"track #{idx+1}: {e}")
            print(f"       [WARN] Could not prepare English subtitle #{idx+1}: {e}")
        finally:
            safe_delete(srt_path)

    return urls, failures


def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]


def beam_upsert(jwt, tmdb_id, quality, languages, url):
    res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
        "content_type": "movie",
        "tmdb_id": int(tmdb_id),
        "url": url,
        "quality": quality,
        "audio_languages": languages
    }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)
    res.raise_for_status()
    return res.json()


BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Connection": "keep-alive",
}


def download_file(url, dest_path):
    cmd = [
        "aria2c", "-x", "8", "-s", "8", "-k", "5M",
        "--file-allocation=none", "--summary-interval=0", "--retry-wait=10",
        "--max-tries=8", "--timeout=45", "--auto-file-renaming=false",
        f"--header=User-Agent: {BROWSER_HEADERS['User-Agent']}",
        f"--header=Accept: {BROWSER_HEADERS['Accept']}",
        "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    print("    [WARN] aria2c failed, trying direct stream...")
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        with requests.get(url, headers=BROWSER_HEADERS, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"    [ERROR] Direct stream failed: {e}")
        return False


def safe_delete(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"    [WARN] Could not delete {path}: {e}")


def format_error(link_number, stage, reason):
    return (
        f"FAILED\n\n"
        f"Link #{link_number}\n\n"
        f"Stage:\n{stage}\n\n"
        f"Reason:\n{reason}"
    )[:1500]


# ============================================================================
# CORE: process one quality cell for one row (no splitting — one upload per link)
# ============================================================================

def process_quality(jwt, tmdb_id, tmdb_name, year, quality, links_raw, row_idx):
    """
    Returns (status, error_text, subtitle_warnings). Never raises.
    """
    links = [l.strip() for l in links_raw.splitlines() if l.strip()]
    if not links:
        return "Done", "", []

    subtitle_warnings = []

    for link_number, link in enumerate(links, start=1):
        temp_path = os.path.join(TEMP_FOLDER, f"row{row_idx}_{quality}_link{link_number}")

        print(f"    -> {quality} Link #{link_number}: downloading...")
        try:
            ok = download_file(link, temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, "Download", str(e)), subtitle_warnings

        if not ok:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, "Download", "Download failed after retries"), subtitle_warnings

        try:
            audio_tracks, subtitle_tracks = inspect_tracks(temp_path)
            ext, fmt_name = detect_container_extension(temp_path)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, "MediaInfo", str(e)), subtitle_warnings

        languages = dedupe_languages(audio_tracks)
        content_type = CONTENT_TYPE_BY_EXT.get(ext, "application/octet-stream")

        print(f"       Container: {fmt_name} (.{ext}) | Audio languages: {languages}"
              + (f" | Subs: {[s['language'] for s in subtitle_tracks]}" if subtitle_tracks else ""))

        output_name = build_filename(tmdb_name, year, quality, languages, ext)

        subtitle_urls, prep_failures = prepare_english_subtitle_urls(
            temp_path, subtitle_tracks, f"{tmdb_id}", f"{tmdb_id}_{quality}_link{link_number}"
        )
        for fail_reason in prep_failures:
            subtitle_warnings.append(
                f"{quality} Link #{link_number}: could not extract/host English subtitle — {fail_reason}"
            )

        try:
            video_url, filecode = upload_to_vidara(temp_path, output_name, content_type)
            beam_upsert(jwt, tmdb_id, quality, languages, video_url)
        except Exception as e:
            safe_delete(temp_path)
            return "Failed", format_error(link_number, "Vidara Upload / BEAM Upsert", str(e)), subtitle_warnings

        print(f"       [OK] {output_name} uploaded and registered ({video_url}).")

        for sub_url in subtitle_urls:
            try:
                attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English")
                print(f"       [SUB] Attached English caption to {filecode}")
            except Exception as e:
                warning = (
                    f"{quality} Link #{link_number} video {video_url} (filecode {filecode}): "
                    f"video uploaded OK but subtitle attach failed ({e}). "
                    f"Download the caption yourself here: {sub_url}"
                )
                subtitle_warnings.append(warning)
                print(f"       [WARN] {warning}")

        safe_delete(temp_path)

    return "Done", "", subtitle_warnings


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("BEAM MOVIE DOWNLOADER v2 (NO SPLIT) — STARTING")
    print("=" * 60)

    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("GOOGLE_SHEETS_JSON is missing.")
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[OK] Connected to Google Sheets API")

    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    try:
        queue_sheet = spreadsheet.worksheet("Queue")
        archive_sheet = spreadsheet.worksheet("Archive")
    except Exception:
        queue_sheet = spreadsheet.get_worksheet(0)
        archive_sheet = spreadsheet.get_worksheet(1)
        print("[WARN] Named tabs not found, falling back to sheet indices 0/1.")

    raw_values = queue_sheet.get_all_values()
    if not raw_values:
        raise Exception("Queue worksheet is empty.")

    headers = [h.strip() for h in raw_values[0]]

    def col(name):
        return headers.index(name) + 1

    required = [
        "Filename", "Status", "TMDB_ID", "TMDB_NAME", "YEAR",
        "Link_1080p", "Link_720p", "Link_480p",
        "DOWNLOAD_STATUS_1080p", "DOWNLOAD_STATUS_720p", "DOWNLOAD_STATUS_480p",
        "Duplicate_Check", "ERROR_1080p", "ERROR_720p", "ERROR_480p"
    ]
    missing = [h for h in required if h not in headers]
    if missing:
        raise Exception(f"Missing required columns: {missing}. Found headers: {headers}")

    cols = {name: col(name) for name in required}

    all_rows = []
    for row_cells in raw_values[1:]:
        padded = row_cells + [""] * (len(headers) - len(row_cells))
        all_rows.append({headers[i]: padded[i] for i in range(len(headers)) if headers[i]})

    print(f"\nLoaded {len(all_rows)} rows.")

    jwt = beam_login()
    print("[OK] Logged into BEAM worker\n")

    QUALITIES = [
        ("1080p", "Link_1080p", "DOWNLOAD_STATUS_1080p", "ERROR_1080p"),
        ("720p", "Link_720p", "DOWNLOAD_STATUS_720p", "ERROR_720p"),
        ("480p", "Link_480p", "DOWNLOAD_STATUS_480p", "ERROR_480p"),
    ]

    for idx in range(len(all_rows) - 1, -1, -1):
        row = all_rows[idx]
        row_idx = idx + 2

        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        tmdb_name = str(row.get("TMDB_NAME", "")).strip()
        year = str(row.get("YEAR", "")).strip()

        if not tmdb_id:
            continue

        if str(row.get("Duplicate_Check", "")).strip().upper() == "DUPLICATE":
            print(f"Skipping Row {row_idx}: DUPLICATE")
            continue

        print(f"\n{'='*60}\nRow {row_idx}: {tmdb_name} ({year})\n{'='*60}")

        row_final_statuses = {}
        row_final_errors = {}

        for quality, link_col_name, status_col_name, error_col_name in QUALITIES:
            link_cell = str(row.get(link_col_name, "")).strip()
            current_status = str(row.get(status_col_name, "")).strip().lower()

            if not link_cell:
                row_final_statuses[quality] = current_status or ""
                row_final_errors[quality] = str(row.get(error_col_name, "")).strip()
                continue

            if current_status == "done":
                row_final_statuses[quality] = "done"
                row_final_errors[quality] = str(row.get(error_col_name, "")).strip()
                continue

            print(f"\n -> {quality}: starting (current status: '{current_status or 'empty'}')")

            queue_sheet.update_cell(row_idx, cols[status_col_name], "Running")

            status, error_text, subtitle_warnings = process_quality(
                jwt, tmdb_id, tmdb_name, year, quality, link_cell, row_idx
            )

            if status == "Done":
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Done")
                if subtitle_warnings:
                    note = ("DONE — but some subtitles need manual attach:\n\n"
                            + "\n\n".join(subtitle_warnings))[:1500]
                    queue_sheet.update_cell(row_idx, cols[error_col_name], note)
                    row_final_errors[quality] = note
                    print(f"    [DONE with subtitle warnings] {quality}")
                else:
                    queue_sheet.update_cell(row_idx, cols[error_col_name], "")
                    row_final_errors[quality] = ""
                    print(f"    [DONE] {quality} completed successfully.")
            else:
                queue_sheet.update_cell(row_idx, cols[status_col_name], "Failed")
                queue_sheet.update_cell(row_idx, cols[error_col_name], error_text)
                row_final_errors[quality] = error_text
                print(f"    [FAILED] {quality}:\n{error_text}")

            row_final_statuses[quality] = status.lower()

        present_qualities = [q for q, lc, _, _ in QUALITIES if str(row.get(lc, "")).strip()]
        all_done = all(row_final_statuses.get(q) == "done" for q in present_qualities) and present_qualities

        if all_done:
            print(f"\nRow {row_idx} fully completed. Archiving...")
            archive_row = [
                row.get("Filename", ""),
                row.get("Status", ""),
                tmdb_id,
                tmdb_name,
                year,
                row.get("Link_1080p", ""),
                row.get("Link_720p", ""),
                row.get("Link_480p", ""),
                "Done" if row.get("Link_1080p", "").strip() else "",
                "Done" if row.get("Link_720p", "").strip() else "",
                "Done" if row.get("Link_480p", "").strip() else "",
                row.get("Duplicate_Check", ""),
                row_final_errors.get("1080p", ""),
                row_final_errors.get("720p", ""),
                row_final_errors.get("480p", ""),
            ]
            archive_sheet.append_row(archive_row, value_input_option="USER_ENTERED")
            queue_sheet.delete_rows(row_idx)
            print(f"[OK] Row {row_idx} archived and removed from Queue.")

    try:
        shutil.rmtree(TEMP_FOLDER, ignore_errors=True)
    except Exception:
        pass

    print(f"\n{'='*60}\nPIPELINE COMPLETE\n{'='*60}")


if __name__ == "__main__":
    main()
