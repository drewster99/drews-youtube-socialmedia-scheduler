"""YouTube Data API v3 wrapper."""

from __future__ import annotations

import json
import os
from pathlib import Path

from googleapiclient.http import MediaFileUpload

from yt_scheduler.services.auth import get_youtube_service


def upload_video(
    file_path: str | Path,
    title: str,
    description: str = "",
    tags: list[str] | None = None,
    category_id: str = "28",  # "Science & Technology" — default for this dev-focused channel
    privacy_status: str = "unlisted",
    publish_at: str | None = None,
    made_for_kids: bool = False,
) -> dict:
    """Upload a video to YouTube using resumable upload.

    Returns the YouTube API response including the video ID.
    """
    youtube = get_youtube_service()

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags or [],
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": made_for_kids,
        },
    }

    if publish_at and privacy_status == "private":
        body["status"]["publishAt"] = publish_at

    media = MediaFileUpload(
        str(file_path),
        mimetype="video/*",
        resumable=True,
        chunksize=256 * 1024 * 10,  # 2.5 MB chunks
    )

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None
    while response is None:
        status, response = request.next_chunk()

    return response


def update_video_metadata(
    video_id: str,
    title: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    category_id: str | None = None,
    privacy_status: str | None = None,
    publish_at: str | None = None,
) -> dict:
    """Update video metadata. Only provided fields are changed."""
    youtube = get_youtube_service()

    # Fetch current metadata first
    current = youtube.videos().list(part="snippet,status", id=video_id).execute()
    if not current.get("items"):
        raise ValueError(f"Video {video_id} not found")

    item = current["items"][0]
    # Only keep mutable snippet fields — sending read-only fields (thumbnails,
    # channelTitle, localized, etc.) causes the YouTube API to reject the update.
    allowed_snippet = {"title", "description", "tags", "categoryId", "defaultLanguage", "defaultAudioLanguage"}
    snippet = {k: v for k, v in item["snippet"].items() if k in allowed_snippet}
    status = item["status"]

    if title is not None:
        snippet["title"] = title
    if description is not None:
        snippet["description"] = description
    if tags is not None:
        snippet["tags"] = tags
    if category_id is not None:
        snippet["categoryId"] = category_id
    if privacy_status is not None:
        status["privacyStatus"] = privacy_status
    if publish_at is not None:
        status["publishAt"] = publish_at

    return youtube.videos().update(
        part="snippet,status",
        body={"id": video_id, "snippet": snippet, "status": status},
    ).execute()


def set_thumbnail(video_id: str, thumbnail_path: str | Path) -> dict:
    """Upload a custom thumbnail for a video."""
    youtube = get_youtube_service()

    media = MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg")
    return youtube.thumbnails().set(videoId=video_id, media_body=media).execute()


def get_video(video_id: str) -> dict | None:
    """Get video details."""
    youtube = get_youtube_service()
    result = youtube.videos().list(
        part="snippet,status,statistics,contentDetails", id=video_id
    ).execute()
    items = result.get("items", [])
    return items[0] if items else None


def list_channel_videos(max_results: int = 25) -> list[dict]:
    """List videos from the authenticated user's channel."""
    youtube = get_youtube_service()

    # Get the uploads playlist
    channels = youtube.channels().list(part="contentDetails", mine=True).execute()
    if not channels.get("items"):
        return []

    uploads_playlist = channels["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # List videos from uploads playlist
    items = []
    request = youtube.playlistItems().list(
        part="snippet,status",
        playlistId=uploads_playlist,
        maxResults=min(max_results, 50),
    )

    while request and len(items) < max_results:
        result = request.execute()
        items.extend(result.get("items", []))
        request = youtube.playlistItems().list_next(request, result)

    return items[:max_results]


# --- Captions ---


def list_captions(video_id: str) -> list[dict]:
    """List caption tracks for a video."""
    youtube = get_youtube_service()
    result = youtube.captions().list(part="snippet", videoId=video_id).execute()
    return result.get("items", [])


def download_caption(caption_id: str, fmt: str = "srt") -> str:
    """Download a caption track. Returns the caption text."""
    youtube = get_youtube_service()
    return youtube.captions().download(id=caption_id, tfmt=fmt).execute().decode("utf-8")


# YouTube auto-captions live on a track owned by YouTube (ASR) that we can't
# overwrite. Our own uploads create / update a separate user-owned track that
# we tag with this name so we can find it again on subsequent saves.
_OUR_CAPTION_TRACK_NAME = "Drew's YT Scheduler"


def upload_caption(
    video_id: str,
    srt_text: str,
    language: str = "en",
    name: str = _OUR_CAPTION_TRACK_NAME,
) -> dict:
    """Push a transcript to YouTube as a user-uploaded caption track.

    Looks for an existing track with our app's name on this video and
    updates it if found; otherwise inserts a new one. Body is the raw SRT
    we already store as the canonical transcript shape — no conversion
    needed.

    Returns the caption resource (with ``id``) on success.
    """
    from io import BytesIO

    from googleapiclient.http import MediaIoBaseUpload

    if not srt_text.strip():
        raise ValueError("Refusing to upload an empty caption body.")

    youtube = get_youtube_service()
    media = MediaIoBaseUpload(
        BytesIO(srt_text.encode("utf-8")),
        mimetype="application/octet-stream",
        resumable=False,
    )

    # Look for an existing track with our name so we update instead of
    # piling up duplicates on each save.
    existing = list_captions(video_id)
    ours = next(
        (c for c in existing if c.get("snippet", {}).get("name") == name),
        None,
    )

    if ours is not None:
        return youtube.captions().update(
            part="snippet",
            body={
                "id": ours["id"],
                "snippet": {
                    "videoId": video_id,
                    "language": language,
                    "name": name,
                },
            },
            media_body=media,
        ).execute()

    return youtube.captions().insert(
        part="snippet",
        body={
            "snippet": {
                "videoId": video_id,
                "language": language,
                "name": name,
                "isDraft": False,
            },
        },
        media_body=media,
    ).execute()


# --- Video file download (for re-transcribing imported videos) -------------


def download_video_file(video_id: str, target_dir: "Path | str") -> "Path":
    """Pull the video file off YouTube via yt-dlp.

    Used when the user wants to transcribe an imported video locally —
    Apple Speech / Whisper backends need a local file path. Picks the
    smallest reasonable mp4 (audio is what the transcribers use; we don't
    need 4K) to minimise download time + disk usage.

    Returns the absolute path to the downloaded file. Raises RuntimeError
    if yt-dlp isn't installed or the download fails.
    """
    from pathlib import Path

    try:
        import yt_dlp
    except ImportError as exc:  # pragma: no cover — dependency probe
        raise RuntimeError(
            "yt-dlp is not installed. Install with: "
            "pip install -e \".[youtube-download]\""
        ) from exc

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(target_dir / f"{video_id}.%(ext)s")

    options = {
        "format": "best[height<=720][ext=mp4]/best[ext=mp4]/best",
        "outtmpl": out_template,
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        # Don't write metadata or thumbnail sidecars — we already have those.
        "writethumbnail": False,
        "writeinfojson": False,
        "writedescription": False,
        "writesubtitles": False,
    }

    url = f"https://www.youtube.com/watch?v={video_id}"
    with yt_dlp.YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=True)
        downloaded_path = ydl.prepare_filename(info)

    path = Path(downloaded_path)
    if not path.exists():
        # yt-dlp sometimes finalises with a different extension than the
        # template suggests; pick the latest matching file in the dir.
        candidates = sorted(
            target_dir.glob(f"{video_id}.*"),
            key=lambda p: p.stat().st_mtime,
        )
        if not candidates:
            raise RuntimeError(f"yt-dlp finished but no file found for {video_id}")
        path = candidates[-1]
    return path.resolve()


# --- Comments ---


def list_comment_threads(
    video_id: str,
    max_results: int = 100,
    moderation_status: str | None = None,
) -> list[dict]:
    """List top-level comment threads on a video."""
    youtube = get_youtube_service()

    kwargs = {
        "part": "snippet,replies",
        "videoId": video_id,
        "maxResults": min(max_results, 100),
        "textFormat": "plainText",
    }
    if moderation_status:
        kwargs["moderationStatus"] = moderation_status

    items = []
    request = youtube.commentThreads().list(**kwargs)

    while request and len(items) < max_results:
        result = request.execute()
        items.extend(result.get("items", []))
        request = youtube.commentThreads().list_next(request, result)

    return items[:max_results]


def reply_to_comment(parent_comment_id: str, text: str) -> dict:
    """Reply to a comment."""
    youtube = get_youtube_service()
    return youtube.comments().insert(
        part="snippet",
        body={
            "snippet": {
                "parentId": parent_comment_id,
                "textOriginal": text,
            }
        },
    ).execute()


def delete_comment(comment_id: str) -> None:
    """Delete a comment."""
    youtube = get_youtube_service()
    youtube.comments().delete(id=comment_id).execute()


def moderate_comment(comment_id: str, status: str = "rejected", ban_author: bool = False) -> None:
    """Set moderation status on a comment.

    status: 'published', 'heldForReview', 'rejected'
    """
    youtube = get_youtube_service()
    youtube.comments().setModerationStatus(
        id=comment_id,
        moderationStatus=status,
        banAuthor=ban_author,
    ).execute()


# --- Channel ---


def get_channel_info() -> dict | None:
    """Get the authenticated user's channel info."""
    youtube = get_youtube_service()
    result = youtube.channels().list(
        part="snippet,statistics,brandingSettings,contentDetails",
        mine=True,
    ).execute()
    items = result.get("items", [])
    return items[0] if items else None
