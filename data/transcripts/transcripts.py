"""
ETL Proccess for youtube transcripts

-1. Fetch all IDs of the tracked Playlists
0. Fetch latest downloadDate from supabase of an specific playlistID
1. Fetch Youtube API with an specific playlist ID, only new videos
2. Save MetaData in Supabase
3. Extract VideoID of each video
4. Send to NodeJS Server to process transcript with defuddle
5. Receive the transcript response
6. Save at Supabase
"""

import os
import requests
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
from pyyoutube import Api

load_dotenv()

url: str = os.getenv("SUPABASE_URL")
key: str = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(url, key)

youtube_api = Api(api_key=os.getenv("GCP_API_KEY"))


def getPlaylistIds() -> list:
    playlists = list()

    response = (
        supabase.table("active_playlists").select("playlistId, channelTitle").execute()
    )

    for row in response.data:
        playlists.append(str(row["playlistId"]))

    return playlists


def getLatestDay(playlists: list) -> dict:
    result = {}

    for playlist_id in playlists:
        response = (
            supabase.table("videos")
            .select("publishedAt")
            .eq("playlistId", playlist_id)
            .order("publishedAt", desc=True)
            .limit(1)
            .execute()
        )

        if response.data and response.data[0].get("publishedAt"):
            published_str = response.data[0]["publishedAt"]
            published_datetime = datetime.fromisoformat(
                published_str.replace("Z", "+00:00")
            )
            result[playlist_id] = published_datetime.date().isoformat()
        else:
            result[playlist_id] = None

    return result


def fetchYoutubePlaylists(playlists: list, latest_dates: dict) -> dict:
    result = {}

    for playlist_id in playlists:
        published_after = latest_dates.get(playlist_id)

        response = youtube_api.get_playlist_items(
            playlist_id=playlist_id, count=None, return_json=True
        )

        videos = []
        for item in response.get("items", []):
            status = item.get("status", {})
            privacy = status.get("privacyStatus")

            if privacy in ("private", "unlisted"):
                continue

            published_at = item["snippet"]["publishedAt"]

            if published_after and published_at < published_after:
                continue

            videos.append(
                {
                    "publishedAt": item["contentDetails"].get("videoPublishedAt"),
                    "channelId": item["snippet"]["channelId"],
                    "title": item["snippet"]["title"],
                    "description": item["snippet"]["description"],
                    "channelTitle": item["snippet"]["channelTitle"],
                    "playlistId": item["snippet"]["playlistId"],
                    "videoId": item["contentDetails"]["videoId"],
                }
            )

        result[playlist_id] = videos

    return result


def saveMetadata(videos_data: dict) -> tuple[int, int]:
    inserted = 0
    skipped = 0

    for playlist_id, videos in videos_data.items():
        for video in videos:
            try:
                supabase.table("videos").insert(
                    {
                        "publishedAt": video["publishedAt"],
                        "channelId": video.get("channelId"),
                        "title": video.get("title"),
                        "description": video.get("description"),
                        "channelTitle": video.get("channelTitle"),
                        "playlistId": video["playlistId"],
                        "videoId": video["videoId"],
                        "downloadDate": datetime.now(timezone.utc).isoformat(),
                    }
                ).execute()
                inserted += 1
            except Exception as e:
                if "duplicate key" in str(e).lower():
                    skipped += 1
                else:
                    raise Exception(f"Failed to insert video {video['videoId']}: {e}")

    return inserted, skipped


def getVideoIdsForProcessing() -> list:
    response = supabase.table("videos").select("videoId").execute()
    all_video_ids = [row["videoId"] for row in response.data]

    response = supabase.table("transcripts").select("videoId").execute()
    existing = set(row["videoId"] for row in response.data)

    return [vid for vid in all_video_ids if vid not in existing]


def fetchTranscripts(video_ids: list) -> dict:
    nodejs_url = os.getenv("SERVER_URL")

    def fetch_one(video_id):
        url = f"https://www.youtube.com/watch?v={video_id}"
        response = requests.get(f"{nodejs_url}/transcript?url={url}")
        if response.status_code == 200:
            return (video_id, response.text)
        return (video_id, "")

    results = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        for video_id, transcript in executor.map(fetch_one, video_ids):
            results[video_id] = transcript

    return results


def saveTranscripts(transcripts: dict) -> tuple[int, int]:
    inserted = 0
    skipped = 0

    for video_id, transcript in transcripts.items():
        try:
            supabase.table("transcripts").insert(
                {"videoId": video_id, "transcript": transcript}
            ).execute()
            inserted += 1
        except Exception as e:
            if "duplicate key" in str(e).lower():
                skipped += 1
            else:
                raise Exception(f"Failed to insert transcript for {video_id}: {e}")

    return inserted, skipped


def run_etl():
    results = {}

    print("[1] Fetching playlist IDs...")
    playlists = getPlaylistIds()
    results["playlists_count"] = len(playlists)
    print(f"    Found {len(playlists)} playlists")

    print("[2] Fetching latest dates...")
    latest_dates = getLatestDay(playlists)
    results["latest_dates"] = latest_dates

    print("[3] Fetching YouTube data...")
    videos_data = fetchYoutubePlaylists(playlists, latest_dates)
    total_videos = sum(len(videos) for videos in videos_data.values())
    results["new_videos"] = total_videos
    print(f"    Found {total_videos} new videos")

    print("[4] Saving metadata to Supabase...")
    print(f"    Total to insert: {total_videos}")
    inserted, skipped = saveMetadata(videos_data)
    results["inserted"] = inserted
    print(f"    Inserted {inserted} videos, skipped {skipped} duplicates")

    print("[5] Extracting video IDs for processing...")
    video_ids = getVideoIdsForProcessing()
    results["video_ids_to_process"] = len(video_ids)
    print(f"    Found {len(video_ids)} videos to process")

    print("[6] Fetching transcripts from NodeJS server...")
    print(f"    Total to process: {len(video_ids)}")
    transcripts = fetchTranscripts(video_ids)
    results["transcripts_fetched"] = len(transcripts)
    print(f"    Fetched {len(transcripts)} transcripts")

    print("[7] Saving transcripts to Supabase...")
    inserted, skipped = saveTranscripts(transcripts)
    results["transcripts_inserted"] = inserted
    print(f"    Inserted {inserted} transcripts, skipped {skipped} duplicates")

    print("\nETL Complete")


run_etl()
