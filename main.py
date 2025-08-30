"""
YouTube Channel Discovery Script
--------------------------------

This script uses Google's YouTube Data API v3 to discover brand‑new YouTube
channels based on a set of simple "first video" search queries.  It then
retrieves basic channel statistics for those channels and writes any channel
created in the last 30 days to a CSV file.  The CSV filename embeds the
current date so that you can collect a daily snapshot over time.

How it works
^^^^^^^^^^^^

1. **Discovery** – For each query in ``SEARCH_QUERIES``, the script issues a
   ``search.list`` request ordered by date.  It sets ``publishedAfter`` to 24
   hours ago so that only very recent videos are considered.  Each
   search result returns a ``channelId`` which the script collects.
2. **Enrichment** – The collected channel IDs are grouped into batches of
   50 (the maximum allowed per ``channels.list`` call) and passed to the
   ``channels.list`` method with ``part=snippet,statistics``.  The script
   extracts the channel title, creation date, subscriber count, video count
   and view count from the response.
3. **Filtering** – Results are loaded into a Pandas ``DataFrame``.  The
   channel age in days is calculated by subtracting the channel's
   ``publishedAt`` date from the time the data was retrieved.  Only channels
   less than or equal to 30 days old are retained.
4. **Saving** – The filtered data is written to a CSV file named
   ``new_youtube_channels_<YYYY-MM-DD>.csv`` in the current directory.  The
   date refers to when the script was executed (UTC).

The script reads your YouTube API key from the environment variable
``YOUTUBE_API_KEY``.  For local development you can place this key in a
``.env`` file alongside ``main.py`` and it will be loaded automatically.

Legal compliance
----------------

This script uses the official YouTube Data API to fetch only public
information about channels and does not scrape any personal or private data.
YouTube imposes a quota system to ensure fair use of its API: each
``search.list`` request costs 100 units and each ``channels.list`` call costs
1 unit【650972628472989†L112-L117】.  By default, a project receives 10 000
quota units per day【650972628472989†L112-L117】, and this script stays well
within that limit.  If your use case grows beyond the default quota, you
should apply for an audit and quota extension by following Google’s
documentation【650972628472989†L121-L126】.

The YouTube API Services Developer Policies emphasise building high quality
applications, being transparent about how data is collected and used, and
respecting user privacy【572623043423†L127-L160】.  This script
operates on publicly available channel metadata and does not collect
individual user information or perform any actions on behalf of users.  It
therefore complies with those principles.
"""

import os
import time
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv


def get_youtube_client(api_key: str):
    """Initialise the YouTube Data API client.

    Parameters
    ----------
    api_key : str
        Your YouTube Data API v3 key.

    Returns
    -------
    googleapiclient.discovery.Resource
        A resource object with methods to call the API.
    """
    return build("youtube", "v3", developerKey=api_key)


def search_new_videos(youtube, query: str, published_after: str) -> List[str]:
    """Search YouTube for videos published after a given date and return channel IDs.

    A helper around ``youtube.search().list`` that catches errors and
    consolidates pagination if necessary.  Currently we limit results to the
    first 50 per query to control quota usage.

    Parameters
    ----------
    youtube : googleapiclient.discovery.Resource
        The YouTube API client.
    query : str
        The search term to use (e.g., "first vlog").
    published_after : str
        ISO 8601 date/time string specifying the earliest publication time of
        videos to include.

    Returns
    -------
    List[str]
        A list of channel IDs associated with the search results.
    """
    channel_ids: List[str] = []
    try:
        search_request = (
            youtube.search()
            .list(
                q=query,
                type="video",
                order="date",
                publishedAfter=published_after,
                part="snippet",
                maxResults=50,
            )
        )
        search_response = search_request.execute()
        for item in search_response.get("items", []):
            channel_id = item.get("snippet", {}).get("channelId")
            if channel_id:
                channel_ids.append(channel_id)
    except HttpError as e:
        # Log the error; in a production system you might integrate with a
        # monitoring/alerting service instead of printing.
        print(f"HTTP error during search for query '{query}': {e}")
    except Exception as e:
        print(f"Unexpected error during search for query '{query}': {e}")
    return channel_ids


def get_channel_details(youtube, channel_ids: List[str]) -> List[Dict[str, str]]:
    """Fetch details for a list of channel IDs.

    Uses the ``channels.list`` endpoint to retrieve the channel title, creation
    date and statistics (subscriber count, view count, video count) for up to
    50 channels at once.  Handles any errors gracefully by returning an empty
    list if the request fails.

    Parameters
    ----------
    youtube : googleapiclient.discovery.Resource
        The YouTube API client.
    channel_ids : List[str]
        A list of up to 50 channel IDs.

    Returns
    -------
    List[Dict[str, str]]
        A list of dictionaries containing channel metadata.
    """
    data: List[Dict[str, str]] = []
    if not channel_ids:
        return data
    try:
        response = (
            youtube.channels()
            .list(
                id=','.join(channel_ids),
                part="snippet,statistics",
            )
            .execute()
        )
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            stats = item.get("statistics", {})
            data.append(
                {
                    "channel_id": item.get("id"),
                    "channel_title": snippet.get("title", ""),
                    "published_at": snippet.get("publishedAt", ""),
                    "subscriber_count": stats.get("subscriberCount", 0),
                    "video_count": stats.get("videoCount", 0),
                    "view_count": stats.get("viewCount", 0),
                    "data_retrieved_at": datetime.utcnow().isoformat() + "Z",
                }
            )
    except HttpError as e:
        print(f"HTTP error retrieving channel details: {e}")
    except Exception as e:
        print(f"Unexpected error retrieving channel details: {e}")
    return data


def collect_new_channels(youtube, queries: List[str], window_hours: int = 24) -> List[Dict[str, str]]:
    """Collect channel details for recent videos across multiple search queries.

    Parameters
    ----------
    youtube : googleapiclient.discovery.Resource
        The YouTube API client.
    queries : List[str]
        A list of search terms to probe for "first video" type uploads.
    window_hours : int, optional
        The time window in hours for which to consider videos.  Defaults to 24.

    Returns
    -------
    List[Dict[str, str]]
        A list of dictionaries containing channel details.
    """
    published_after = (datetime.utcnow() - timedelta(hours=window_hours)).isoformat("T") + "Z"
    all_channel_ids = set()
    # Discover channel IDs
    for query in queries:
        channel_ids = search_new_videos(youtube, query, published_after)
        all_channel_ids.update(channel_ids)
        # Sleep briefly to avoid sending too many requests in quick succession
        time.sleep(0.1)
    # Enrich channel IDs into detailed records
    all_channel_data: List[Dict[str, str]] = []
    channel_id_list = list(all_channel_ids)
    for i in range(0, len(channel_id_list), 50):
        batch = channel_id_list[i:i + 50]
        details = get_channel_details(youtube, batch)
        all_channel_data.extend(details)
        time.sleep(0.1)
    return all_channel_data


def filter_and_save_channels(all_channel_data: List[Dict[str, str]], max_age_days: float = 30.0) -> str:
    """Filter channel records for those created within ``max_age_days`` and save to CSV.

    Parameters
    ----------
    all_channel_data : List[Dict[str, str]]
        List of channel metadata dictionaries as returned by ``get_channel_details``.
    max_age_days : float, optional
        Maximum age of channels (in days) to retain.  Defaults to 30.

    Returns
    -------
    str
        The filename of the generated CSV file.  Returns an empty string if no
        qualifying channels were found.
    """
    if not all_channel_data:
        print("No channel data collected; nothing to save.")
        return ""
    df = pd.DataFrame(all_channel_data)
    # Ensure datetime columns are parsed correctly
    df["published_at_dt"] = pd.to_datetime(df["published_at"], errors="coerce")
    df["data_retrieved_at_dt"] = pd.to_datetime(df["data_retrieved_at"], errors="coerce")
    # Compute age in days
    df["channel_age_days"] = (df["data_retrieved_at_dt"] - df["published_at_dt"]).dt.total_seconds() / (24 * 3600)
    # Filter channels younger than max_age_days
    df_new = df[df["channel_age_days"] <= max_age_days].copy()
    # Convert numeric fields to integers (coerce non‑numeric to NaN then fill with 0)
    for col in ["subscriber_count", "video_count", "view_count"]:
        df_new[col] = pd.to_numeric(df_new[col], errors="coerce").fillna(0).astype("int64")
    if df_new.empty:
        print(f"No channels younger than {max_age_days} days found.")
        return ""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"new_youtube_channels_{date_str}.csv"
    df_new.to_csv(filename, index=False)
    print(f"Saved {len(df_new)} new channels to {filename}.")
    return filename


def main() -> None:
    """Main entry point for running the discovery pipeline.

    Loads environment variables, initialises the API client, collects recent
    channel data based on the configured search queries, filters the results
    for channels created within 30 days, and writes the output to CSV.
    """
    # Load environment variables from .env if present.  This allows you to
    # develop locally without exposing the API key in your code.  When running
    # on GitHub Actions, the API key will be injected via an environment
    # variable instead.
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise RuntimeError(
            "YOUTUBE_API_KEY environment variable not set. "
            "Define it in a .env file or set it in your execution environment."
        )
    youtube = get_youtube_client(api_key)
    # Configure your search queries here.  Feel free to add or remove terms.
    search_queries = [
        "first vlog",
        "new channel",
        "hello everyone",
        "channel trailer",
        "gameplay episode 1",
        "vlog day 1",
        "first video",
    ]
    all_data = collect_new_channels(youtube, search_queries, window_hours=24)
    filter_and_save_channels(all_data, max_age_days=30.0)


if __name__ == "__main__":
    main()