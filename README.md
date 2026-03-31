# insta_crawler

This project builds social network graphs of Instagram users, storing the results in Google BigQuery and local Parquet files. It uses Apify actors to fetch following/follower relationships and profile metadata.

## Features
- Crawl Instagram following/follower relationships for a set of seed accounts
- Build a directed graph (using NetworkX) of user relationships
- Store nodes and edges in BigQuery tables (auto-creates tables if needed)
- Save local Parquet backups of crawled data
- Deduplicate and merge with existing BigQuery data
- Fetch detailed profile metadata for a list of usernames

## Requirements
- Python 3.8+
- Google Cloud project with BigQuery enabled
- Apify account and API token

Install dependencies:
```bash
pip install -r requirements.txt
```

## Environment Variables
Create a `.env` file in the project root with:
```
APIFY_API_TOKEN=your_apify_token
GCP_PROJECT_ID=your_gcp_project_id
GCS_BUCKET_NAME=your_gcs_bucket_name  # (not required unless you add GCS upload)
GOOGLE_APPLICATION_CREDENTIALS=path/to/your/service_account.json
```

## Usage

### 1. Run the main crawl
Edit the `seed_accounts` list in `main.py` to your desired Instagram usernames. Then run:
```bash
python main.py
```
This will:
- Crawl followers (and optionally following) for each seed account
- Build a directed graph of users
- Save nodes/edges as Parquet files locally
- Store/merge nodes and edges in BigQuery tables

### 2. Fetch profile metadata
You can use the `run_profile_crawl` function to fetch detailed profile info for a list of usernames:
```python
from main import run_profile_crawl
profiles = run_profile_crawl(["username1", "username2"])
```

### 3. BigQuery Table Schemas
Tables are auto-created if they do not exist:
- **Nodes Table**: username, pk, full_name, is_private, is_verified, profile_pic_url, latest_reel_media, depth, followed_to
- **Edges Table**: source, target, relationship, depth

## Code Structure
- `main.py`: Orchestrates the crawl, graph building, BigQuery storage, and profile crawling.
- `crawler.py`: Contains functions to fetch data from Apify and build the NetworkX graph.
- `requirements.txt`: Python dependencies.

## Notes
- The script currently only fetches followers for each seed account (following is commented out, but can be enabled).
- Set `MAX_DEPTH2_ACCOUNTS` in `main.py` to control how many high-value accounts are crawled at depth 2.
- The script merges new data with existing BigQuery data, deduplicating by username (nodes) and (source, target) (edges).
- Local Parquet files are always saved as a backup.

## Example
```python
seed_accounts = ["natgeo", "nasa", "bbcearth"]
G = run_bfs_crawl(seed_accounts)
store_to_bigquery(G)
```

## License
MIT