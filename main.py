import os
import time
import networkx as nx
from google.cloud import bigquery, storage
import pandas as pd
from apify_client import ApifyClient

from crawler import (
    fetch_following,
    fetch_followers,
    add_following_to_graph,
    add_followers_to_graph,
    fetch_profiles,
)

from dotenv import load_dotenv
load_dotenv()

GCP_PROJECT     = os.environ["GCP_PROJECT_ID"]
BQ_DATASET      = "instagram_graph"
BQ_NODES_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.nodes"
BQ_EDGES_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.edges"
GCS_BUCKET      = os.environ["GCS_BUCKET_NAME"]

MAX_DEPTH2_ACCOUNTS = 0     # max accounts to crawl at depth 2


# ── 1. Deduplication tracker ───────────────────────────────────────────────────
class CrawlState:
    """Tracks which accounts have already been fetched to avoid redundant API calls."""
    def __init__(self):
        self.fetched: set[str] = set()

    def mark_fetched(self, username: str):
        self.fetched.add(username)

    def should_fetch(self, username: str) -> bool:
        return username not in self.fetched


# ── 2. Priority queue for depth 2 ─────────────────────────────────────────────
def get_depth2_queue(G: nx.DiGraph, seed_accounts: list[str]) -> list[str]:
    """
    From depth-1 nodes, pick the top-N most connected accounts to crawl deeper.
    Sorted by in-degree within the current graph (most followed first).
    """
    seeds = set(seed_accounts)
    candidates = [
        node for node, data in G.nodes(data=True)
        if node not in seeds and data.get("depth", 0) == 1
    ]
    candidates.sort(key=lambda n: G.in_degree(n), reverse=True)
    return candidates[:MAX_DEPTH2_ACCOUNTS]


# ── 3. BFS orchestrator ────────────────────────────────────────────────────────
def run_bfs_crawl(seed_accounts: list[str]) -> nx.DiGraph:
    G = nx.DiGraph()
    state = CrawlState()

    # ── Depth 1: crawl all seeds ──
    print(f"[Depth 1] Crawling {len(seed_accounts)} seed accounts...")
    for username in seed_accounts:
        if not state.should_fetch(username):
            continue    

        # print(f"  Fetching following for {username}...")
        # following = fetch_following([username])
        # add_following_to_graph(G, following, depth=1)

        print(f"  Fetching followers for {username}...")
        followers = fetch_followers([username])
        add_followers_to_graph(G, followers, depth=1)

        state.mark_fetched(username)
        print(f"  {username}: {len(followers)} followers — "
              f"graph now {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
        time.sleep(2)

    # ── Depth 2: crawl top-N high-value nodes ──
    depth2_queue = get_depth2_queue(G, seed_accounts)
    print(f"\n[Depth 2] Queue: {len(depth2_queue)} accounts selected")

    for username in depth2_queue:
        if not state.should_fetch(username):
            continue
        print(f"  Crawling {username}...")

        following = fetch_following([username])
        add_following_to_graph(G, following, depth=2)

        followers = fetch_followers([username])
        add_followers_to_graph(G, followers, depth=2)

        state.mark_fetched(username)
        time.sleep(2)

    print(f"\n[Done] Final graph: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    return G

def run_profile_crawl(usernames: list[str]):
    #Function used to pull profile information for list of usernames 
    print(f"Crawling profiles for {len(usernames)} accounts...")

    profiles = fetch_profiles(usernames)
    print(f"Fetched profiles for {len(profiles)} accounts")
    return profiles


# ── 4. Store to BigQuery ───────────────────────────────────────────────────────
def store_to_bigquery(G: nx.DiGraph):
    bq = bigquery.Client(project=GCP_PROJECT)
    
    # Create dataset if it doesn't exist
    dataset_id = f"{GCP_PROJECT}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    bq.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {BQ_DATASET} ready")
    
    # Define nodes table schema
    nodes_schema = [
        bigquery.SchemaField("username", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pk", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("full_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("is_private", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("is_verified", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("profile_pic_url", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("latest_reel_media", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("depth", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("followed_to", "STRING", mode="NULLABLE"),
    ]
    
    # Create nodes table if it doesn't exist
    nodes_table = bigquery.Table(BQ_NODES_TABLE, schema=nodes_schema)
    bq.create_table(nodes_table, exists_ok=True)
    print(f"Nodes table ready")
    
    # Define edges table schema
    edges_schema = [
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("target", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("relationship", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("depth", "INTEGER", mode="NULLABLE"),
    ]
    
    # Create edges table if it doesn't exist
    edges_table = bigquery.Table(BQ_EDGES_TABLE, schema=edges_schema)
    bq.create_table(edges_table, exists_ok=True)
    print(f"Edges table ready")

    #if tables exist pull data
    if bq.get_table(BQ_NODES_TABLE).num_rows > 0 and bq.get_table(BQ_EDGES_TABLE).num_rows > 0:
        print("Existing data found in BigQuery, merging with new data...")

        existing_nodes = bq.list_rows(BQ_NODES_TABLE).to_dataframe()
        existing_edges = bq.list_rows(BQ_EDGES_TABLE).to_dataframe()
        print(f"Existing nodes: {len(existing_nodes)}, Existing edges: {len(existing_edges)}")

        # Combine with new data and drop duplicates
        new_nodes = pd.DataFrame([{"username": n, **d} for n, d in G.nodes(data=True)])
        new_edges = pd.DataFrame([{"source": u, "target": v, **d} for u, v, d in G.edges(data=True)])
        combined_nodes = pd.concat([existing_nodes, new_nodes]).drop_duplicates(subset=["username"])
        combined_edges = pd.concat([existing_edges, new_edges]).drop_duplicates(subset=["source", "target"])
        
        # Clear existing tables before inserting combined data
        bq.query(f"DELETE FROM `{BQ_NODES_TABLE}` WHERE TRUE").result()
        bq.query(f"DELETE FROM `{BQ_EDGES_TABLE}` WHERE TRUE").result()
        
        # Insert combined data
        bq.insert_rows_json(BQ_NODES_TABLE, combined_nodes.to_dict(orient="records"))
        bq.insert_rows_json(BQ_EDGES_TABLE, combined_edges.to_dict(orient="records"))
        print(f"Updated BigQuery with combined data: {len(combined_nodes)} nodes, {len(combined_edges)} edges")
        
        return new_nodes, new_edges
    
    # Insert data
    nodes = [{"username": n, **d} for n, d in G.nodes(data=True)]
    edges = [{"source": u, "target": v, **d} for u, v, d in G.edges(data=True)]

    new_nodes = pd.DataFrame(nodes)
    new_edges = pd.DataFrame(edges)

    if nodes:
        bq.insert_rows_json(BQ_NODES_TABLE, nodes)
        print(f"Stored {len(nodes)} nodes to BigQuery")
    if edges:
        bq.insert_rows_json(BQ_EDGES_TABLE, edges)
        print(f"Stored {len(edges)} edges to BigQuery")
    
    return new_nodes, new_edges

# ── 6. Entry point ─────────────────────────────────────────────────────────────
def main():
    seed_accounts = [
        "giberdaughter"
    ]

    run_id = str(int(time.time()))
    G = run_bfs_crawl(seed_accounts)
    #before storing save as parquet files and save locally as backup 
    # Insert data
    nodes = [{"username": n, **d} for n, d in G.nodes(data=True)]
    edges = [{"source": u, "target": v, **d} for u, v, d in G.edges(data=True)]

    # Save as parquet files locally
    nodes_df = pd.DataFrame(nodes)
    edges_df = pd.DataFrame(edges)
    nodes_df.to_parquet(f"nodes_{run_id}.parquet", index=False)
    edges_df.to_parquet(f"edges_{run_id}.parquet", index=False)
    print(f"Saved nodes and edges as parquet files locally")

    #saving to biquery dataset
    new_nodes, new_edges = store_to_bigquery(G)

    print("\n── Graph analysis ──────────────────────────────────────────")
    centrality = nx.degree_centrality(G)
    top_nodes  = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    components = nx.number_weakly_connected_components(G)
    print(f"Top 5 by centrality: {top_nodes}")
    print(f"Connected components: {components}")

    return new_nodes, new_edges


if __name__ == "__main__":
    new_nodes, new_edges  = main()

    #crawl ran now prompting user for next action
    #deduplicating nodes based on username and full_name
    nodes_dedup = new_nodes.drop_duplicates(subset = ["username", "full_name"])

    #filter to public accounts only
    nodes_dedup_public = nodes_dedup[nodes_dedup['is_private'] == False].reset_index(drop=True)
    #split into two df's based on is_verified
    nodes_verified = nodes_dedup_public[nodes_dedup_public['is_verified'] == True].reset_index(drop=True)
    nodes_unverified = nodes_dedup_public[nodes_dedup_public['is_verified'] == False].reset_index(drop=True)

    print(f"Total nodes: {len(nodes_dedup)}")
    print(f"Total nodes (public only): {len(nodes_dedup_public)}")
    print(f"Public Verified nodes: {len(nodes_verified)}")
    print(f"Public Unverified nodes: {len(nodes_unverified)}")
    
    # Prompt user for next action
    response = input("\nDo you want to pull ratio vals for public verified nodes? (y/n): ").strip().lower()
    if response == "y":
        # Call your next function here, e.g.:
        # next_function()
        print("Running the next step...")
        username_list = nodes_verified['username'].tolist()
        profiles = run_profile_crawl(username_list)

        # Turning profiles into dataframe and saving as parquet file
        profiles_df = pd.DataFrame(profiles)
        profiles_df['Ratio'] = profiles_df['followersCount'] / profiles_df['followsCount'].replace(0, 1)  # Avoid division by zero
        run_id = str(int(time.time()))
        profiles_df.to_parquet(f"profiles_{run_id}.parquet", index=False)
        print(f"Saved profiles with ratio as parquet file locally")

        print("Top 5 profiles by ratio and total followers:")
        top_profiles = profiles_df.sort_values(by=["Ratio", "followersCount"], ascending=False).head(5)
        print(top_profiles)
    else:
        print("Exiting.")
    
    