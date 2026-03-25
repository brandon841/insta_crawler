import os
import time
import networkx as nx
from google.cloud import bigquery, storage

from crawler import (
    fetch_following,
    fetch_followers,
    add_following_to_graph,
    add_followers_to_graph,
)

GCP_PROJECT     = os.environ["GCP_PROJECT_ID"]
BQ_DATASET      = "instagram_graph"
BQ_NODES_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.nodes"
BQ_EDGES_TABLE  = f"{GCP_PROJECT}.{BQ_DATASET}.edges"
GCS_BUCKET      = os.environ["GCS_BUCKET_NAME"]

FOLLOWER_THRESHOLD  = 1_000  # min in-graph followers to qualify for depth-2 crawl
MAX_DEPTH2_ACCOUNTS = 50     # max accounts to crawl at depth 2


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

        print(f"  Fetching following for {username}...")
        following = fetch_following([username])
        add_following_to_graph(G, following, depth=1)

        print(f"  Fetching followers for {username}...")
        followers = fetch_followers([username])
        add_followers_to_graph(G, followers, depth=1)

        state.mark_fetched(username)
        print(f"  {username}: {len(following)} following, {len(followers)} followers — "
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


# ── 4. Store to BigQuery ───────────────────────────────────────────────────────
def store_to_bigquery(G: nx.DiGraph):
    bq    = bigquery.Client(project=GCP_PROJECT)
    nodes = [{"username": n, **d} for n, d in G.nodes(data=True)]
    edges = [{"source": u, "target": v, **d} for u, v, d in G.edges(data=True)]

    if nodes:
        bq.insert_rows_json(BQ_NODES_TABLE, nodes)
        print(f"Stored {len(nodes)} nodes to BigQuery")
    if edges:
        bq.insert_rows_json(BQ_EDGES_TABLE, edges)
        print(f"Stored {len(edges)} edges to BigQuery")

# ── 6. Entry point ─────────────────────────────────────────────────────────────
def main():
    seed_accounts = [
        "natgeo",
        "nasa",
        "bbcearth",
    ]

    run_id = str(int(time.time()))
    G = run_bfs_crawl(seed_accounts)

    store_to_bigquery(G)

    print("\n── Graph analysis ──────────────────────────────────────────")
    centrality = nx.degree_centrality(G)
    top_nodes  = sorted(centrality.items(), key=lambda x: x[1], reverse=True)[:5]
    components = nx.number_weakly_connected_components(G)
    print(f"Top 5 by centrality: {top_nodes}")
    print(f"Connected components: {components}")


if __name__ == "__main__":
    main()
