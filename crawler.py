import os
import networkx as nx
from apify_client import ApifyClient

APIFY_TOKEN        = os.environ["APIFY_API_TOKEN"]
FOLLOWING_ACTOR_ID = "louisdeconinck/instagram-following-scraper"
FOLLOWER_ACTOR_ID  = "louisdeconinck/instagram-followers-scraper"


# ── 1. Apify fetch ─────────────────────────────────────────────────────────────
def fetch_following(usernames: list[str]) -> list[dict]:
    """
    For each username, return the accounts they follow.
    Each result item contains `followed_by: <seed_username>`.
    Edge direction: seed → item  (seed follows item)
    """
    apify = ApifyClient(APIFY_TOKEN)
    run   = apify.actor(FOLLOWING_ACTOR_ID).call(
        run_input={"usernames": usernames}
    )
    return list(apify.dataset(run["defaultDatasetId"]).iterate_items())


def fetch_followers(usernames: list[str]) -> list[dict]:
    """
    For each username, return the accounts that follow them.
    Each result item contains `followed_to: <seed_username>`.
    Edge direction: item → seed  (item follows seed)
    """
    apify = ApifyClient(APIFY_TOKEN)
    run   = apify.actor(FOLLOWER_ACTOR_ID).call(
        run_input={"usernames": usernames}
    )
    return list(apify.dataset(run["defaultDatasetId"]).iterate_items())


# ── 2. Graph builder ───────────────────────────────────────────────────────────
def _upsert_node(G: nx.DiGraph, item: dict, depth: int):
    """Add a node from an Apify result item if it doesn't already exist."""
    username = item.get("username")
    if not username or username in G:
        return
    G.add_node(
        username,
        pk          = item.get("pk") or item.get("id", ""),
        full_name   = item.get("full_name", ""),
        is_private  = item.get("is_private", False),
        is_verified = item.get("is_verified", False),
        profile_pic = item.get("profile_pic_url", ""),
        depth       = depth,
    )


def add_following_to_graph(G: nx.DiGraph, items: list[dict], depth: int):
    """
    Items from the following actor — each item is someone the seed follows.
    `followed_by` = seed username  →  edge: seed → item.username
    """
    for item in items:
        seed     = item.get("followed_by")
        username = item.get("username")
        if not seed or not username:
            continue
        if seed not in G:
            G.add_node(seed, depth=depth)
        _upsert_node(G, item, depth)
        G.add_edge(seed, username, relationship="follows", depth=depth)


def add_followers_to_graph(G: nx.DiGraph, items: list[dict], depth: int):
    """
    Items from the follower actor — each item is someone who follows the seed.
    `followed_to` = seed username  →  edge: item.username → seed
    """
    for item in items:
        seed     = item.get("followed_to")
        username = item.get("username")
        if not seed or not username:
            continue
        if seed not in G:
            G.add_node(seed, depth=depth)
        _upsert_node(G, item, depth)
        G.add_edge(username, seed, relationship="follows", depth=depth)
