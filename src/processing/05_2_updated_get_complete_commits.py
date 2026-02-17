######## Goal: 
######## Create tables to record GitHub repos that a user contributed to before or after a hackathon project
######## Then populate the tables with GitHub GraphQL API

"""
users (
    user_id TEXT PRIMARY KEY,
    hash TEXT
);

projects (
    project_id SERIAL PRIMARY KEY,
    project_url TEXT UNIQUE,
    repo_links TEXT[],
    start_date TIMESTAMPTZ,
    end_date TIMESTAMPTZ
);

user_projects (
    user_project_id SERIAL PRIMARY KEY,
    user_id TEXT REFERENCES users(user_id),
    project_id INT REFERENCES projects(project_id)
);

-- user_proj_repo: stores contribution windows for each user-project combination
CREATE TABLE IF NOT EXISTS user_proj_repo (
    user_id TEXT REFERENCES users(user_id),
    project_id INT REFERENCES projects(project_id),
    window_type TEXT CHECK (window_type IN ('before','after')),
    window_start_time TIMESTAMPTZ NOT NULL,
    window_end_time TIMESTAMPTZ NOT NULL,
    repos JSONB,
    PRIMARY KEY (user_id, project_id, window_type)
);

-- processed_users: track which users have been processed
CREATE TABLE IF NOT EXISTS processed_users (
    user_id TEXT PRIMARY KEY
);
"""

import os, sys, time, json, argparse, asyncio, math, random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
import psycopg2
import httpx
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv
import time
import pytz

# ────── CLI ──────────────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--limit-users", type=int, default=0)
args = parser.parse_args()

# ────── Environment variables ──────────────────────────────────────────────────────
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")
DB_DSN = f"dbname={os.getenv('DB_NAME')} user={os.getenv('DB_USER')} " \
         f"password={os.getenv('DB_PASSWORD')} host={os.getenv('DB_HOST','localhost')} " \
         f"port={os.getenv('DB_PORT','5432')}"
TOKENS = [t.strip() for t in os.getenv("TOKENS","").split(",") if t.strip()]
if not TOKENS:
    sys.exit("Set TOKENS=ghp_xxx,... in .env")

# ────── Token Manager ────────────────────────────────────────────────
class TokenState:
    __slots__ = ("token", "remaining", "reset", "in_use")
    def __init__(self, token): self.token, self.remaining, self.reset, self.in_use = token, 5000, 0, False

class TokenManager:
    def __init__(self, tokens, per_token_concurrency=5):
        self.tokens = [TokenState(t) for t in tokens]
        self.permit = asyncio.Semaphore(len(tokens)*per_token_concurrency)
        self.lock   = asyncio.Lock()

    async def acquire(self):
        await self.permit.acquire()

        while True:
            async with self.lock:
                now = time.time()

            
                for i, t in enumerate(self.tokens):
                    if t.remaining == 0 and now >= t.reset:
                        t.remaining = 5000  # auto recover 
                    if t.remaining > 0 and not t.in_use:
                        t.in_use = True
                        t.remaining -= 1
                        return t

                active_tokens = [t for t in self.tokens if t.remaining > 0]

                if active_tokens:
                    sleep_sec = 5 
                    print("All tokens are busy but still have quota. Waiting shortly for any to free...")
                else:
                    next_reset = min(t.reset for t in self.tokens)
                    sleep_sec = max(next_reset - now + 5, 5)
                    local_tz = pytz.timezone('Europe/Amsterdam')
                    # print("No tokens with remaining quota.")
                    # print("Now:", datetime.fromtimestamp(now, local_tz))
                    # print("Reset time:", datetime.fromtimestamp(next_reset, local_tz))
                    print(f"No tokens with remaining quota. Sleeping for {sleep_sec:.1f}s to wait for rate limit reset.")

            await asyncio.sleep(sleep_sec)


    async def release(self, t: TokenState, hdr):
        async with self.lock:
            t.in_use = False
            # print(f"Releasing token: {t.token[:6]}...")

            if 'X-RateLimit-Remaining' in hdr:
                t.remaining = int(hdr['X-RateLimit-Remaining'])
                # print(f"Updated remaining = {t.remaining}")

        if 'X-RateLimit-Reset' in hdr:
            try:
                reset_ts = float(hdr['X-RateLimit-Reset'])
                now = time.time()
                if reset_ts < now:  
                    reset_ts = now + 300 # wait 300 seconds
                t.reset = reset_ts
            except:
                t.reset = time.time() + 300  # fallback
        else:
            t.reset = time.time() + 300  # fallback if reset header missing

        self.permit.release()

tm = TokenManager(TOKENS)

# ────── GraphQL helpers ──────────────────────────────────────────────
# Only keep commits, ignore pr and issues
GQL = """
query($login:String!,$from:DateTime!,$to:DateTime!){
  user(login:$login){
    contributionsCollection(from:$from,to:$to){
      commitContributionsByRepository{
        repository{nameWithOwner}
      }
    }
  }
}"""

async def call_github(login: str, start: datetime, end: datetime, client: httpx.AsyncClient, retries=3):
    delay = 1
    for attempt in range(1, retries + 1):
        token_state = await tm.acquire()
        headers = {"Authorization": f"Bearer {token_state.token}"}

        try:
            t0 = time.time()
            resp = await client.post(
                "https://api.github.com/graphql",
                json={
                    "query": GQL,
                    "variables": {
                        "login": login,
                        "from": start.isoformat(),
                        "to": end.isoformat()
                    }
                },
                headers=headers,
                timeout=120
            )
            elapsed = time.time() - t0

            await tm.release(token_state, resp.headers)

            resp.raise_for_status()
            data = resp.json()

            if "errors" in data:
                print(f"GraphQL returned error: {data['errors']}")
                raise RuntimeError(data["errors"])

            repos = {
                i["repository"]["nameWithOwner"]
                for i in data["data"]["user"]["contributionsCollection"]["commitContributionsByRepository"]
            }
            return repos

        except Exception as e:
            print(f" Error for {login} [{start.date()} → {end.date()}] on try {attempt}: {e}")
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})

            if attempt == retries:
                raise RuntimeError(f"GraphQL failed permanently for {login} {start}-{end}")

            await asyncio.sleep(delay + random.random())
            delay *= 2

# ────── DB helpers ───────────────────────────────────────────────────
def merge_intervals(intervals):
    """Merge overlapping intervals"""
    intervals.sort(key=lambda x: x[0]) # sorted by start date
    merged = []
    for s, e in intervals:
        if not merged or s > merged[-1][1]: # do NOT need to be merged
            merged.append([s, e]) # add the new interval
        else: # need to be merged. 
            # Since s>=merged[-1][0], pick merged[-1][0] as the start date of merged interval
            merged[-1][1] = max(merged[-1][1], e) # pick the max end date for merged interval
    return merged

async def fetch_repos_for_window(login: str, window_start: datetime, window_end: datetime, client: httpx.AsyncClient):
    """Fetch repositories for a given time window, splitting into 1-year chunks if needed"""
    # print(f"processing user {login} contribution during {window_start} and {window_end}")
    repos = set()
    ptr = window_start
    
    while ptr < window_end:
        nxt = min(ptr.replace(year=ptr.year + 1), window_end)
        try:
            chunk_repos = await call_github(login, ptr, nxt, client)
            repos |= chunk_repos
        except Exception as e:
            print(f"Exception processing user {login} contribution during {window_start} and {window_end}: {str(e)}")
            return set()  
        ptr = nxt

    return repos

async def process_user(db_dsn, client, login, projects):
    """
    Process a user's projects and store repo contributions for each project's before/after windows
    projects: list of (user_project_id, project_id, start_date, end_date)
    """
    t0 = time.time()
    conn = psycopg2.connect(db_dsn)
    cur = conn.cursor()
    
    try:
        td2y = timedelta(days=730)  # 2 years
        
        # Group projects by (project_id, window_type) for potential merging
        window_groups = defaultdict(list)  # {(project_id, window_type): [(start, end), ...]}
        
        for up_id, proj_id, start_date, end_date in projects:
            before_window = (start_date - td2y, start_date)
            after_window = (end_date, end_date + td2y)
            
            window_groups[(proj_id, 'before')].append(before_window)
            window_groups[(proj_id, 'after')].append(after_window)
        
        print(f"  Processing user: {login} with {len(projects)} projects")
        
        # Process each (project_id, window_type) group
        for (proj_id, window_type), windows in window_groups.items():
            # Merge overlapping windows for the same project and window_type
            merged_windows = merge_intervals(windows)
            
            # print(f"    Processing project {proj_id} ({window_type}): {len(windows)} windows merged into {len(merged_windows)}")
            
            # For each merged window, collect all repos
            all_repos = set()
            final_start = None
            final_end = None
            
            for window_start, window_end in merged_windows:
                if final_start is None or window_start < final_start:
                    final_start = window_start
                if final_end is None or window_end > final_end:
                    final_end = window_end
                    
                repos = await fetch_repos_for_window(login, window_start, window_end, client)
                all_repos |= repos
                # print(f"      Window {window_start.date()} → {window_end.date()}: {len(repos)} repos")
            
            # Insert into user_proj_repo table
            print(f"      Inserting {len(all_repos)} repos for project {proj_id} ({window_type})")
            cur.execute("""
                INSERT INTO user_proj_repo (user_id, project_id, window_type, window_start_time, window_end_time, repos)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id, project_id, window_type) 
                DO UPDATE SET 
                    window_start_time = EXCLUDED.window_start_time,
                    window_end_time = EXCLUDED.window_end_time,
                    repos = EXCLUDED.repos
            """, (login, proj_id, window_type, final_start, final_end, json.dumps(sorted(all_repos))))
        
        # Mark this user as processed
        cur.execute("INSERT INTO processed_users(user_id) VALUES (%s) ON CONFLICT DO NOTHING", (login,))
        
        conn.commit()
        print(f"Successfully processed user {login}")

    except Exception as e:
        conn.rollback()
        print(f"Error while processing user {login}: {e}")
    finally:
        cur.close()
        conn.close()
        
    

async def main():
    # ── DB connect & fetch data ──
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    where = []
    if args.limit_users:
        where.append(f"up.user_id IN (SELECT user_id FROM user_projects LIMIT {args.limit_users})")
    if args.whitelist:
        users = "','".join([u.strip() for u in args.whitelist.split(",")])
        where.append(f"up.user_id IN ('{users}')")

    # where_sql = "WHERE " + " AND ".join(where) if where else ""
    
    # Fetch unprocessed users and their projects
    cur.execute(f"""
        SELECT up.user_id, up.user_project_id, up.project_id, p.start_date, p.end_date
        FROM user_projects up
        JOIN projects p ON p.project_id = up.project_id
        LEFT JOIN processed_users pu ON pu.user_id = up.user_id
        WHERE pu.user_id IS NULL
        {('AND ' + ' AND '.join(where)) if where else ''};
    """)
    rows = cur.fetchall()

    # Group by user_id
    user_projects = defaultdict(list)
    for uid, upid, proj_id, start_date, end_date in rows:
        user_projects[uid].append((upid, proj_id, start_date, end_date))

    cur.close()
    conn.close()

    print(f"Loaded {len(user_projects)} unique users to process\n")

    # ── Control per-user concurrency ──
    sem = asyncio.Semaphore(10)  # Limit concurrent users

    async def sem_task(uid, projects):
        async with sem:
            await process_user(DB_DSN, client, uid, projects)

    # ── Start all tasks ──
    async with httpx.AsyncClient(http2=True, timeout=40) as client:
        tasks = [
            sem_task(uid, projects)
            for uid, projects in user_projects.items()
        ]
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Users"):
            await f

    print("All done.")

if __name__ == "__main__":
    asyncio.run(main())