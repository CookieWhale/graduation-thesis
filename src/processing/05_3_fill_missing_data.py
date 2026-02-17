######## Goal: Fill missing data in user_proj_repo_after_6mon table
######## Only process the missing (user, project) combinations
######## Track users that don't exist on GitHub

import os, sys, time, json, argparse, asyncio, random
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import psycopg2
import httpx
from tqdm.asyncio import tqdm_asyncio
from dotenv import load_dotenv
import pytz

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
                        t.remaining = 5000
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
                    print(f"No tokens with remaining quota. Sleeping for {sleep_sec:.1f}s to wait for rate limit reset.")

            await asyncio.sleep(sleep_sec)

    async def release(self, t: TokenState, hdr):
        async with self.lock:
            t.in_use = False

            if 'X-RateLimit-Remaining' in hdr:
                t.remaining = int(hdr['X-RateLimit-Remaining'])

        if 'X-RateLimit-Reset' in hdr:
            try:
                reset_ts = float(hdr['X-RateLimit-Reset'])
                now = time.time()
                if reset_ts < now:  
                    reset_ts = now + 300
                t.reset = reset_ts
            except:
                t.reset = time.time() + 300
        else:
            t.reset = time.time() + 300

        self.permit.release()

tm = TokenManager(TOKENS)

# ────── GraphQL helpers ──────────────────────────────────────────────
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

            await tm.release(token_state, resp.headers)

            resp.raise_for_status()
            data = resp.json()

            # Check if user exists
            if "errors" in data:
                error_msg = str(data['errors'])
                if "Could not resolve to a User" in error_msg or "NOT_FOUND" in error_msg:
                    print(f"User '{login}' not found on GitHub")
                    raise UserNotFoundError(f"User {login} does not exist")
                else:
                    print(f"GraphQL returned error: {data['errors']}")
                    raise RuntimeError(data["errors"])

            # Check if user data is None
            if data.get("data", {}).get("user") is None:
                print(f"User '{login}' not found on GitHub (user is None)")
                raise UserNotFoundError(f"User {login} does not exist")

            repos = {
                i["repository"]["nameWithOwner"]
                for i in data["data"]["user"]["contributionsCollection"]["commitContributionsByRepository"]
            }
            return repos

        except UserNotFoundError:
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})
            raise  # Re-raise immediately, don't retry
            
        except Exception as e:
            print(f"Error for {login} [{start.date()} → {end.date()}] on try {attempt}: {e}")
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})

            if attempt == retries:
                raise RuntimeError(f"GraphQL failed permanently for {login} {start}-{end}")

            await asyncio.sleep(delay + random.random())
            delay *= 2

# Custom exception for non-existent users
class UserNotFoundError(Exception):
    pass

async def fetch_repos_for_window(login: str, window_start: datetime, window_end: datetime, client: httpx.AsyncClient):
    """Fetch repositories for a given time window, splitting into 1-year chunks if needed"""
    repos = set()
    ptr = window_start
    
    while ptr < window_end:
        # Handle leap year edge case (Feb 29 -> Feb 28 in non-leap year)
        try:
            nxt = ptr.replace(year=ptr.year + 1)
        except ValueError:
            # Feb 29 in leap year -> Feb 28 in non-leap year
            nxt = ptr.replace(year=ptr.year + 1, day=28)
        
        nxt = min(nxt, window_end)
        
        try:
            chunk_repos = await call_github(login, ptr, nxt, client)
            repos |= chunk_repos
        except UserNotFoundError:
            # User doesn't exist, propagate this up
            raise
        except Exception as e:
            # Don't return immediately, log and continue to next chunk
            print(f"Failed to fetch {login} [{ptr.date()} → {nxt.date()}]: {e}")
            # Continue processing next year chunk instead of returning
        ptr = nxt

    return repos

async def process_missing_pair(db_dsn, client, user_id, project_id, start_date, end_date, nonexistent_users):
    """
    Process a single missing (user, project) pair
    Returns True if successful, False if user doesn't exist
    """
    conn = psycopg2.connect(db_dsn)
    cur = conn.cursor()
    
    try:
        td2y = timedelta(days=730) 
        td6m = timedelta(days=183) 
        
        # Calculate windows
        before_start = start_date - td2y
        before_end = start_date
        after_start = end_date
        after_end = end_date + td6m
        
        print(f"  Processing {user_id} / project {project_id}")
        
        # Fetch before window repos
        try:
            before_repos = await fetch_repos_for_window(user_id, before_start, before_end, client)
            print(f"Before: {len(before_repos)} repos")
        except UserNotFoundError:
            nonexistent_users.add(user_id)
            print(f"User {user_id} does not exist on GitHub")
            return False
        
        # Fetch after window repos
        try:
            after_repos = await fetch_repos_for_window(user_id, after_start, after_end, client)
            print(f"After: {len(after_repos)} repos")
        except UserNotFoundError:
            nonexistent_users.add(user_id)
            print(f" User {user_id} does not exist on GitHub")
            return False
        
        # Insert before record
        cur.execute("""
            INSERT INTO user_proj_repo_after_6mon (user_id, project_id, window_type, window_start_time, window_end_time, repos)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, project_id, window_type) 
            DO UPDATE SET 
                window_start_time = EXCLUDED.window_start_time,
                window_end_time = EXCLUDED.window_end_time,
                repos = EXCLUDED.repos
        """, (user_id, project_id, 'before', before_start, before_end, json.dumps(sorted(before_repos))))
        
        # Insert after record
        cur.execute("""
            INSERT INTO user_proj_repo_after_6mon (user_id, project_id, window_type, window_start_time, window_end_time, repos)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, project_id, window_type) 
            DO UPDATE SET 
                window_start_time = EXCLUDED.window_start_time,
                window_end_time = EXCLUDED.window_end_time,
                repos = EXCLUDED.repos
        """, (user_id, project_id, 'after', after_start, after_end, json.dumps(sorted(after_repos))))
        
        conn.commit()
        print(f"Successfully inserted data for {user_id} / project {project_id}")
        return True

    except Exception as e:
        conn.rollback()
        print(f"Error processing {user_id} / project {project_id}: {e}")
        return False
    finally:
        cur.close()
        conn.close()

async def main():
    # ── DB connect & fetch missing data ──
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()

    print("Finding missing (user, project) combinations...")
    
    # Find missing (user, project) pairs
    cur.execute("""
        WITH missing_pairs AS (
            SELECT DISTINCT user_id, project_id
            FROM user_projects
            EXCEPT
            SELECT DISTINCT user_id, project_id
            FROM user_proj_repo_after_6mon
        )
        SELECT 
            mp.user_id, 
            mp.project_id, 
            p.start_date, 
            p.end_date
        FROM missing_pairs mp
        JOIN projects_clean p ON p.project_id = mp.project_id
        ORDER BY mp.user_id, mp.project_id;
    """)
    missing_data = cur.fetchall()

    cur.close()
    conn.close()

    print(f"found {len(missing_data)} missing (user, project) combinations")
    print(f"   This should result in {len(missing_data) * 2} new rows in user_proj_repo_after_6mon\n")

    if len(missing_data) == 0:
        print("No missing data found! All done.")
        return

    # Track non-existent users
    nonexistent_users = set()

    # ── Process each missing pair ──
    sem = asyncio.Semaphore(10)  # Limit concurrent tasks

    async def sem_task(user_id, project_id, start_date, end_date):
        async with sem:
            return await process_missing_pair(DB_DSN, client, user_id, project_id, start_date, end_date, nonexistent_users)

    async with httpx.AsyncClient(http2=True, timeout=120) as client:
        tasks = [
            sem_task(user_id, project_id, start_date, end_date)
            for user_id, project_id, start_date, end_date in missing_data
        ]
        
        results = []
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Processing"):
            result = await f
            results.append(result)

    # ── Summary ──
    successful = sum(results)
    failed = len(results) - successful
    
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total missing pairs processed: {len(missing_data)}")
    print(f"Successfully filled: {successful} pairs ({successful * 2} rows)")
    print(f"Failed: {failed} pairs")
    print(f"\n Non-existent GitHub users: {len(nonexistent_users)}")
    
    if nonexistent_users:
        print("\nList of non-existent users:")
        for user in sorted(nonexistent_users):
            print(f"  - {user}")
    
    print("\nAll done!")

if __name__ == "__main__":
    asyncio.run(main())