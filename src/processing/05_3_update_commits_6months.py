import asyncio
import asyncpg
import httpx
import os
import sys
import time
import random
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from tqdm.asyncio import tqdm
from typing import List
import pytz
import json
import socket

"""
CREATE TABLE IF NOT EXISTS user_proj_repo_after_6mon (
    user_id TEXT REFERENCES users(user_id),
    project_id INT REFERENCES projects(project_id),
    window_type TEXT CHECK (window_type IN ('before','after')),
    window_start_time TIMESTAMPTZ NOT NULL,
    window_end_time TIMESTAMPTZ NOT NULL,
    repos JSONB,
    PRIMARY KEY (user_id, project_id, window_type)
);
CREATE TABLE IF NOT EXISTS processed_keys (
    user_id TEXT NOT NULL,
    project_id INT NOT NULL,
    processed_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, project_id)
);
"""

# Define a DNS error due to network and VPN configuration
# If this error occurs, catch it and skip it temporarily. Do NOT mark the keys as processed!
class RetryableNetworkError(Exception):
    pass

# Load environment
load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

DB_DSN = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME')}"


TOKENS = [t.strip() for t in os.getenv("TOKENS", "").split(",") if t.strip()]
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

# ────── GraphQL ─────────────────────────────────────────────────────
GQL = """
query($login:String!,$from:DateTime!,$to:DateTime!){
  user(login:$login){
    contributionsCollection(from:$from,to:$to){
      commitContributionsByRepository{
        repository{nameWithOwner}
      }
    }
  }
}
"""


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
            
            if resp.status_code in {502, 503, 504}:  
                raise RetryableNetworkError(f"Server error {resp.status_code} for {login}")

            resp.raise_for_status()

            data = resp.json()
            if "errors" in data:
                raise RuntimeError(data["errors"])

            repos = {
                i["repository"]["nameWithOwner"]
                for i in data["data"]["user"]["contributionsCollection"]["commitContributionsByRepository"]
            }
            return repos

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:  
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})
            raise RetryableNetworkError(f"Network timeout or protocol error: {e}") from e

        except socket.gaierror as e:  
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})
            raise RetryableNetworkError(f"DNS resolution failed: {e}") from e

        except RetryableNetworkError as e:  
            print(f"⏳ Retryable error for {login} [{start.date()} → {end.date()}] try {attempt}: {e}")
            if attempt == retries:
                return [] 
            await asyncio.sleep(delay + random.random())
            delay *= 2

        except Exception as e:  
            print(f"Fatal error for {login} [{start.date()} → {end.date()}] try {attempt}: {e}")
            await tm.release(token_state, resp.headers if 'resp' in locals() else {})
            return []


# ────── Database Operations ─────────────────────────────────────────

async def create_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_proj_repo_after_6mon (
            user_id TEXT REFERENCES users(user_id),
            project_id INT REFERENCES projects(project_id),
            window_type TEXT CHECK (window_type IN ('before','after')),
            window_start_time TIMESTAMPTZ NOT NULL,
            window_end_time TIMESTAMPTZ NOT NULL,
            repos JSONB,
            PRIMARY KEY (user_id, project_id, window_type)
        );
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_keys (
            user_id TEXT NOT NULL,
            project_id INT NOT NULL,
            processed_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (user_id, project_id)
        );
    """)

async def copy_before_rows(conn):
    print("Copying 'before' rows...")
    await conn.execute("""
        INSERT INTO user_proj_repo_after_6mon (
            user_id, project_id, window_type, window_start_time, window_end_time, repos
        )
        SELECT user_id, project_id, window_type, window_start_time, window_end_time, repos
        FROM user_proj_repo
        WHERE window_type = 'before'
        ON CONFLICT DO NOTHING;
    """)

async def fetch_unprocessed_rows(conn):
    return await conn.fetch("""
        SELECT upr.user_id, upr.project_id, upr.window_start_time
        FROM user_proj_repo upr
        LEFT JOIN processed_keys pk
        ON upr.user_id = pk.user_id AND upr.project_id = pk.project_id
        WHERE upr.window_type = 'after' AND pk.user_id IS NULL
    """)

async def process_row(row, client, pool, sem) -> bool:
    async with sem:
        user_id = row["user_id"]
        project_id = row["project_id"]
        start = row["window_start_time"]
        end = start + timedelta(days=183)

        try:
            repos = await call_github(user_id, start, end, client)
        except RetryableNetworkError as e:
            print(f"Skipping {user_id}, {project_id} due to retryable network error: {e}")
            return False  
        except Exception as e:
            print(f"call_github failed for {user_id}, using empty repos: {e}")
            repos = []

        repos_to_save = json.dumps(list(repos))

        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO user_proj_repo_after_6mon (
                            user_id, project_id, window_type, window_start_time, window_end_time, repos
                        ) VALUES ($1, $2, 'after', $3, $4, $5)
                        ON CONFLICT DO UPDATE
                    """, user_id, project_id, start, end, repos_to_save)

                    await conn.execute("""
                        INSERT INTO processed_keys (user_id, project_id)
                        VALUES ($1, $2)
                        ON CONFLICT DO UPDATE
                    """, user_id, project_id)
        except Exception as e:
            print(f"Transaction failed (rolled back) at {user_id}, {project_id}: {e}")
            return False

        return True


# ────── Main ────────────────────────────────────────────────────────

async def main():
    pool = await asyncpg.create_pool(dsn=DB_DSN, max_size=20)

    async with pool.acquire() as conn:
        await create_tables(conn)
        await copy_before_rows(conn)
        rows = await fetch_unprocessed_rows(conn)

    print(f"Rows to process: {len(rows)}")
    sem = asyncio.Semaphore(10)
    progress = tqdm(total=len(rows), desc="Processing", unit="row")

    async with httpx.AsyncClient() as client:
        async def wrapped_process_row(row):
            success = await process_row(row, client, pool, sem)
            if success:
                progress.update(1)
            else:
                print(f"Skipped progress update for {row['user_id']}, {row['project_id']} due to insert failure")

        tasks = [wrapped_process_row(row) for row in rows]
        await asyncio.gather(*tasks)

    await pool.close()
    progress.close()
    print("All done!")

if __name__ == "__main__":
    asyncio.run(main())
