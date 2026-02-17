import os
import sys
import time
import asyncio
import requests
import psycopg2
import pytz

from pathlib import Path
from datetime import timedelta
from dotenv import load_dotenv
from tqdm import tqdm


load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

DB_DSN = (
    f"dbname={os.getenv('DB_NAME')} "
    f"user={os.getenv('DB_USER')} "
    f"password={os.getenv('DB_PASSWORD')} "
    f"host={os.getenv('DB_HOST','localhost')} "
    f"port={os.getenv('DB_PORT','5432')}"
)

TOKENS = [t.strip() for t in os.getenv("TOKENS", "").split(",") if t.strip()]
if not TOKENS:
    sys.exit("Set TOKENS=ghp_xxx,... in .env")

GITHUB_API = "https://api.github.com"
PER_PAGE = 100
MAX_RETRIES = 3

class TokenState:
    __slots__ = ("token", "remaining", "reset", "in_use")
    def __init__(self, token):
        self.token = token
        self.remaining = 5000
        self.reset = 0
        self.in_use = False


class TokenManager:
    def __init__(self, tokens, per_token_concurrency=5):
        self.tokens = [TokenState(t) for t in tokens]
        self.permit = asyncio.Semaphore(len(tokens) * per_token_concurrency)
        self.lock = asyncio.Lock()

    async def acquire(self):
        await self.permit.acquire()
        while True:
            async with self.lock:
                now = time.time()
                for t in self.tokens:
                    if t.remaining == 0 and now >= t.reset:
                        t.remaining = 5000
                    if t.remaining > 0 and not t.in_use:
                        t.in_use = True
                        t.remaining -= 1
                        return t

                active = [t for t in self.tokens if t.remaining > 0]
                sleep_sec = 5 if active else max(min(t.reset for t in self.tokens) - now + 5, 5)
                print(f"⏳ Waiting {sleep_sec:.1f}s for available token...")
            await asyncio.sleep(sleep_sec)

    async def release(self, t: TokenState, headers):
        async with self.lock:
            t.in_use = False
            if "X-RateLimit-Remaining" in headers:
                t.remaining = int(headers["X-RateLimit-Remaining"])

        if "X-RateLimit-Reset" in headers:
            try:
                t.reset = float(headers["X-RateLimit-Reset"])
            except:
                t.reset = time.time() + 300
        else:
            t.reset = time.time() + 300

        self.permit.release()


tm = TokenManager(TOKENS)


def inclusive_since(start_date):
    since_dt = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    return since_dt.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

def inclusive_until(end_date):
    until_dt = (end_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return until_dt.astimezone(pytz.utc).strftime('%Y-%m-%dT%H:%M:%SZ') 


async def fetch_repo_contributors(repo, since_iso, until_iso):
    contributors = set()
    page = 1
    retry = 0
    last_error = None

    while True:
        token_state = await tm.acquire()
        headers = {
            "Authorization": f"Bearer {token_state.token}",
            "Accept": "application/vnd.github+json",
        }

        try:
            resp = requests.get(
                f"{GITHUB_API}/repos/{repo}/commits",
                headers=headers,
                params={
                    "since": since_iso,
                    "until": until_iso,
                    "per_page": PER_PAGE,
                    "page": page,
                },
                timeout=30,
            )

            if resp.status_code == 403:
                retry += 1
                last_error = "403 rate limited"
                await tm.release(token_state, resp.headers)
                if retry > MAX_RETRIES:
                    break
                await asyncio.sleep(5)
                continue

            if resp.status_code != 200:
                retry += 1
                last_error = f"HTTP {resp.status_code}"
                await tm.release(token_state, resp.headers)
                if retry > MAX_RETRIES:
                    break
                await asyncio.sleep(2 ** retry)
                continue

            data = resp.json()
            await tm.release(token_state, resp.headers)

            if not data:
                break

            for c in data:
                author = c.get("author")
                if author and author.get("login"):
                    contributors.add(author["login"])

            page += 1
            retry = 0

        except Exception as e:
            await tm.release(token_state, {})
            retry += 1
            last_error = str(e)
            if retry > MAX_RETRIES:
                break
            await asyncio.sleep(2 ** retry)

    success = len(contributors) > 0
    return success, contributors, last_error


async def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("""
        SELECT project_id, github_repos, start_date, end_date
        FROM public.projects_clean
        WHERE github_repos IS NOT NULL
          AND start_date IS NOT NULL
          AND end_date IS NOT NULL
          AND (
                contributors_during_status IS NULL
             OR contributors_during_status = 'partial'
          )
        ORDER BY project_id
    """)

    rows = cur.fetchall()
    print(f"Projects to process: {len(rows)}")

    stats = {"done": 0, "partial": 0, "failed": 0}

    with tqdm(total=len(rows), desc="Processing projects") as pbar:
        for project_id, github_repos, start_date, end_date in rows:
            since_iso = inclusive_since(start_date)
            until_iso = inclusive_until(end_date)

            all_contributors = set()
            success_repos = 0
            failed_repos = 0
            errors = []

            for repo in github_repos:
                ok, users, err = await fetch_repo_contributors(repo, since_iso, until_iso)
                if ok:
                    success_repos += 1
                    all_contributors.update(users)
                else:
                    failed_repos += 1
                    if err:
                        errors.append(f"{repo}: {err}")

            if success_repos > 0 and failed_repos == 0:
                status = "done"
            elif success_repos > 0:
                status = "partial"
            else:
                status = "failed"

            cur.execute(
                """
                UPDATE public.projects_clean
                SET contributors_during = %s,
                    contributors_during_status = %s,
                    contributors_during_error = %s
                WHERE project_id = %s
                """,
                (
                    list(all_contributors),
                    status,
                    "; ".join(errors)[:1000] if errors else None,
                    project_id,
                ),
            )

            stats[status] += 1
            pbar.update(1)
            pbar.set_postfix(stats)

    cur.close()
    conn.close()

    print("Finished.")
    print(stats)


if __name__ == "__main__":
    asyncio.run(main())


