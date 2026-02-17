import pandas as pd
import requests
import time
import random
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import threading
import os
from tqdm import tqdm
from datetime import datetime
import pytz


TOKENS = [
    
]  

INPUT_CSV = '../data/hackathon_project.csv'
OUTPUT_CSV = 'hackathon_project_contributor.csv'
THREADS = 10               # Number of total threads
REQUEST_DELAY = (0.1, 0.3) # Random delay range (seconds)
MAX_RETRIES = 3            # Max retry times per request


class TokenManager:
    """Multi-token load balancing manager"""
    def __init__(self, tokens):
        self.tokens = tokens
        self.token_info = {token: {"remaining": 0, "resetAt": 0} for token in tokens}
        self.lock = threading.Lock()

    def update_token_info(self, token, remaining, reset_time):
        """Update remaining request count and reset time for a token"""
        with self.lock:
            self.token_info[token]["remaining"] = remaining
            self.token_info[token]["resetAt"] = reset_time

    def get_best_token(self):
        """Select the best token"""
        with self.lock:
            # Prefer tokens with remaining > 0
            available_tokens = [
                (token, info) for token, info in self.token_info.items() if info["remaining"] > 0
            ]
            if available_tokens:
                best_token = max(available_tokens, key=lambda x: x[1]["remaining"])[0]
                return best_token

            # All tokens exhausted, choose the one that resets earliest
            exhausted_tokens = [
                (token, info) for token, info in self.token_info.items()
            ]
            best_token = min(exhausted_tokens, key=lambda x: x[1]["resetAt"])[0]
            return best_token

# GraphQL and REST API have different rate limits!
class RateLimiter:
    """Rate limiter for GraphQL (modified to use REST API)"""
    def __init__(self, token_manager):
        self.token_manager = token_manager
        self.current_token = None

    def _update_limits(self, token):
        """Update rate limit info using REST API (main change)"""
        headers = {'Authorization': f'token {token}'}
        try:
            response = requests.get('https://api.github.com/rate_limit', headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                core_limit = data['resources']['core']
                remaining = core_limit['remaining']
                reset_time = core_limit['reset']
                
                self.token_manager.update_token_info(token, remaining, reset_time)
                return remaining, reset_time
            else:
                raise Exception(f"HTTP {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f"Failed to update token limit info: {str(e)}")
            raise

    def wait_for_token(self):
        """When all tokens are exhausted, wait for the earliest reset"""
        while True:
            try:
                self.current_token = self.token_manager.get_best_token()
                remaining, reset_time = self._update_limits(self.current_token)
                
                if remaining > 0:
                    return
                else:
                    all_reset_times = [
                        info["resetAt"] 
                        for info in self.token_manager.token_info.values()
                    ]
                    earliest_reset = min(all_reset_times)
                    
                    sleep_time = earliest_reset - time.time()
                    if sleep_time > 0:
                        print(f"All tokens exhausted, waiting for reset: {sleep_time:.1f}s")
                        time.sleep(sleep_time + 5)
                    else:
                        time.sleep(5)
                        
            except Exception as e:
                print(f"Exception while waiting for token: {e}")
                time.sleep(10)

def parse_github_url(url):
    """Parse GitHub repository URL"""
    try:
        parsed = urlparse(url.strip())
        if parsed.netloc != 'github.com':
            return None, None
        path = parsed.path.strip('/').split('/')
        if len(path) < 2:
            return None, None
        return path[0], path[1]
    except:
        return None, None

# REST API version
def fetch_contributors(owner, repo, limiter):
    """Fetch contributors of a repository using REST API (with pagination and retry)"""
    limiter.wait_for_token()
    
    contributors = []
    url = f'https://api.github.com/repos/{owner}/{repo}/contributors'
    headers = {'Authorization': f'token {limiter.current_token}'}
    
    MAX_RETRIES_PER_PAGE = 3
    
    while url:
        retry_count = 0
        page_data = None
        
        while retry_count < MAX_RETRIES_PER_PAGE:
            try:
                response = requests.get(url, headers=headers, params={'per_page': 100}, timeout=20)
                
                if response.status_code == 403:
                    limiter.wait_for_token()
                    headers = {'Authorization': f'token {limiter.current_token}'}
                    retry_count += 1
                    continue
                
                if response.status_code != 200:
                    print(f"Temporary error: {response.status_code}, retrying...")
                    retry_count += 1
                    time.sleep(2 ** retry_count)
                    continue
                
                page_data = response.json()
                break
                
            except Exception as e:
                print(f"Request exception: {str(e)}, retrying...")
                retry_count += 1
                time.sleep(2 ** retry_count)
        
        if not page_data:
            print(f"Failed to fetch page after max retries: {owner}/{repo}")
            break
        
        contributors.extend([f"https://github.com/{user['login']}" for user in page_data if 'login' in user])
        
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            url = None

        remaining = int(response.headers.get('X-RateLimit-Remaining', 1))
        reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
        limiter.token_manager.update_token_info(limiter.current_token, remaining, reset_time)

    return contributors

def process_row(row, limiter):
    """Process a single row of input data"""
    try:
        if pd.isna(row['github_links']):
            return ''
        
        seen = set()
        contributors = []
        
        for link in row['github_links'].split(','):
            link = link.strip()
            if not link or link in seen:
                continue
            seen.add(link)
            
            owner, repo = parse_github_url(link)
            if not owner or not repo:
                continue
            fetched = fetch_contributors(owner, repo, limiter)
            
            if isinstance(fetched, str):
                contributors.append(fetched)
            elif isinstance(fetched, list):
                contributors += fetched
            else:
                print(f"Unexpected type from fetch_contributors: {type(fetched)}")
        
        return ','.join(sorted(set(contributors)))
    except Exception as e:
        print(f"Exception processing row: {str(e)}")
        return ''

def process_dataframe(df):
    token_manager = TokenManager(TOKENS)
    limiter = RateLimiter(token_manager)
        
    df = df.copy()
    df['contributors'] = ''
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {}
        for idx, row in df.iterrows():
            future = executor.submit(
                process_row,
                row,
                limiter
            )
            futures[future] = idx
    
        for future in tqdm(as_completed(futures), total=len(futures)):
            idx = futures[future]
            try:
                df.at[idx, 'contributors'] = future.result()
            except Exception as e:
                print(f"Processing failed: {str(e)}")
                df.at[idx, 'contributors'] = ''

    df.to_csv(OUTPUT_CSV, index=False)

if __name__ == "__main__":
    start_time = time.time()
    df = pd.read_csv(INPUT_CSV)
    process_dataframe(df)
    print(f"Time cost: {time.time()-start_time:.2f} seconds")
