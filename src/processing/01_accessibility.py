import requests
import time
import threading
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from queue import Queue
from datetime import datetime
import pytz

TOKENS = [
]
MAX_WORKERS = 10  # Number of concurrent threads
BATCH_SIZE = 100   # DataFrame batch size
GRAPHQL_BATCH = 100 # Max repos per GraphQL request

class TokenManager:
    """Token load balancing manager"""
    def __init__(self, tokens):
        self.tokens = tokens
        self.token_info = {token: {"remaining": 0, "resetAt": 0} for token in tokens}
        self.lock = threading.Lock()

    def update_token_info(self, token, remaining, reset_time):
        """Update the remaining quota and reset time for a token"""
        with self.lock:
            self.token_info[token]["remaining"] = remaining
            self.token_info[token]["resetAt"] = reset_time

    def get_best_token(self):
        """Choose the best available token"""
        with self.lock:
            # Prefer tokens with remaining > 0
            available_tokens = [
                (token, info) for token, info in self.token_info.items() if info["remaining"] > 0
            ]
            if available_tokens:
                # Choose the one with most remaining
                best_token = max(available_tokens, key=lambda x: x[1]["remaining"])[0]
                return best_token

            # If all tokens are exhausted, choose the one resetting soonest
            exhausted_tokens = [
                (token, info) for token, info in self.token_info.items()
            ]
            best_token = min(exhausted_tokens, key=lambda x: x[1]["resetAt"])[0]
            return best_token

class GraphQLRateLimiter:
    """GraphQL-specific rate limiter"""
    def __init__(self, token_manager):
        self.token_manager = token_manager
        self.current_token = None

    def _update_limits(self, token):
        """Update rate limit info for a token"""
        headers = {'Authorization': f'token {token}'}
        query = '{ rateLimit { remaining resetAt } }'
        response = requests.post('https://api.github.com/graphql', 
                                 json={'query': query}, headers=headers)
        if response.status_code == 200:
            data = response.json()
            rate_limit = data.get("data", {}).get("rateLimit", {})
            remaining = rate_limit.get('remaining')
            
            reset_time_str = rate_limit.get('resetAt')  
            reset_time_utc = datetime.strptime(reset_time_str, "%Y-%m-%dT%H:%M:%SZ")
            reset_time_local = reset_time_utc.replace(tzinfo=pytz.utc).astimezone(pytz.timezone("Europe/Amsterdam"))
            reset_time = reset_time_local.timestamp()

            self.token_manager.update_token_info(token, remaining, reset_time)
            return remaining, reset_time
        else:
            raise Exception(f"Failed to fetch rate limit info for token {token}")

    def wait_for_token(self):
        while True:
            self.current_token = self.token_manager.get_best_token()

            try:
                remaining, reset_time = self._update_limits(self.current_token)

                if remaining > 0:
                    return
                else:
                    sleep_time = reset_time - time.time()
                    if sleep_time > 0:
                        print(f"Token exhausted, sleeping {sleep_time:.1f}s")
                        time.sleep(sleep_time + 5)
            except Exception as e:
                print(f"Error updating token limits: {e}")
                time.sleep(10)

def parse_github_url(url):
    """Parse GitHub URL to return (owner, repo)"""
    parts = url.rstrip('/').split('/')
    if len(parts) >= 5 and parts[2] == 'github.com':
        return parts[3], parts[4]
    return None, None

def batch_check(urls, limiter):
    """Batch check a list of URLs for accessibility"""
    limiter.wait_for_token()
    
    query_parts = []
    valid_urls = []
    for idx, url in enumerate(urls):
        owner, repo = parse_github_url(url)
        if owner and repo: # only when the url is a valid github repo url, this url will have an aliases repo_{idx}
            query_parts.append(f'repo_{idx}: repository(owner: "{owner}", name: "{repo}") {{ id }}')
            valid_urls.append(url)
    
    if not query_parts:
        return {url: False for url in urls}
    
    query = 'query {' + '\n'.join(query_parts) + '}'
    
    headers = {'Authorization': f'token {limiter.current_token}'}
    response = requests.post(
        'https://api.github.com/graphql',
        json={'query': query},
        headers=headers,
        timeout=15
    )
    
    results = {}
    if response.status_code == 200:
        data = response.json().get('data', {})
        for idx, url in enumerate(urls):
            if idx < len(valid_urls): 
            # if url in valid_urls:
                repo_key = f'repo_{idx}'
                results[url] = repo_key in data and data[repo_key] is not None
            else: # other urls which do NOT have aliases repo_{idx}
                results[url] = False
    else:
        print(f"GraphQL request failed: {response.status_code}")
        results = {url: False for url in urls}
    
    remaining = limiter.token_manager.token_info[limiter.current_token]["remaining"]
    limiter.token_manager.update_token_info(
        limiter.current_token,
        remaining - 1,
        limiter.token_manager.token_info[limiter.current_token]["resetAt"]
    )
    
    return results

def process_row(urls_str, limiter):
    """Process a single row (optimized version)"""
    try:
        urls = urls_str.split(',')
        all_results = []
        
        for i in range(0, len(urls), GRAPHQL_BATCH):
            batch = urls[i:i+GRAPHQL_BATCH]
            results = batch_check(batch, limiter)
            all_results.extend([results[url] for url in batch])
        
        return all(all_results)
    except Exception as e:
        print(f"Exception processing rows: {str(e)}")
        return False

def process_dataframe(df):
    """Process entire DataFrame"""
    token_manager = TokenManager(TOKENS)
    limiter = GraphQLRateLimiter(token_manager)
    
    df = df.copy()
    df['accessibility'] = False
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}
        for idx, row in df.iterrows():
            future = executor.submit(
                process_row,
                row['github_links'],
                limiter
            )
            futures[future] = idx
        
        for future in tqdm(as_completed(futures), total=len(futures)):
            idx = futures[future]
            try:
                df.at[idx, 'accessibility'] = future.result()
            except Exception as e:
                print(f"Failed to process row: {str(e)}")
                df.at[idx, 'accessibility'] = False
    
    return df

if __name__ == "__main__":
    project = pd.read_csv('../data/projects.csv')

    project_filtered = project[['submitted_to_link', 'project_URL', 'github_links']]
    project_filtered = project_filtered.dropna(subset=['submitted_to_link', 'project_URL', 'github_links'])

    test_df = project_filtered.copy()
    processed_df = process_dataframe(test_df)

    target_columns = ['project_URL', 'github_links', 'submitted_to_link', 'accessibility']
    processed_df = processed_df[target_columns].reset_index(drop=True)

    processed_df.to_csv(
        "projects_github_accessibility.csv", 
        index=False, 
        encoding='utf-8',
        chunksize=1000
    )
