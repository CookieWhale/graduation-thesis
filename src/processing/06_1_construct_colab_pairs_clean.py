import psycopg2
from collections import defaultdict
from tqdm import tqdm
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

conn = psycopg2.connect(
    dbname=dbname, user=user, password=password, host=host, port=port
)
cur = conn.cursor()


cur.execute("""
CREATE TABLE IF NOT EXISTS colab_pairs_clean (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_ids INT[] NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);
""")
conn.commit()

# 1. Build mapping: project_id → [user_id,...]
print("Loading project → users map")
cur.execute("SELECT project_id, user_id FROM user_projects")
project_users = defaultdict(set)
for project_id, user_id in cur.fetchall():
    project_users[project_id].add(user_id)

# 2. Build collaboration pairs
print("Building colab pairs clean...")
pair_projects = defaultdict(set)

for project_id, users in tqdm(project_users.items()):
    user_list = sorted(users)
    for i in range(len(user_list)):
        for j in range(i + 1, len(user_list)):
            u1, u2 = user_list[i], user_list[j]  # user1 < user2
            pair_projects[(u1, u2)].add(project_id)

# 3. Insert into database - batch processing
print("Inserting into colab_pairs_clean...")
data = [
    (u1, u2, list(sorted(proj_ids)))
    for (u1, u2), proj_ids in pair_projects.items()
]

BATCH_SIZE = 10000
for i in tqdm(range(0, len(data), BATCH_SIZE)):
    batch = data[i:i + BATCH_SIZE]
    cur.executemany("""
        INSERT INTO colab_pairs_clean (user1_id, user2_id, project_ids)
        VALUES (%s, %s, %s)
        ON CONFLICT (user1_id, user2_id) DO UPDATE
        SET project_ids = EXCLUDED.project_ids
    """, batch)
    conn.commit()

print("All done.")
cur.close()
conn.close()

# Test:Should return 0 row
# SELECT
#     a.user1_id AS u1,
#     a.user2_id AS u2
# FROM colab_pairs_clean a
# JOIN colab_pairs_clean b
#   ON a.user1_id = b.user2_id
#  AND a.user2_id = b.user1_id
# WHERE a.user1_id < a.user2_id;

