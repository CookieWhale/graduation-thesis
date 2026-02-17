-- Create 8 new tables with correct primary key structure
CREATE TABLE triggered_6m (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE triggered_2y (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE terminated_6m (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE terminated_2y (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE sustained_6m (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE sustained_2y (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE temporary_6m (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

CREATE TABLE temporary_2y (
    user1_id TEXT NOT NULL,
    user2_id TEXT NOT NULL,
    project_id INTEGER NOT NULL,
    before_repos_num INTEGER NOT NULL,
    after_repos_6m_num INTEGER NOT NULL,
    after_repos_2y_num INTEGER NOT NULL,
    PRIMARY KEY (user1_id, user2_id)
);

-- Table 1: triggered_6m (no collaboration before, collaborated within 6 months after)
INSERT INTO triggered_6m
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND array_length(common_repos_after_6m, 1) >= 1

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND array_length(common_repos_after_6m, 1) >= 1;

-- Table 2: triggered_2y (no collaboration before, collaborated within 2 years after)
INSERT INTO triggered_2y
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND array_length(common_repos_after_2y, 1) >= 1

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND array_length(common_repos_after_2y, 1) >= 1;

-- Table 3: terminated_6m (collaborated before, no collaboration within 6 months after)
INSERT INTO terminated_6m
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND (common_repos_after_6m IS NULL OR array_length(common_repos_after_6m, 1) IS NULL OR array_length(common_repos_after_6m, 1) = 0)

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND (common_repos_after_6m IS NULL OR array_length(common_repos_after_6m, 1) IS NULL OR array_length(common_repos_after_6m, 1) = 0);

-- Table 4: terminated_2y (collaborated before, no collaboration within 2 years after)
INSERT INTO terminated_2y
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND (common_repos_after_2y IS NULL OR array_length(common_repos_after_2y, 1) IS NULL OR array_length(common_repos_after_2y, 1) = 0)

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND (common_repos_after_2y IS NULL OR array_length(common_repos_after_2y, 1) IS NULL OR array_length(common_repos_after_2y, 1) = 0);

-- Table 5: sustained_6m (collaborated before, collaborated within 6 months after)
INSERT INTO sustained_6m
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND array_length(common_repos_after_6m, 1) >= 1

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND array_length(common_repos_after_6m, 1) >= 1;

-- Table 6: sustained_2y (collaborated before, collaborated within 2 years after)
INSERT INTO sustained_2y
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND array_length(common_repos_after_2y, 1) >= 1

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE array_length(common_repos_before, 1) >= 1
  AND array_length(common_repos_after_2y, 1) >= 1;

-- Table 7: temporary_6m (no collaboration before, no collaboration within 6 months after)
INSERT INTO temporary_6m
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND (common_repos_after_6m IS NULL OR array_length(common_repos_after_6m, 1) IS NULL OR array_length(common_repos_after_6m, 1) = 0)

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND (common_repos_after_6m IS NULL OR array_length(common_repos_after_6m, 1) IS NULL OR array_length(common_repos_after_6m, 1) = 0);

-- Table 8: temporary_2y (no collaboration before, no collaboration within 2 years after)
INSERT INTO temporary_2y
SELECT 
    user1_id,
    user2_id,
    project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_single_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND (common_repos_after_2y IS NULL OR array_length(common_repos_after_2y, 1) IS NULL OR array_length(common_repos_after_2y, 1) = 0)

UNION ALL

SELECT 
    user1_id,
    user2_id,
    first_proj as project_id,
    COALESCE(array_length(common_repos_before, 1), 0) as before_repos_num,
    COALESCE(array_length(common_repos_after_6m, 1), 0) as after_repos_6m_num,
    COALESCE(array_length(common_repos_after_2y, 1), 0) as after_repos_2y_num
FROM colab_pairs_multi_proj
WHERE (common_repos_before IS NULL OR array_length(common_repos_before, 1) IS NULL OR array_length(common_repos_before, 1) = 0)
  AND (common_repos_after_2y IS NULL OR array_length(common_repos_after_2y, 1) IS NULL OR array_length(common_repos_after_2y, 1) = 0);

-- Statistics Report
SELECT 
    'Original Data' as category,
    'colab_pairs_single_proj' as table_name,
    COUNT(*) as row_count
FROM colab_pairs_single_proj

UNION ALL

SELECT 
    'Original Data',
    'colab_pairs_multi_proj',
    COUNT(*)
FROM colab_pairs_multi_proj

UNION ALL

SELECT 
    'Original Data',
    'single + multi Total',
    (SELECT COUNT(*) FROM colab_pairs_single_proj) + (SELECT COUNT(*) FROM colab_pairs_multi_proj)

UNION ALL

SELECT 
    '6-Month Classification',
    'triggered_6m (Table 1)',
    COUNT(*)
FROM triggered_6m

UNION ALL

SELECT 
    '6-Month Classification',
    'terminated_6m (Table 3)',
    COUNT(*)
FROM terminated_6m

UNION ALL

SELECT 
    '6-Month Classification',
    'sustained_6m (Table 5)',
    COUNT(*)
FROM sustained_6m

UNION ALL

SELECT 
    '6-Month Classification',
    'temporary_6m (Table 7)',
    COUNT(*)
FROM temporary_6m

UNION ALL

SELECT 
    '6-Month Classification',
    'Table 1+3+5+7 Total',
    (SELECT COUNT(*) FROM triggered_6m) + 
    (SELECT COUNT(*) FROM terminated_6m) + 
    (SELECT COUNT(*) FROM sustained_6m) + 
    (SELECT COUNT(*) FROM temporary_6m)

UNION ALL

SELECT 
    '2-Year Classification',
    'triggered_2y (Table 2)',
    COUNT(*)
FROM triggered_2y

UNION ALL

SELECT 
    '2-Year Classification',
    'terminated_2y (Table 4)',
    COUNT(*)
FROM terminated_2y

UNION ALL

SELECT 
    '2-Year Classification',
    'sustained_2y (Table 6)',
    COUNT(*)
FROM sustained_2y

UNION ALL

SELECT 
    '2-Year Classification',
    'temporary_2y (Table 8)',
    COUNT(*)
FROM temporary_2y

UNION ALL

SELECT 
    '2-Year Classification',
    'Table 2+4+6+8 Total',
    (SELECT COUNT(*) FROM triggered_2y) + 
    (SELECT COUNT(*) FROM terminated_2y) + 
    (SELECT COUNT(*) FROM sustained_2y) + 
    (SELECT COUNT(*) FROM temporary_2y)

ORDER BY category, table_name;