-- ============================================
-- Add hackathon-related fields to colab_pairs tables
-- Data size: single table ~109,500 rows, multi table ~10,500 rows
-- Environment: 8GB RAM laptop, local database
-- ============================================

-- ===== Part 1: Preparation =====

-- 1. Set memory parameters
SET work_mem = '128MB';
SET maintenance_work_mem = '512MB';
SET temp_buffers = '64MB';

-- 2. Create index to speed up queries (if not exists)
CREATE INDEX IF NOT EXISTS idx_projects_pid_hid 
ON projects_clean(project_id, hackathon_id);

-- 3. Check current status
SELECT 'single table' as table_name, COUNT(*) as row_count
FROM colab_pairs_single_proj
UNION ALL
SELECT 'multi table' as table_name, COUNT(*) as row_count
FROM colab_pairs_multi_proj;


-- ===== Part 2: Update single table =====

DO $$
DECLARE
    start_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Processing colab_pairs_single_proj table';
    RAISE NOTICE 'Start time: %', start_time;
    RAISE NOTICE '========================================';
END $$;

-- 1. Add hackathon_id field
ALTER TABLE colab_pairs_single_proj
ADD COLUMN IF NOT EXISTS hackathon_id integer;

-- 2. Update hackathon_id in one batch
DO $$
DECLARE
    start_time TIMESTAMP;
    rows_affected INTEGER;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE 'Updating hackathon_id...';
    
    UPDATE colab_pairs_single_proj cp
    SET hackathon_id = p.hackathon_id
    FROM projects_clean p
    WHERE cp.project_id = p.project_id
      AND cp.hackathon_id IS NULL;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    RAISE NOTICE 'Update completed!';
    RAISE NOTICE '  Rows updated: %', rows_affected;
    RAISE NOTICE '  Time elapsed: %', clock_timestamp() - start_time;
    RAISE NOTICE '========================================';
END $$;

-- 3. Verify single table update results
SELECT 
    'single table verification' as status,
    COUNT(*) as total_rows,
    COUNT(hackathon_id) as rows_with_hackathon_id,
    COUNT(*) - COUNT(hackathon_id) as null_rows,
    ROUND(COUNT(hackathon_id)::NUMERIC / COUNT(*) * 100, 2) as completion_percentage
FROM colab_pairs_single_proj;

-- 4. Show sample data from single table
SELECT 
    user1_id,
    user2_id,
    project_id,
    hackathon_id,
    first_proj_start
FROM colab_pairs_single_proj
WHERE hackathon_id IS NOT NULL
LIMIT 5;


-- ===== Part 3: Update multi table =====

DO $$
DECLARE
    start_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Processing colab_pairs_multi_proj table';
    RAISE NOTICE 'Start time: %', start_time;
    RAISE NOTICE '========================================';
END $$;

-- 1. Add new fields
ALTER TABLE colab_pairs_multi_proj
ADD COLUMN IF NOT EXISTS first_hack integer,
ADD COLUMN IF NOT EXISTS last_hack integer,
ADD COLUMN IF NOT EXISTS hack_ids integer[];

-- 2. Update first_hack
DO $$
DECLARE
    start_time TIMESTAMP;
    rows_affected INTEGER;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE 'Updating first_hack...';
    
    UPDATE colab_pairs_multi_proj cp
    SET first_hack = p.hackathon_id
    FROM projects_clean p
    WHERE cp.first_proj = p.project_id
      AND cp.first_hack IS NULL;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    RAISE NOTICE '  Rows updated: %', rows_affected;
    RAISE NOTICE '  Time elapsed: %', clock_timestamp() - start_time;
END $$;

-- 3. Update last_hack
DO $$
DECLARE
    start_time TIMESTAMP;
    rows_affected INTEGER;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE 'Updating last_hack...';
    
    UPDATE colab_pairs_multi_proj cp
    SET last_hack = p.hackathon_id
    FROM projects_clean p
    WHERE cp.last_proj = p.project_id
      AND cp.last_hack IS NULL;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    RAISE NOTICE '  Rows updated: %', rows_affected;
    RAISE NOTICE '  Time elapsed: %', clock_timestamp() - start_time;
END $$;

-- 4. Update hack_ids (aggregate array)
DO $$
DECLARE
    start_time TIMESTAMP;
    rows_affected INTEGER;
BEGIN
    start_time := clock_timestamp();
    
    RAISE NOTICE 'Updating hack_ids (this may take a bit longer)...';
    
    UPDATE colab_pairs_multi_proj cp
    SET hack_ids = (
        SELECT ARRAY_AGG(DISTINCT p.hackathon_id ORDER BY p.hackathon_id)
        FROM unnest(cp.project_ids) AS pid
        JOIN projects_clean p ON p.project_id = pid
        WHERE p.hackathon_id IS NOT NULL
    )
    WHERE cp.hack_ids IS NULL;
    
    GET DIAGNOSTICS rows_affected = ROW_COUNT;
    
    RAISE NOTICE 'Update completed!';
    RAISE NOTICE '  Rows updated: %', rows_affected;
    RAISE NOTICE '  Time elapsed: %', clock_timestamp() - start_time;
    RAISE NOTICE '========================================';
END $$;

-- 5. Verify multi table update results
SELECT 
    'multi table verification' as status,
    COUNT(*) as total_rows,
    COUNT(first_hack) as rows_with_first_hack,
    COUNT(last_hack) as rows_with_last_hack,
    COUNT(hack_ids) as rows_with_hack_ids,
    COUNT(*) FILTER (WHERE first_hack IS NULL) as null_first_hack,
    COUNT(*) FILTER (WHERE last_hack IS NULL) as null_last_hack,
    COUNT(*) FILTER (WHERE hack_ids IS NULL) as null_hack_ids,
    ROUND(COUNT(hack_ids)::NUMERIC / COUNT(*) * 100, 2) as completion_percentage
FROM colab_pairs_multi_proj;

-- 6. Show data distribution in multi table
SELECT 
    array_length(project_ids, 1) as num_projects,
    array_length(hack_ids, 1) as num_hackathons,
    COUNT(*) as num_pairs,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 2) as percentage
FROM colab_pairs_multi_proj
WHERE hack_ids IS NOT NULL
GROUP BY 
    array_length(project_ids, 1),
    array_length(hack_ids, 1)
ORDER BY num_projects, num_hackathons;

-- 7. Show sample data from multi table
SELECT 
    user1_id,
    user2_id,
    array_length(project_ids, 1) as num_projects,
    first_proj,
    last_proj,
    first_hack,
    last_hack,
    hack_ids,
    array_length(hack_ids, 1) as num_hackathons
FROM colab_pairs_multi_proj
WHERE hack_ids IS NOT NULL
ORDER BY array_length(project_ids, 1) DESC
LIMIT 10;


-- ===== Part 4: Data consistency checks =====

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Starting data consistency checks';
    RAISE NOTICE '========================================';
END $$;

-- 1. Check single table: rows with project_id but no hackathon_id
SELECT 
    '[single table] Missing hackathon_id' as check_item,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL - Issues found'
    END as status
FROM colab_pairs_single_proj
WHERE project_id IS NOT NULL 
  AND hackathon_id IS NULL;

-- 2. Check multi table: first_hack should be in hack_ids
SELECT 
    '[multi table] first_hack not in hack_ids' as check_item,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL - Issues found'
    END as status
FROM colab_pairs_multi_proj
WHERE first_hack IS NOT NULL 
  AND hack_ids IS NOT NULL
  AND NOT (first_hack = ANY(hack_ids));

-- 3. Check multi table: last_hack should be in hack_ids
SELECT 
    '[multi table] last_hack not in hack_ids' as check_item,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL - Issues found'
    END as status
FROM colab_pairs_multi_proj
WHERE last_hack IS NOT NULL 
  AND hack_ids IS NOT NULL
  AND NOT (last_hack = ANY(hack_ids));

-- 4. Check multi table: hack_ids count should be <= project_ids count
SELECT 
    '[multi table] hack_ids count abnormal' as check_item,
    COUNT(*) as count,
    CASE 
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL - hack_ids has more items than project_ids'
    END as status
FROM colab_pairs_multi_proj
WHERE array_length(hack_ids, 1) > array_length(project_ids, 1);

-- 5. Show problematic data (if any)
DO $$
DECLARE
    problem_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO problem_count
    FROM colab_pairs_multi_proj
    WHERE hack_ids IS NULL 
      AND array_length(project_ids, 1) > 0;
    
    IF problem_count > 0 THEN
        RAISE NOTICE 'Found % rows with issues (has project_ids but no hack_ids)', problem_count;
        RAISE NOTICE 'Please check the query results below:';
    ELSE
        RAISE NOTICE 'No problematic data found';
    END IF;
END $$;

SELECT 
    user1_id,
    user2_id,
    project_ids,
    hack_ids,
    first_proj,
    last_proj,
    first_hack,
    last_hack
FROM colab_pairs_multi_proj
WHERE hack_ids IS NULL 
  AND array_length(project_ids, 1) > 0
LIMIT 5;


-- ===== Part 5: Summary statistics =====

DO $$
BEGIN
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Summary Statistics';
    RAISE NOTICE '========================================';
END $$;

-- 1. Overall statistics
SELECT 
    'single table' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT hackathon_id) as unique_hackathons,
    NULL::numeric as avg_projects_per_pair,
    NULL::numeric as avg_hackathons_per_pair
FROM colab_pairs_single_proj
WHERE hackathon_id IS NOT NULL
UNION ALL
SELECT 
    'multi table' as table_name,
    COUNT(*) as total_rows,
    COUNT(DISTINCT unnest(hack_ids)) as unique_hackathons,
    ROUND(AVG(array_length(project_ids, 1)), 2) as avg_projects_per_pair,
    ROUND(AVG(array_length(hack_ids, 1)), 2) as avg_hackathons_per_pair
FROM colab_pairs_multi_proj
WHERE hack_ids IS NOT NULL;

-- 2. Relationship between project count and hackathon count in multi table
SELECT 
    array_length(project_ids, 1) as num_projects,
    MIN(array_length(hack_ids, 1)) as min_hackathons,
    ROUND(AVG(array_length(hack_ids, 1)), 2) as avg_hackathons,
    MAX(array_length(hack_ids, 1)) as max_hackathons,
    COUNT(*) as num_pairs
FROM colab_pairs_multi_proj
WHERE hack_ids IS NOT NULL
GROUP BY array_length(project_ids, 1)
ORDER BY num_projects;

-- 3. Most active collaboration pairs (multi table)
SELECT 
    user1_id,
    user2_id,
    array_length(project_ids, 1) as num_projects,
    array_length(hack_ids, 1) as num_hackathons,
    hack_ids
FROM colab_pairs_multi_proj
WHERE hack_ids IS NOT NULL
ORDER BY 
    array_length(project_ids, 1) DESC,
    array_length(hack_ids, 1) DESC
LIMIT 10;



-- Check table sizes
SELECT 
    'colab_pairs_single_proj' as table_name,
    pg_size_pretty(pg_total_relation_size('colab_pairs_single_proj')) as total_size,
    pg_size_pretty(pg_relation_size('colab_pairs_single_proj')) as table_size,
    pg_size_pretty(pg_indexes_size('colab_pairs_single_proj')) as indexes_size
UNION ALL
SELECT 
    'colab_pairs_multi_proj' as table_name,
    pg_size_pretty(pg_total_relation_size('colab_pairs_multi_proj')) as total_size,
    pg_size_pretty(pg_relation_size('colab_pairs_multi_proj')) as table_size,
    pg_size_pretty(pg_indexes_size('colab_pairs_multi_proj')) as indexes_size;

