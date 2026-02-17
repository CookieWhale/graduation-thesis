-- ============================================
-- Complete Resumable Batch Processing Script (FIXED)
-- For 8GB RAM laptop with checkpoint support
-- ============================================

-- ============================================
-- CONFIGURATION
-- ============================================
SET client_min_messages TO NOTICE;

-- Memory allocation (Conservative)
SET work_mem = '128MB';
SET maintenance_work_mem = '256MB';
SET temp_buffers = '64MB';

-- ==========================================
-- PHASE 1: Pre-compute avg_outside_repo_contributors in user_proj_repo tables
-- ==========================================

-- -- 1.1 Add column to user_proj_repo (2-year window)
-- DO $$ 
-- BEGIN
--     IF NOT EXISTS (
--         SELECT 1 FROM information_schema.columns 
--         WHERE table_name = 'user_proj_repo' 
--         AND column_name = 'avg_outside_repo_contributors'
--     ) THEN
--         ALTER TABLE user_proj_repo ADD COLUMN avg_outside_repo_contributors NUMERIC(10,2);
--         RAISE NOTICE 'Added avg_outside_repo_contributors to user_proj_repo';
--     ELSE
--         RAISE NOTICE 'Column avg_outside_repo_contributors already exists in user_proj_repo';
--     END IF;
-- END $$;

-- -- 1.2 Add column to user_proj_repo_after_6mon (6-month window)
-- DO $$ 
-- BEGIN
--     IF NOT EXISTS (
--         SELECT 1 FROM information_schema.columns 
--         WHERE table_name = 'user_proj_repo_after_6mon' 
--         AND column_name = 'avg_outside_repo_contributors'
--     ) THEN
--         ALTER TABLE user_proj_repo_after_6mon ADD COLUMN avg_outside_repo_contributors NUMERIC(10,2);
--         RAISE NOTICE 'Added avg_outside_repo_contributors to user_proj_repo_after_6mon';
--     ELSE
--         RAISE NOTICE 'Column avg_outside_repo_contributors already exists in user_proj_repo_after_6mon';
--     END IF;
-- END $$;

-- -- 1.3 Calculate avg_outside_repo_contributors for user_proj_repo (2-year, only update NULL values)
-- DO $$
-- DECLARE
--     rows_to_update INTEGER;
-- BEGIN
--     SELECT COUNT(*) INTO rows_to_update
--     FROM user_proj_repo
--     WHERE window_type = 'before'
--       AND avg_outside_repo_contributors IS NULL;
    
--     RAISE NOTICE 'user_proj_repo: % rows to update', rows_to_update;
    
--     IF rows_to_update > 0 THEN
--         WITH repo_contributors AS (
--             SELECT 
--                 u.user_id,
--                 u.project_id,
--                 u.window_type,
--                 repo_element.repo_name,
--                 r.contributor_count
--             FROM user_proj_repo u
--             CROSS JOIN LATERAL jsonb_array_elements_text(u.repos_outside) AS repo_element(repo_name)
--             LEFT JOIN repo_contributor_cache r 
--                 ON repo_element.repo_name = r.repo_full_name
--             WHERE u.window_type = 'before'
--               AND u.repos_outside IS NOT NULL
--               AND jsonb_array_length(u.repos_outside) > 0
--               AND u.avg_outside_repo_contributors IS NULL
--               AND r.contributor_count > 0  -- Filter out 0, -1, and NULL
--         )
--         UPDATE user_proj_repo u
--         SET avg_outside_repo_contributors = COALESCE(subq.avg_contributors, 0)
--         FROM (
--             SELECT 
--                 user_id,
--                 project_id,
--                 window_type,
--                 AVG(contributor_count)::numeric(10,2) as avg_contributors
--             FROM repo_contributors
--             GROUP BY user_id, project_id, window_type
--         ) subq
--         WHERE u.user_id = subq.user_id 
--           AND u.project_id = subq.project_id 
--           AND u.window_type = subq.window_type
--           AND u.avg_outside_repo_contributors IS NULL;
        
--         -- Set 0 for records with no valid repos_outside
--         UPDATE user_proj_repo 
--         SET avg_outside_repo_contributors = 0 
--         WHERE window_type = 'before' 
--           AND avg_outside_repo_contributors IS NULL;
        
--         RAISE NOTICE 'user_proj_repo: Update completed';
--     ELSE
--         RAISE NOTICE 'user_proj_repo: Already up to date, skipping';
--     END IF;
-- END $$;

-- -- 1.4 Calculate avg_outside_repo_contributors for user_proj_repo_after_6mon (6-month, only update NULL values)
-- DO $$
-- DECLARE
--     rows_to_update INTEGER;
-- BEGIN
--     SELECT COUNT(*) INTO rows_to_update
--     FROM user_proj_repo_after_6mon
--     WHERE window_type = 'before'
--       AND avg_outside_repo_contributors IS NULL;
    
--     RAISE NOTICE 'user_proj_repo_after_6mon: % rows to update', rows_to_update;
    
--     IF rows_to_update > 0 THEN
--         WITH repo_contributors AS (
--             SELECT 
--                 u.user_id,
--                 u.project_id,
--                 u.window_type,
--                 repo_element.repo_name,
--                 r.contributor_count
--             FROM user_proj_repo_after_6mon u
--             CROSS JOIN LATERAL jsonb_array_elements_text(u.repos_outside) AS repo_element(repo_name)
--             LEFT JOIN repo_contributor_cache r 
--                 ON repo_element.repo_name = r.repo_full_name
--             WHERE u.window_type = 'before'
--               AND u.repos_outside IS NOT NULL
--               AND jsonb_array_length(u.repos_outside) > 0
--               AND u.avg_outside_repo_contributors IS NULL
--               AND r.contributor_count > 0  -- Filter out 0, -1, and NULL
--         )
--         UPDATE user_proj_repo_after_6mon u
--         SET avg_outside_repo_contributors = COALESCE(subq.avg_contributors, 0)
--         FROM (
--             SELECT 
--                 user_id,
--                 project_id,
--                 window_type,
--                 AVG(contributor_count)::numeric(10,2) as avg_contributors
--             FROM repo_contributors
--             GROUP BY user_id, project_id, window_type
--         ) subq
--         WHERE u.user_id = subq.user_id 
--           AND u.project_id = subq.project_id 
--           AND u.window_type = subq.window_type
--           AND u.avg_outside_repo_contributors IS NULL;
        
--         -- Set 0 for records with no valid repos_outside
--         UPDATE user_proj_repo_after_6mon 
--         SET avg_outside_repo_contributors = 0 
--         WHERE window_type = 'before' 
--           AND avg_outside_repo_contributors IS NULL;
        
--         RAISE NOTICE 'user_proj_repo_after_6mon: Update completed';
--     ELSE
--         RAISE NOTICE 'user_proj_repo_after_6mon: Already up to date, skipping';
--     END IF;
-- END $$;

-- ==========================================
-- PHASE 2: Add columns to 8 pair tables
-- ==========================================

DO $$
DECLARE
    table_names TEXT[] := ARRAY[
        'triggered_6m', 'triggered_2y',
        'terminated_6m', 'terminated_2y',
        'sustained_6m', 'sustained_2y',
        'temporary_6m', 'temporary_2y'
    ];
    tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY table_names LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = tbl
              AND column_name = 'avg_outside_repos_before'
        ) THEN
            EXECUTE format(
                'ALTER TABLE public.%I ADD COLUMN avg_outside_repos_before NUMERIC(10,2)',
                tbl
            );
        END IF;
    END LOOP;
END $$;


-- ==========================================
-- PHASE 3: Update 6-month tables (from user_proj_repo_after_6mon)
-- ==========================================

-- 3.1 Update triggered_6m
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM triggered_6m
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'triggered_6m: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE triggered_6m t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo_after_6mon u1, user_proj_repo_after_6mon u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'triggered_6m: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'triggered_6m: Already up to date, skipping';
    END IF;
END $$;

-- 3.2 Update terminated_6m
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM terminated_6m
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'terminated_6m: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE terminated_6m t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo_after_6mon u1, user_proj_repo_after_6mon u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'terminated_6m: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'terminated_6m: Already up to date, skipping';
    END IF;
END $$;

-- 3.3 Update sustained_6m
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM sustained_6m
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'sustained_6m: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE sustained_6m t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo_after_6mon u1, user_proj_repo_after_6mon u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'sustained_6m: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'sustained_6m: Already up to date, skipping';
    END IF;
END $$;

-- 3.4 Update temporary_6m
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM temporary_6m
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'temporary_6m: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE temporary_6m t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0

        FROM user_proj_repo_after_6mon u1, user_proj_repo_after_6mon u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'temporary_6m: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'temporary_6m: Already up to date, skipping';
    END IF;
END $$;

-- ==========================================
-- PHASE 4: Update 2-year tables (from user_proj_repo)
-- ==========================================

-- 4.1 Update triggered_2y
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM triggered_2y
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'triggered_2y: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE triggered_2y t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo u1, user_proj_repo u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'triggered_2y: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'triggered_2y: Already up to date, skipping';
    END IF;
END $$;

-- 4.2 Update terminated_2y
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM terminated_2y
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'terminated_2y: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE terminated_2y t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo u1, user_proj_repo u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'terminated_2y: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'terminated_2y: Already up to date, skipping';
    END IF;
END $$;

-- 4.3 Update sustained_2y
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM sustained_2y
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'sustained_2y: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE sustained_2y t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo u1, user_proj_repo u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'sustained_2y: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'sustained_2y: Already up to date, skipping';
    END IF;
END $$;

-- 4.4 Update temporary_2y
DO $$
DECLARE
    rows_to_update INTEGER;
    rows_updated INTEGER;
BEGIN
    SELECT COUNT(*) INTO rows_to_update
    FROM temporary_2y
    WHERE avg_outside_repos_before IS NULL;
    
    RAISE NOTICE 'temporary_2y: % rows to update', rows_to_update;
    
    IF rows_to_update > 0 THEN
        UPDATE temporary_2y t
        SET 
            avg_outside_repos_before = (
                COALESCE(jsonb_array_length(u1.repos_outside), 0) + 
                COALESCE(jsonb_array_length(u2.repos_outside), 0)
            ) / 2.0
        FROM user_proj_repo u1, user_proj_repo u2
        WHERE t.user1_id = u1.user_id 
          AND t.project_id = u1.project_id 
          AND u1.window_type = 'before'
          AND t.user2_id = u2.user_id 
          AND t.project_id = u2.project_id 
          AND u2.window_type = 'before'
          AND (t.avg_outside_repos_before IS NULL);
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        RAISE NOTICE 'temporary_2y: Updated % rows', rows_updated;
    ELSE
        RAISE NOTICE 'temporary_2y: Already up to date, skipping';
    END IF;
END $$;

-- ==========================================
-- PHASE 5: Verification
-- ==========================================

SELECT 
    'triggered_6m' as table_name,
    COUNT(*) as total,
    COUNT(avg_outside_repos_before) as repos_non_null,
    ROUND(AVG(avg_outside_repos_before)::numeric, 2) as avg_repos
FROM triggered_6m
UNION ALL
SELECT 'triggered_2y', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2)  FROM triggered_2y
UNION ALL
SELECT 'terminated_6m', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2)  FROM terminated_6m
UNION ALL
SELECT 'terminated_2y', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2)  FROM terminated_2y
UNION ALL
SELECT 'sustained_6m', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2) FROM sustained_6m
UNION ALL
SELECT 'sustained_2y', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2) FROM sustained_2y
UNION ALL
SELECT 'temporary_6m', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2)  FROM temporary_6m
UNION ALL
SELECT 'temporary_2y', COUNT(*), COUNT(avg_outside_repos_before), 
       ROUND(AVG(avg_outside_repos_before)::numeric, 2) FROM temporary_2y
ORDER BY table_name;