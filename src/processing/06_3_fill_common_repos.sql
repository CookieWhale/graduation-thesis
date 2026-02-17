-- ============================================
-- Batch Update Script with Resumable Progress
-- Add common_repos columns to colab_pairs tables
-- FIXED VERSION: Use NULL for unprocessed rows instead of empty arrays
-- ============================================

-- Set working memory (adjust based on your laptop)
SET work_mem = '256MB';
SET maintenance_work_mem = '512MB';

-- ============================================
-- Step 0: Drop existing columns if they exist (cleanup from previous runs)
-- ============================================

ALTER TABLE colab_pairs_single_proj 
DROP COLUMN IF EXISTS common_repos_before,
DROP COLUMN IF EXISTS common_repos_after_2y,
DROP COLUMN IF EXISTS common_repos_after_2y_continuation_inc,
DROP COLUMN IF EXISTS common_repos_after_6m,
DROP COLUMN IF EXISTS common_repos_after_6m_continuation_inc;

ALTER TABLE colab_pairs_multi_proj 
DROP COLUMN IF EXISTS common_repos_before,
DROP COLUMN IF EXISTS common_repos_after_2y,
DROP COLUMN IF EXISTS common_repos_after_2y_continuation_inc,
DROP COLUMN IF EXISTS common_repos_after_6m,
DROP COLUMN IF EXISTS common_repos_after_6m_continuation_inc;

COMMIT;

-- ============================================
-- Step 1: Add columns to both tables (NULL as default)
-- ============================================

-- Add columns to colab_pairs_single_proj
ALTER TABLE colab_pairs_single_proj 
ADD COLUMN common_repos_before text[],
ADD COLUMN common_repos_after_2y text[],
ADD COLUMN common_repos_after_2y_continuation_inc text[],
ADD COLUMN common_repos_after_6m text[],
ADD COLUMN common_repos_after_6m_continuation_inc text[];

-- Add columns to colab_pairs_multi_proj
ALTER TABLE colab_pairs_multi_proj 
ADD COLUMN common_repos_before text[],
ADD COLUMN common_repos_after_2y text[],
ADD COLUMN common_repos_after_2y_continuation_inc text[],
ADD COLUMN common_repos_after_6m text[],
ADD COLUMN common_repos_after_6m_continuation_inc text[];

COMMIT;

-- ============================================
-- Helper Function: Convert JSONB array to text array
-- ============================================
CREATE OR REPLACE FUNCTION jsonb_array_to_text_array(j jsonb)
RETURNS text[] AS $$
BEGIN
    IF j IS NULL THEN
        RETURN ARRAY[]::text[];
    END IF;
    RETURN ARRAY(SELECT jsonb_array_elements_text(j));
EXCEPTION
    WHEN OTHERS THEN
        RETURN ARRAY[]::text[];
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================
-- Helper Function: Calculate intersection of two text arrays
-- ============================================
CREATE OR REPLACE FUNCTION array_intersect(arr1 text[], arr2 text[])
RETURNS text[] AS $$
BEGIN
    IF arr1 IS NULL OR arr2 IS NULL OR array_length(arr1, 1) IS NULL OR array_length(arr2, 1) IS NULL THEN
        RETURN ARRAY[]::text[];
    END IF;
    RETURN ARRAY(
        SELECT UNNEST(arr1)
        INTERSECT
        SELECT UNNEST(arr2)
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMIT;

-- ============================================
-- Step 2: Update colab_pairs_single_proj.common_repos_before
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    -- Get total rows to update
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_single_proj 
    WHERE common_repos_before IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_single_proj.common_repos_before';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, project_id
            FROM colab_pairs_single_proj
            WHERE common_repos_before IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.project_id,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.project_id = upr.project_id 
                AND upr.window_type = 'before'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.project_id,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.project_id = upr.project_id 
                AND upr.window_type = 'before'
        )
        UPDATE colab_pairs_single_proj cps
        SET common_repos_before = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cps.user1_id = ur.user1_id 
            AND cps.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        -- Exit if no rows updated
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_before update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 3: Update colab_pairs_single_proj.common_repos_after_2y
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_single_proj 
    WHERE common_repos_after_2y IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_single_proj.common_repos_after_2y';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, project_id
            FROM colab_pairs_single_proj
            WHERE common_repos_after_2y IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.project_id,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.project_id = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.project_id,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.project_id = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_single_proj cps
        SET common_repos_after_2y = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cps.user1_id = ur.user1_id 
            AND cps.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_2y update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 4: Update colab_pairs_single_proj.common_repos_after_2y_continuation_inc
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_single_proj 
    WHERE common_repos_after_2y_continuation_inc IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_single_proj.common_repos_after_2y_continuation_inc';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, project_id
            FROM colab_pairs_single_proj
            WHERE common_repos_after_2y_continuation_inc IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.project_id,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.project_id = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.project_id,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.project_id = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_single_proj cps
        SET common_repos_after_2y_continuation_inc = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cps.user1_id = ur.user1_id 
            AND cps.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_2y_continuation_inc update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 5: Update colab_pairs_single_proj.common_repos_after_6m
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_single_proj 
    WHERE common_repos_after_6m IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_single_proj.common_repos_after_6m';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, project_id
            FROM colab_pairs_single_proj
            WHERE common_repos_after_6m IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.project_id,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON b.user1_id = upr.user_id 
                AND b.project_id = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.project_id,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON ur.user2_id = upr.user_id 
                AND ur.project_id = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_single_proj cps
        SET common_repos_after_6m = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cps.user1_id = ur.user1_id 
            AND cps.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_6m update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 6: Update colab_pairs_single_proj.common_repos_after_6m_continuation_inc
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_single_proj 
    WHERE common_repos_after_6m_continuation_inc IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_single_proj.common_repos_after_6m_continuation_inc';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, project_id
            FROM colab_pairs_single_proj
            WHERE common_repos_after_6m_continuation_inc IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.project_id,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON b.user1_id = upr.user_id 
                AND b.project_id = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.project_id,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON ur.user2_id = upr.user_id 
                AND ur.project_id = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_single_proj cps
        SET common_repos_after_6m_continuation_inc = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cps.user1_id = ur.user1_id 
            AND cps.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_6m_continuation_inc update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 7: Update colab_pairs_multi_proj.common_repos_before
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_multi_proj 
    WHERE common_repos_before IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_multi_proj.common_repos_before';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, first_proj
            FROM colab_pairs_multi_proj
            WHERE common_repos_before IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.first_proj,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.first_proj = upr.project_id 
                AND upr.window_type = 'before'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.first_proj,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.first_proj = upr.project_id 
                AND upr.window_type = 'before'
        )
        UPDATE colab_pairs_multi_proj cpm
        SET common_repos_before = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cpm.user1_id = ur.user1_id 
            AND cpm.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_before update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 8: Update colab_pairs_multi_proj.common_repos_after_2y
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_multi_proj 
    WHERE common_repos_after_2y IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_multi_proj.common_repos_after_2y';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, last_proj
            FROM colab_pairs_multi_proj
            WHERE common_repos_after_2y IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.last_proj,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.last_proj,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_multi_proj cpm
        SET common_repos_after_2y = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cpm.user1_id = ur.user1_id 
            AND cpm.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_2y update completed, total time: %', clock_timestamp() - start_time;
END $$;


-- ============================================
-- Step 9: Update colab_pairs_multi_proj.common_repos_after_2y_continuation_inc
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_multi_proj 
    WHERE common_repos_after_2y_continuation_inc IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_multi_proj.common_repos_after_2y_continuation_inc';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, last_proj
            FROM colab_pairs_multi_proj
            WHERE common_repos_after_2y_continuation_inc IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.last_proj,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo upr 
                ON b.user1_id = upr.user_id 
                AND b.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.last_proj,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo upr 
                ON ur.user2_id = upr.user_id 
                AND ur.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_multi_proj cpm
        SET common_repos_after_2y_continuation_inc = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cpm.user1_id = ur.user1_id 
            AND cpm.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_2y_continuation_inc update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 10: Update colab_pairs_multi_proj.common_repos_after_6m
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_multi_proj 
    WHERE common_repos_after_6m IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_multi_proj.common_repos_after_6m';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, last_proj
            FROM colab_pairs_multi_proj
            WHERE common_repos_after_6m IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.last_proj,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON b.user1_id = upr.user_id 
                AND b.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.last_proj,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos_outside), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON ur.user2_id = upr.user_id 
                AND ur.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_multi_proj cpm
        SET common_repos_after_6m = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cpm.user1_id = ur.user1_id 
            AND cpm.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_6m update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Step 11: Update colab_pairs_multi_proj.common_repos_after_6m_continuation_inc
-- ============================================

DO $$
DECLARE
    batch_size INT := 5000;
    total_rows INT;
    rows_updated INT := 0;
    total_processed INT := 0;
    start_time TIMESTAMP;
    batch_start_time TIMESTAMP;
BEGIN
    SELECT COUNT(*) INTO total_rows 
    FROM colab_pairs_multi_proj 
    WHERE common_repos_after_6m_continuation_inc IS NULL;
    
    RAISE NOTICE 'Starting update of colab_pairs_multi_proj.common_repos_after_6m_continuation_inc';
    RAISE NOTICE 'Rows to process: %', total_rows;
    start_time := clock_timestamp();
    
    LOOP
        batch_start_time := clock_timestamp();
        
        WITH batch AS (
            SELECT user1_id, user2_id, last_proj
            FROM colab_pairs_multi_proj
            WHERE common_repos_after_6m_continuation_inc IS NULL
            LIMIT batch_size
            FOR UPDATE SKIP LOCKED
        ),
        user1_repos AS (
            SELECT 
                b.user1_id,
                b.user2_id,
                b.last_proj,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos1
            FROM batch b
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON b.user1_id = upr.user_id 
                AND b.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        ),
        user2_repos AS (
            SELECT 
                ur.user1_id,
                ur.user2_id,
                ur.last_proj,
                ur.repos1,
                COALESCE(jsonb_array_to_text_array(upr.repos), ARRAY[]::text[]) AS repos2
            FROM user1_repos ur
            LEFT JOIN user_proj_repo_after_6mon upr 
                ON ur.user2_id = upr.user_id 
                AND ur.last_proj = upr.project_id 
                AND upr.window_type = 'after'
        )
        UPDATE colab_pairs_multi_proj cpm
        SET common_repos_after_6m_continuation_inc = array_intersect(ur.repos1, ur.repos2)
        FROM user2_repos ur
        WHERE cpm.user1_id = ur.user1_id 
            AND cpm.user2_id = ur.user2_id;
        
        GET DIAGNOSTICS rows_updated = ROW_COUNT;
        total_processed := total_processed + rows_updated;
        
        IF rows_updated = 0 THEN
            EXIT;
        END IF;
        
        RAISE NOTICE 'Batch completed: % rows, time: %, overall progress: %/%', 
            rows_updated, 
            clock_timestamp() - batch_start_time,
            total_processed,
            total_rows;
        
        COMMIT;
    END LOOP;
    
    RAISE NOTICE 'common_repos_after_6m_continuation_inc update completed, total time: %', clock_timestamp() - start_time;
END $$;

-- ============================================
-- Verify Results
-- ============================================

-- Check single project table
SELECT 
    'Single Project Table' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN common_repos_before IS NOT NULL THEN 1 END) as before_processed,
    COUNT(CASE WHEN common_repos_after_2y IS NOT NULL THEN 1 END) as after_2y_processed,
    COUNT(CASE WHEN common_repos_after_2y_continuation_inc IS NOT NULL THEN 1 END) as continuation_2y_processed,
    COUNT(CASE WHEN common_repos_after_6m IS NOT NULL THEN 1 END) as after_6m_processed,
    COUNT(CASE WHEN common_repos_after_6m_continuation_inc IS NOT NULL THEN 1 END) as continuation_6m_processed,
    COUNT(CASE WHEN common_repos_before IS NOT NULL AND array_length(common_repos_before, 1) > 0 THEN 1 END) as before_has_repos,
    COUNT(CASE WHEN common_repos_after_2y IS NOT NULL AND array_length(common_repos_after_2y, 1) > 0 THEN 1 END) as after_2y_has_repos,
    COUNT(CASE WHEN common_repos_after_2y_continuation_inc IS NOT NULL AND array_length(common_repos_after_2y_continuation_inc, 1) > 0 THEN 1 END) as continuation_2y_has_repos,
    COUNT(CASE WHEN common_repos_after_6m IS NOT NULL AND array_length(common_repos_after_6m, 1) > 0 THEN 1 END) as after_6m_has_repos,
    COUNT(CASE WHEN common_repos_after_6m_continuation_inc IS NOT NULL AND array_length(common_repos_after_6m_continuation_inc, 1) > 0 THEN 1 END) as continuation_6m_has_repos
FROM colab_pairs_single_proj;

-- Check multi project table
SELECT 
    'Multi Project Table' as table_name,
    COUNT(*) as total_rows,
    COUNT(CASE WHEN common_repos_before IS NOT NULL THEN 1 END) as before_processed,
    COUNT(CASE WHEN common_repos_after_2y IS NOT NULL THEN 1 END) as after_2y_processed,
    COUNT(CASE WHEN common_repos_after_2y_continuation_inc IS NOT NULL THEN 1 END) as continuation_2y_processed,
    COUNT(CASE WHEN common_repos_after_6m IS NOT NULL THEN 1 END) as after_6m_processed,
    COUNT(CASE WHEN common_repos_after_6m_continuation_inc IS NOT NULL THEN 1 END) as continuation_6m_processed,
    COUNT(CASE WHEN common_repos_before IS NOT NULL AND array_length(common_repos_before, 1) > 0 THEN 1 END) as before_has_repos,
    COUNT(CASE WHEN common_repos_after_2y IS NOT NULL AND array_length(common_repos_after_2y, 1) > 0 THEN 1 END) as after_2y_has_repos,
    COUNT(CASE WHEN common_repos_after_2y_continuation_inc IS NOT NULL AND array_length(common_repos_after_2y_continuation_inc, 1) > 0 THEN 1 END) as continuation_2y_has_repos,
    COUNT(CASE WHEN common_repos_after_6m IS NOT NULL AND array_length(common_repos_after_6m, 1) > 0 THEN 1 END) as after_6m_has_repos,
    COUNT(CASE WHEN common_repos_after_6m_continuation_inc IS NOT NULL AND array_length(common_repos_after_6m_continuation_inc, 1) > 0 THEN 1 END) as continuation_6m_has_repos
FROM colab_pairs_multi_proj;

