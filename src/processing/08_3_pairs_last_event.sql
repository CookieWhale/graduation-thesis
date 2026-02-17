CREATE TABLE triggered_6m_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM triggered_6m t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE triggered_6m_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE terminated_6m_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM terminated_6m t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE terminated_6m_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE sustained_6m_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM sustained_6m t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE sustained_6m_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE temporary_6m_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM temporary_6m t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE temporary_6m_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE triggered_2y_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM triggered_2y t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE triggered_2y_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE terminated_2y_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM terminated_2y t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE terminated_2y_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE sustained_2y_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM sustained_2y t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE sustained_2y_last ADD PRIMARY KEY (user1_id, user2_id);

CREATE TABLE temporary_2y_last AS
SELECT 
    t.user1_id,
    t.user2_id,
    COALESCE(s.project_id, m.last_proj) AS project_id,
    t.before_repos_num,
    t.after_repos_6m_num,
    t.after_repos_2y_num,
    t.common_event_num,
    t.common_project_num,
    t.time,
    t.avg_outside_repos_before
FROM temporary_2y t
LEFT JOIN colab_pairs_single_proj s ON t.user1_id = s.user1_id AND t.user2_id = s.user2_id
LEFT JOIN colab_pairs_multi_proj m ON t.user1_id = m.user1_id AND t.user2_id = m.user2_id;

ALTER TABLE temporary_2y_last ADD PRIMARY KEY (user1_id, user2_id);

DO $$
DECLARE
    table_name TEXT;
    tables TEXT[] := ARRAY[
        'terminated_2y_last', 'terminated_6m_last',
        'sustained_2y_last', 'sustained_6m_last',
        'triggered_2y_last', 'triggered_6m_last',
        'temporary_2y_last', 'temporary_6m_last'
    ];
BEGIN
    FOREACH table_name IN ARRAY tables
    LOOP
        EXECUTE format('
            ALTER TABLE %I 
            ADD COLUMN IF NOT EXISTS h_duration INTEGER,
            ADD COLUMN IF NOT EXISTS is_offline_event INTEGER,
            ADD COLUMN IF NOT EXISTS hackathon_contributor_size INTEGER,
            ADD COLUMN IF NOT EXISTS team_contributor_size INTEGER,
            ADD COLUMN IF NOT EXISTS hackathon_id INTEGER,
            ADD COLUMN IF NOT EXISTS hackathon_participants_size INTEGER,
            ADD COLUMN IF NOT EXISTS hackathon_size INTEGER
        ', table_name);
        
        EXECUTE format('
            UPDATE %I pairs
            SET 
                h_duration = p.h_duration,
                is_offline_event = p.is_offline_event,
                hackathon_contributor_size = p.hackathon_contributor_size,
                team_contributor_size = p.team_contributor_size,
                hackathon_id = p.hackathon_id,
                hackathon_participants_size = p.hackathon_participants_size,
                hackathon_size = p.hackathon_size
            FROM projects_clean p
            WHERE pairs.project_id = p.project_id
        ', table_name);
        
        RAISE NOTICE 'Finished: %', table_name;
    END LOOP;
END $$;