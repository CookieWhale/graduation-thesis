-- Remove projects with >20 contributors
-- These projects may include large repositories such as numpy, rust, python...
CREATE TABLE projects_clean AS
SELECT *
FROM public.projects
WHERE team_contributor_size <= 20;

ALTER TABLE projects_clean ADD PRIMARY KEY (project_id);

-- CREATE TABLE user_projects_clean (LIKE user_projects INCLUDING ALL);

-- -- Only keep filtered user-projects
-- -- Use this clean dataset to generate pairs
-- INSERT INTO user_projects_clean (user_project_id, user_id, project_id)
-- SELECT up.user_project_id, up.user_id, up.project_id
-- FROM user_projects up
-- JOIN projects p ON up.project_id = p.project_id
-- WHERE p.team_contributor_size <= 20;






