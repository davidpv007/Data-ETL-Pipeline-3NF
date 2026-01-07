-- ============================================================================
-- DATA INGESTION PIPELINE - PostgreSQL 3NF Transformation
-- ============================================================================
-- This script handles the transformation of raw job data into a normalized
-- 3NF relational model with proper constraints, indexes, and views.
-- ============================================================================

-- ============================================================================
-- SECTION 1: INITIAL DATA VERIFICATION
-- ============================================================================

-- Verify bridge table export (pre-check)
SELECT *
FROM core.bridge_job_skills
LIMIT 10;

-- ============================================================================
-- SECTION 2: DATA INGESTION - JOBS TABLE
-- ============================================================================

-- Insert job records from staging table into core jobs table
-- Uses ON CONFLICT to handle duplicate job_ids gracefully
INSERT INTO jobs (
  job_id,
  job_title_short,
  job_title,
  company_name,
  job_location,
  job_via,
  job_schedule_type,
  job_work_from_home,
  search_location,
  job_posted_date,
  job_no_degree_mention,
  job_health_insurance,
  job_country,
  salary_rate,
  salary_year_avg,
  salary_hour_avg
)
SELECT
  job_id,
  job_title_short,
  job_title,
  company_name,
  job_location,
  job_via,
  job_schedule_type,
  job_work_from_home,
  search_location,
  job_posted_date,
  job_no_degree_mention,
  job_health_insurance,
  job_country,
  salary_rate,
  salary_year_avg,
  salary_hour_avg
FROM data_jobs_raw
ON CONFLICT (job_id) DO NOTHING;

-- Verify jobs table data
SELECT *
FROM jobs
LIMIT 10;

-- List all tables in public schema
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public';

-- ============================================================================
-- SECTION 3: SCHEMA SETUP - PRIMARY KEYS AND CONSTRAINTS
-- ============================================================================

-- Add primary key and unique constraint to dim_skill table
ALTER TABLE dim_skill
  ADD PRIMARY KEY (skill_id),
  ADD UNIQUE (skill_name);

-- Add primary key and foreign keys to bridge_job_skill table
ALTER TABLE bridge_job_skill
  ADD PRIMARY KEY (job_id, skill_id),
  ADD FOREIGN KEY (job_id) REFERENCES jobs(job_id),
  ADD FOREIGN KEY (skill_id) REFERENCES dim_skill(skill_id);

-- Add primary key constraint to jobs table
ALTER TABLE jobs
  ADD CONSTRAINT jobs_pk PRIMARY KEY (job_id);

-- Add primary key constraint to dim_skill table (named constraint)
ALTER TABLE dim_skill
  ADD CONSTRAINT dim_skill_pk PRIMARY KEY (skill_id);

-- Add unique constraint to dim_skill.skill_name (named constraint)
ALTER TABLE dim_skill
  ADD CONSTRAINT dim_skill_name_uniq UNIQUE (skill_name);

-- Add primary key constraint to bridge_job_skill table (named constraint)
ALTER TABLE bridge_job_skill
  ADD CONSTRAINT bridge_job_skill_pk PRIMARY KEY (job_id, skill_id);

-- Add foreign key constraint: bridge_job_skill -> jobs (with CASCADE)
ALTER TABLE bridge_job_skill
  ADD CONSTRAINT bridge_job_skill_job_fk
    FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE;

-- Add foreign key constraint: bridge_job_skill -> dim_skill (with CASCADE)
ALTER TABLE bridge_job_skill
  ADD CONSTRAINT bridge_job_skill_skill_fk
    FOREIGN KEY (skill_id) REFERENCES dim_skill(skill_id) ON DELETE CASCADE;

-- Create indexes on bridge table for improved query performance
CREATE INDEX IF NOT EXISTS idx_bridge_skill_id ON bridge_job_skill(skill_id);
CREATE INDEX IF NOT EXISTS idx_bridge_job_id ON bridge_job_skill(job_id);

-- Verify constraints on jobs table
SELECT
  conname,
  pg_get_constraintdef(c.oid)
FROM pg_constraint c
JOIN pg_class t ON t.oid = c.conrelid
WHERE t.relname = 'jobs';

-- Verify constraints on bridge_job_skill table
SELECT conname, pg_get_constraintdef(oid) AS def
FROM pg_constraint
WHERE conrelid = 'bridge_job_skill'::regclass;

-- ============================================================================
-- SECTION 4: DIMENSION TABLES - COMPANY
-- ============================================================================

-- Create dim_company dimension table
CREATE TABLE IF NOT EXISTS dim_company (
  company_id BIGSERIAL PRIMARY KEY,
  company_name TEXT UNIQUE
);

-- Populate dim_company with distinct company names from jobs table
INSERT INTO dim_company (company_name)
SELECT DISTINCT company_name
FROM jobs
WHERE company_name IS NOT NULL AND company_name <> ''
ON CONFLICT (company_name) DO NOTHING;

-- Add company_id column to jobs table
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS company_id BIGINT;

-- Update jobs table with company_id from dim_company
UPDATE jobs j
SET company_id = c.company_id
FROM dim_company c
WHERE j.company_name = c.company_name
  AND j.company_id IS NULL;

-- Add foreign key constraint: jobs -> dim_company
ALTER TABLE jobs
  ADD CONSTRAINT jobs_company_fk
  FOREIGN KEY (company_id) REFERENCES dim_company(company_id);

-- Check current database and schema
SELECT current_database(), current_schema();

-- Create dim_company table in public schema (if needed)
CREATE TABLE public.dim_company (
  company_id   BIGSERIAL PRIMARY KEY,
  company_name TEXT UNIQUE
);

-- Populate public.dim_company with distinct company names
INSERT INTO public.dim_company (company_name)
SELECT DISTINCT company_name
FROM public.jobs
WHERE company_name IS NOT NULL AND company_name <> ''
ON CONFLICT (company_name) DO NOTHING;

-- Add company_id column to public.jobs table
ALTER TABLE public.jobs ADD COLUMN company_id BIGINT;

-- Update public.jobs with company_id from dim_company
UPDATE public.jobs j
SET company_id = c.company_id
FROM public.dim_company c
WHERE j.company_name = c.company_name
  AND j.company_id IS NULL;

-- Add foreign key constraint: public.jobs -> public.dim_company
ALTER TABLE public.jobs
  ADD CONSTRAINT jobs_company_fk
  FOREIGN KEY (company_id) REFERENCES public.dim_company(company_id);

-- Verify dim_company table exists
SELECT schemaname, tablename
FROM pg_tables
WHERE tablename = 'dim_company';

-- Verify dim_company row count
SELECT COUNT(*) FROM public.dim_company;

-- Sample company names
SELECT company_name FROM public.dim_company;

-- ============================================================================
-- SECTION 5: DATA QUALITY CHECKS - ROW COUNTS
-- ============================================================================

-- Compare row counts across all tables
SELECT
  (SELECT COUNT(*) FROM public.data_jobs_raw)      AS raw_rows,
  (SELECT COUNT(*) FROM public.jobs)              AS jobs_rows,
  (SELECT COUNT(*) FROM public.dim_skill)         AS skills_rows,
  (SELECT COUNT(*) FROM public.bridge_job_skill)  AS bridge_rows,
  (SELECT COUNT(*) FROM public.dim_company)       AS companies_rows;

-- Check for orphaned job_ids in bridge table (should be 0)
SELECT COUNT(*) AS orphan_job_ids
FROM public.bridge_job_skill b
LEFT JOIN public.jobs j ON j.job_id = b.job_id
WHERE j.job_id IS NULL;

-- Check for orphaned skill_ids in bridge table (should be 0)
SELECT COUNT(*) AS orphan_skill_ids
FROM public.bridge_job_skill b
LEFT JOIN public.dim_skill s ON s.skill_id = b.skill_id
WHERE s.skill_id IS NULL;

-- Check for jobs with missing company_id (should be 0)
SELECT COUNT(*) AS missing_company_id
FROM public.jobs
WHERE (company_name IS NOT NULL AND company_name <> '')
  AND company_id IS NULL;

-- Check for duplicate job_id, skill_id pairs in bridge table (should be 0)
SELECT job_id, skill_id, COUNT(*)
FROM public.bridge_job_skill
GROUP BY job_id, skill_id
HAVING COUNT(*) > 1;

-- Top skills by posting count
SELECT s.skill_name, COUNT(*) AS postings
FROM public.bridge_job_skill b
JOIN public.dim_skill s ON s.skill_id = b.skill_id
GROUP BY s.skill_name
ORDER BY postings DESC;

-- ============================================================================
-- SECTION 6: SCHEMA ORGANIZATION
-- ============================================================================

-- Create schemas for data warehouse organization
CREATE SCHEMA IF NOT EXISTS staging;   -- Raw / landing zone
CREATE SCHEMA IF NOT EXISTS core;      -- 3NF normalized entities
CREATE SCHEMA IF NOT EXISTS marts;     -- Analytics / reporting layer

-- Move tables to appropriate schemas
ALTER TABLE public.jobs SET SCHEMA core;
ALTER TABLE public.dim_skill SET SCHEMA core;
ALTER TABLE public.bridge_job_skill SET SCHEMA core;
ALTER TABLE public.dim_company SET SCHEMA core;
ALTER TABLE public.data_jobs_raw SET SCHEMA staging;

-- ============================================================================
-- SECTION 7: VIEWS CREATION - ANALYTICS LAYER
-- ============================================================================

-- Drop and create comprehensive job_skills view (enhanced version)
DROP VIEW IF EXISTS marts.job_skills;

CREATE VIEW marts.job_skills AS
SELECT
  j.job_id,
  j.job_title_short,
  j.job_title,
  j.job_country,
  j.search_location,
  j.job_location,
  j.job_schedule_type,
  j.job_work_from_home,
  j.job_posted_date,
  j.salary_rate,
  j.salary_year_avg,
  j.salary_hour_avg,
  c.company_name,
  s.skill_name
FROM core.jobs j
JOIN core.bridge_job_skill b
  ON b.job_id = j.job_id
JOIN core.dim_skill s
  ON s.skill_id = b.skill_id
LEFT JOIN core.dim_company c
  ON c.company_id = j.company_id;

-- Verify view structure
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'marts'
  AND table_name = 'job_skills'
ORDER BY ordinal_position;

-- ============================================================================
-- SECTION 8: INDEXES FOR PERFORMANCE
-- ============================================================================

-- Create indexes on foreign key columns for improved join performance
CREATE INDEX IF NOT EXISTS idx_jobs_company_id
  ON core.jobs(company_id);

CREATE INDEX IF NOT EXISTS idx_bridge_skill_id
  ON core.bridge_job_skill(skill_id);

CREATE INDEX IF NOT EXISTS idx_bridge_job_id
  ON core.bridge_job_skill(job_id);

-- Verify primary key constraints across all core tables
SELECT conrelid::regclass AS table_name, conname, pg_get_constraintdef(oid) AS def
FROM pg_constraint
WHERE contype='p'
  AND conrelid::regclass::text IN ('jobs','dim_skill','bridge_job_skill','dim_company')
ORDER BY table_name;

-- ============================================================================
-- SECTION 9: DATA QUALITY CHECKS - DUPLICATES
-- ============================================================================

-- Check for duplicate skill names in core schema (should return 0)
SELECT COUNT(*) AS dup_skill_names
FROM (
  SELECT skill_name
  FROM core.dim_skill
  GROUP BY skill_name
  HAVING COUNT(*) > 1
) d;

-- Check for duplicate company names in core schema (should return 0)
SELECT COUNT(*) AS dup_company_names
FROM (
  SELECT company_name
  FROM core.dim_company
  GROUP BY company_name
  HAVING COUNT(*) > 1
) d;

-- Analyze distinct values in jobs table
SELECT
  COUNT(DISTINCT company_name)      AS distinct_companies,
  COUNT(DISTINCT job_location)      AS distinct_job_locations,
  COUNT(DISTINCT job_schedule_type) AS distinct_schedule_types,
  COUNT(DISTINCT job_via)           AS distinct_sources
FROM jobs;

-- Check current database and schema settings
SELECT current_database() AS db, current_schema() AS schema;
SHOW search_path;

-- List all tables across schemas
SELECT schemaname, tablename
FROM pg_tables
WHERE tablename IN ('data_jobs_raw','jobs','dim_skill','bridge_job_skill','dim_company')
ORDER BY schemaname, tablename;

-- Row count verification by schema
SELECT COUNT(*) FROM core.jobs;
SELECT COUNT(*) FROM core.dim_skill;
SELECT COUNT(*) FROM core.bridge_job_skill;
SELECT COUNT(*) FROM core.dim_company;
SELECT COUNT(*) FROM staging.data_jobs_raw;

-- ============================================================================
-- SECTION 10: ENHANCED VIEWS - MART LAYER
-- ============================================================================

-- Create skill_counts view: aggregate skills by posting count
DROP VIEW IF EXISTS marts.skill_counts;

CREATE VIEW marts.skill_counts AS
SELECT
  skill_name,
  COUNT(*) AS postings
FROM marts.job_skills
GROUP BY skill_name
ORDER BY postings DESC;

-- Create skill_counts_by_title view: skills grouped by job title
DROP VIEW IF EXISTS marts.skill_counts_by_title;

CREATE VIEW marts.skill_counts_by_title AS
SELECT
  job_title_short,
  skill_name,
  COUNT(*) AS postings
FROM marts.job_skills
GROUP BY job_title_short, skill_name;

-- Create skill_counts_by_country view: skills grouped by country
DROP VIEW IF EXISTS marts.skill_counts_by_country;

CREATE VIEW marts.skill_counts_by_country AS
SELECT
  job_country,
  skill_name,
  COUNT(*) AS postings
FROM marts.job_skills
GROUP BY job_country, skill_name;

-- ============================================================================
-- SECTION 11: VIEW TESTING
-- ============================================================================

-- Test skill_counts view
SELECT * FROM marts.skill_counts LIMIT 20;

-- Test skill_counts_by_title view for Data Engineer role
SELECT *
FROM marts.skill_counts_by_title
WHERE job_title_short = 'Data Engineer'
ORDER BY postings DESC
LIMIT 20;

-- Test skill_counts_by_country view for United States
SELECT *
FROM marts.skill_counts_by_country
WHERE job_country = 'United States'
ORDER BY postings DESC
LIMIT 20;

-- ============================================================================
-- SECTION 12: FINAL DATA QUALITY VALIDATION
-- ============================================================================

-- Check for duplicate job_ids (should return 0)
SELECT COUNT(*) AS dup_job_ids
FROM (
  SELECT job_id
  FROM core.jobs
  GROUP BY job_id
  HAVING COUNT(*) > 1
) d;

-- Check for duplicate skill_ids (should return 0)
SELECT COUNT(*) AS dup_skill_ids
FROM (
  SELECT skill_id
  FROM core.dim_skill
  GROUP BY skill_id
  HAVING COUNT(*) > 1
) d;

-- Check for duplicate company_ids (should return 0)
SELECT COUNT(*) AS dup_company_ids
FROM (
  SELECT company_id
  FROM core.dim_company
  GROUP BY company_id
  HAVING COUNT(*) > 1
) d;

-- Check for duplicate job_id, skill_id pairs in bridge table (should return 0 rows)
SELECT job_id, skill_id, COUNT(*)
FROM core.bridge_job_skill
GROUP BY job_id, skill_id
HAVING COUNT(*) > 1;

-- Check for null values in bridge table (should return 0,0)
SELECT
  SUM((job_id IS NULL)::int)  AS null_job_id,
  SUM((skill_id IS NULL)::int) AS null_skill_id
FROM core.bridge_job_skill;

-- Check for orphaned job_ids in bridge table (should return 0)
SELECT COUNT(*) AS orphan_job_ids
FROM core.bridge_job_skill b
LEFT JOIN core.jobs j ON j.job_id = b.job_id
WHERE j.job_id IS NULL;

-- Check for orphaned skill_ids in bridge table (should return 0)
SELECT COUNT(*) AS orphan_skill_ids
FROM core.bridge_job_skill b
LEFT JOIN core.dim_skill s ON s.skill_id = b.skill_id
WHERE s.skill_id IS NULL;

-- Check for orphaned company_ids in jobs table (should return 0)
SELECT COUNT(*) AS orphan_company_ids
FROM core.jobs j
LEFT JOIN core.dim_company c ON c.company_id = j.company_id
WHERE j.company_id IS NOT NULL AND c.company_id IS NULL;

-- Analyze distinct values in core.jobs table
SELECT
  COUNT(DISTINCT job_location)      AS distinct_job_locations,
  COUNT(DISTINCT job_via)           AS distinct_job_via,
  COUNT(DISTINCT job_schedule_type) AS distinct_schedule_types,
  COUNT(DISTINCT job_country)       AS distinct_countries
FROM core.jobs;