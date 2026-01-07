-- 3NF Schema for Data Jobs Pipeline
-- PostgreSQL 16

-- Drop tables if they exist (for clean recreation)
DROP TABLE IF EXISTS job_skills CASCADE;
DROP TABLE IF EXISTS jobs CASCADE;
DROP TABLE IF EXISTS skills CASCADE;
DROP TABLE IF EXISTS skill_types CASCADE;
DROP TABLE IF EXISTS schedule_types CASCADE;
DROP TABLE IF EXISTS platforms CASCADE;
DROP TABLE IF EXISTS locations CASCADE;
DROP TABLE IF EXISTS companies CASCADE;

-- Companies dimension table
CREATE TABLE companies (
    company_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);

-- Locations dimension table
CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    job_location VARCHAR(255),
    search_location VARCHAR(255),
    country VARCHAR(100)
);

-- Platforms dimension table
CREATE TABLE platforms (
    platform_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Schedule types dimension table
CREATE TABLE schedule_types (
    schedule_type_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);

-- Skill types dimension table
CREATE TABLE skill_types (
    skill_type_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- Skills dimension table
CREATE TABLE skills (
    skill_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    skill_type_id INT REFERENCES skill_types(skill_type_id)
);

-- Jobs fact table
CREATE TABLE jobs (
    job_id INT PRIMARY KEY,
    job_title VARCHAR(500),
    job_title_short VARCHAR(255),
    company_id INT REFERENCES companies(company_id),
    location_id INT REFERENCES locations(location_id),
    platform_id INT REFERENCES platforms(platform_id),
    schedule_type_id INT REFERENCES schedule_types(schedule_type_id),
    work_from_home BOOLEAN,
    posted_date TIMESTAMP,
    no_degree_mention BOOLEAN,
    health_insurance BOOLEAN,
    salary_rate VARCHAR(50),
    salary_year_avg DECIMAL(12, 2),
    salary_hour_avg DECIMAL(8, 2)
);

-- Job-Skills junction table (many-to-many relationship)
CREATE TABLE job_skills (
    job_id INT REFERENCES jobs(job_id) ON DELETE CASCADE,
    skill_id INT REFERENCES skills(skill_id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);

-- Indexes for frequently queried columns
CREATE INDEX idx_jobs_company ON jobs(company_id);
CREATE INDEX idx_jobs_location ON jobs(location_id);
CREATE INDEX idx_jobs_platform ON jobs(platform_id);
CREATE INDEX idx_jobs_posted_date ON jobs(posted_date);
CREATE INDEX idx_jobs_schedule_type ON jobs(schedule_type_id);
CREATE INDEX idx_skills_skill_type ON skills(skill_type_id);
CREATE INDEX idx_job_skills_job ON job_skills(job_id);
CREATE INDEX idx_job_skills_skill ON job_skills(skill_id);

