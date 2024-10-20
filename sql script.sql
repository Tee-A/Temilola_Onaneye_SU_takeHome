-- Create raw tables

-- create clickup table sql
CREATE TABLE IF NOT EXISTS raw.clickup (
    client VARCHAR(200) NOT NULL,
    project VARCHAR(200) NOT NULL,
    name VARCHAR(200) NOT NULL,
    task VARCHAR(200) NOT NULL,
    date DATE,
    hours FLOAT,
    note VARCHAR(250),
    billable VARCHAR(5)
);

-- create float_allocation table sql
CREATE TABLE IF NOT EXISTS raw.float (
    client VARCHAR(200) NOT NULL,
    project VARCHAR(200) NOT NULL,
    role VARCHAR(200) NOT NULL,
    name VARCHAR(200) NOT NULL,
    task VARCHAR(200) NOT NULL,
    start_date DATE,
    end_date DATE,
    estimated_hours FLOAT
);

-- Create another clickup and float table to be used for optimization
CREATE TABLE IF NOT EXISTS raw.clickup_opt (
    client VARCHAR(200) NOT NULL,
    project VARCHAR(200) NOT NULL,
    name VARCHAR(200) NOT NULL,
    task VARCHAR(200) NOT NULL,
    date DATE,
    hours FLOAT,
    note VARCHAR(250),
    billable VARCHAR(5)
);

CREATE TABLE IF NOT EXISTS raw.float_opt (
    client VARCHAR(200) NOT NULL,
    project VARCHAR(200) NOT NULL,
    role VARCHAR(200) NOT NULL,
    name VARCHAR(200) NOT NULL,
    task VARCHAR(200) NOT NULL,
    start_date DATE,
    end_date DATE,
    estimated_hours FLOAT
);

-- Create Indexes on the tables
-- Create index on 'Name' in ClickUp table
CREATE INDEX idx_clickup_name ON raw.clickup_opt (name);

-- Create index on 'Name' in Float table
CREATE INDEX idx_float_name ON raw.float_opt (name);

-- Create a multi-column index on 'Name' and 'Role' in the Clickup table
CREATE INDEX idx_float_name_role ON raw.float_opt (name, role);


---- Dimensional modelling design
-- create dim_projects table sql
CREATE TABLE IF NOT EXISTS dim.dim_projects (
    project_id VARCHAR(200) PRIMARY KEY,
    project VARCHAR(200) NOT NULL,
    client VARCHAR(200)
);

-- create dim_team table sql 
CREATE TABLE IF NOT EXISTS dim.dim_team (
    member_id VARCHAR(200) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    role VARCHAR(200)
);

# create fct_tasks table sql
CREATE TABLE IF NOT EXISTS fact.fct_tasks (
    task_id VARCHAR(200),
    task VARCHAR(200) NOT NULL,
    project_id VARCHAR(200) REFERENCES dim.dim_projects(project_id),
    member_id VARCHAR(200) REFERENCES dim.dim_team(member_id),
    start_date DATE,
    end_date DATE,
    estimated_hours FLOAT,
    task_date DATE NOT NULL,
    task_hours FLOAT NOT NULL,
    task_note VARCHAR(250),
    billable VARCHAR(5)
);