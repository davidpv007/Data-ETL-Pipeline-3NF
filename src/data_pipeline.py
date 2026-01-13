#!/usr/bin/env python
# coding: utf-8

# # Data Cleaning for 3NF in PostgreSQL using Polars

# In[2]:


# Standard library

import os
import sys
import json
import ast
from pathlib import Path

# Third-party libraries
import numpy as np
import polars as pl
from sqlalchemy import create_engine, text

# Local config (pulls DB settings from environment when running in Docker/Airflow)
from src.config import PostgresConfig

pl.Config.set_tbl_rows(-1)   # show all rows
pl.Config.set_tbl_cols(-1)   # show all columns
pl.Config.set_fmt_str_lengths(10_000)

# setting up csv path

def main():
    print(">>> ETL starting inside container")

    csv_path = Path(os.getenv("CSV_PATH", "/opt/airflow/data/data_jobs.csv"))
    df = pl.read_csv(csv_path)


    # initial data exploration

    # print(f"Data shape: {df.shape}")
    # print(f"\nColumns: {df.columns}")
    # print(df.select(pl.all().is_null().sum()))
    # df.null_count()
    # df.schema
    # df.head(5)

    # In[4]:


    # Cleaning white space on columns

    df = df.with_columns(
        pl.col(pl.Utf8).str.strip_chars()
    )


    # Parse job_posted_date to datetime
    df = df.with_columns(
        pl.col("job_posted_date")
        .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
        .dt.replace_time_zone("UTC")
        .alias("job_posted_date")
    )

    # verify date columns are parsed correctly

    # print(f"Parsed job_posted_date: {df['job_posted_date'].dtype}")
    # for col, dtype in df.schema.items():
    #     print(f" {col}: {dtype}")



    # In[5]:


    # Standardize Boolean Columns 

    bool_cols = [ c for c, t in df.schema.items() if t == pl.Boolean]

    # print(bool_cols)


    # Convert to boolean (handles various string representations)
    df = df.with_columns(
        pl.col(bool_cols)
        .cast(pl.String)
        .str.to_lowercase()
        .is_in(["true", "1", "yes", "y"])
        .cast(pl.Boolean)
    )

    #verify bools cols

    # print(df.select(bool_cols).schema)


    # Ensure numeric columns are floats

    num_cols = [c for c, t in df.schema.items() if t == pl.Float64]

    # print(num_cols)


    # Convert and cast numeric columns to float

    df = df.with_columns([
        pl.col("salary_year_avg").cast(pl.Float64, strict=False),
        pl.col("salary_hour_avg").cast(pl.Float64, strict=False)
    ])

    #verify for nulls on float cols and convert nulls to 0

    df.select(num_cols).null_count()

    df = df.with_columns(
        pl.col(num_cols).fill_null(0)
    )

    #verify nulls are converted to 0

    df.select(num_cols).null_count()

    # adding a job id (row index starting at 1)

    if "job_id" not in df.columns:
        df = df.with_row_index(name="job_id", offset=1)




    # In[6]:


    # Parse JSON Columns (job_skills and job_type_skills)

    df = df.with_columns(
        pl.col("job_skills")
        .str.replace_all("'", '"')
        .str.json_decode(pl.List(pl.Utf8))
        .alias("job_skills")
    )

    # clean elements inside job_skills list

    # print(df.schema["job_skills"])

    df = df.with_columns(
        pl.col("job_skills")
        .fill_null(pl.lit([]))
        .list.eval(
            pl.element()
                .str.strip_chars()
                .str.to_lowercase()
        )
        .alias("job_skills")
    )

    df.select("job_skills").head(5)

    # Explode into one row per skill

    job_skills_rel = (
        df
        .select(["job_id", "job_skills"])
        .explode("job_skills")  # 1 row per skill
        .rename({"job_skills": "skill_name"})
    )

    # cleaned exploded df columns

    job_skills_rel = (
        job_skills_rel
        .filter(pl.col("skill_name").is_not_null() & (pl.col("skill_name") != ""))
        .unique()
    )

    def parse_skill_dict(x):
        if x is None:
            return None
        s = str(x).strip()
        if s in ("", "null", "None", "[]", "{}"):
            return None
        try:
            return ast.literal_eval(s)  # parses "{'cloud': ['aws']}" into dict
        except Exception:
            return None

    # parsed job_type_skills into a dictionary

    parsed = df.with_columns(
        pl.col("job_type_skills")
        .map_elements(parse_skill_dict, return_dtype=pl.Object)
        .alias("skills_obj")
    )

    # 1) Normalize job_type_skills (skills_obj dict) into long rows: job_id, skill_group, skill_name

    job_type_skills_rel = (
        parsed
        .select(["job_id", "skills_obj"])
        .drop_nulls("skills_obj")
        .with_columns(
            pl.col("skills_obj").map_elements(
                lambda d: [
                    {"skill_group": k, "skill_name": v}
                    for k, vals in d.items()
                    for v in (vals or [])
                ],
                return_dtype=pl.List(
                    pl.Struct([
                        pl.Field("skill_group", pl.Utf8),
                        pl.Field("skill_name", pl.Utf8),
                    ])
                ),
            ).alias("pairs")
        )
        .explode("pairs")
        .unnest("pairs")
        .with_columns(
            pl.col("skill_group").str.strip_chars().str.to_lowercase(),
            pl.col("skill_name").str.strip_chars().str.to_lowercase(),
        )
        .filter(pl.col("skill_name").is_not_null() & (pl.col("skill_name") != ""))
        .unique()
    )

    # 2) Add a nullable group to job_skills_rel so schemas match for union

    job_skills_rel2 = (
        job_skills_rel
        .with_columns(pl.lit(None).cast(pl.Utf8).alias("skill_group"))
        .select(["job_id", "skill_group", "skill_name"])
    )

    # 3) Union both sources + dedupe
    job_skill_all = (
        pl.concat([
            job_skills_rel2,
            job_type_skills_rel.select(["job_id", "skill_group", "skill_name"])
        ])
        .unique()
    )

    # 4) Build dim_skill

    dim_skill = (
        job_skill_all
        .select("skill_name")
        .unique()
        .sort("skill_name")
        .with_row_index("skill_id", offset=1)
    )

    # 5) Build bridge table (pure 3NF)

    bridge_job_skill = (
        job_skill_all
        .join(dim_skill, on="skill_name", how="left")
        .select(["job_id", "skill_id"])
        .unique()
    )

    # 6) Quick sanity check

    # print(job_skill_all.select(
    #     pl.len().alias("rows"),
    #     pl.n_unique("job_id").alias("jobs_with_skills"),
    #     pl.n_unique("skill_name").alias("unique_skills"),
    # ))

    # print("sample job_skills:")
    # print(df.select("job_skills").head(5))
    # print("\nSample job_type_skills:")
    # print(df.select("job_type_skills").head(5))


    # In[7]:


    #Handle Null Values

    # print("null counts after cleaning:")
    # print(df.null_count())


    # filling empty strings with null

    df = df.with_columns([
        pl.when(pl.col(col) == "")
        .then(None)
        .otherwise(pl.col(col))
        .alias(col)
        for col in df.columns
        if df[col].dtype == pl.Utf8
    ])


    # In[8]:


    # Check for duplicates

    duplicate_count = df.is_duplicated().sum()

    # print(f"duplicate count: {duplicate_count}")


    # In[9]:


    # Expected columns for 3NF preparation

    EXPECTED_COLS = {
        "job_id",
        "job_title_short",
        "job_title",
        "company_name",
        "job_location",
        "job_via",
        "job_schedule_type",
        "job_work_from_home",
        "search_location",
        "job_posted_date",
        "job_no_degree_mention",
        "job_health_insurance",
        "job_country",
        "salary_rate",
        "salary_year_avg",
        "salary_hour_avg",
        "job_skills",
        "job_type_skills"
    }

    # Verify all expected columns exist

    assert set(df.columns).issuperset(EXPECTED_COLS), "Missing expected columns"
    assert df["job_id"].null_count() == 0, "job_id cannot be null"
    assert df["job_posted_date"].dtype == pl.Datetime, "job_posted_date must be datetime"
    assert df["job_work_from_home"].dtype == pl.Boolean, "job_work_from_home must be boolean"
    assert df["job_no_degree_mention"].dtype == pl.Boolean, "job_no_degree_mention must be boolean"
    assert df["job_health_insurance"].dtype == pl.Boolean, "job_health_insurance must be boolean"
    assert df["job_country"].dtype == pl.Utf8, "job_country must be string"
    assert df["salary_rate"].dtype == pl.Utf8, "salary_rate must be string"
    assert df["salary_year_avg"].dtype == pl.Float64, "salary_year_avg must be float"
    assert df["salary_hour_avg"].dtype == pl.Float64, "salary_hour_avg must be float"
    assert job_skills_rel.schema["skill_name"] == pl.Utf8, "skill_name must be a string after explode"
    assert job_skills_rel.filter(pl.col("skill_name").is_null() | (pl.col("skill_name") == "")).height == 0, \
        "skill_name should not contain null/empty values"
    assert job_skills_rel.height == job_skills_rel.unique(["job_id", "skill_name"]).height, \
        "Duplicate (job_id, skill_name) pairs found"


    print("validations passed")

    # summary

    # print("="*60)
    # print("CLEANED DATA SUMMARY")
    # print("="*60)
    # print(f"Total rows: {df.shape[0]:,}")
    # print(f"Total columns: {df.shape[1]}")
    # print(f"\nSchema:")
    # print(df.schema)
    # print(f"\nNull counts:")
    # print(df.null_count())
    # print(f"\nSample of cleaned data:")
    # df.head(5)


    # In[10]:

    # SQLAlchemy URL from environment (works in Airflow/Docker)

    DATABASE_URL = PostgresConfig.sqlalchemy_url()
    engine = create_engine(DATABASE_URL)

    # quick connection test

    with engine.connect() as conn:
        conn.execute(text("SELECT 1 AS ok"))


    # Make a DB-friendly copy of df for export (do NOT mutate df)
    # Convert list-like job_skills to JSON string so psycopg2 can handle it
    # Note: map_elements on List columns receives Polars Series, not Python lists

    df_for_sql = df

    if "job_skills" in df.columns:
        df_for_sql = df.with_columns(
            pl.col("job_skills").map_elements(
                lambda v: json.dumps(list(v)) if v is not None else None,
                return_dtype=pl.Utf8,
            ).alias("job_skills")
        )

    # Export the cleaned staging data directly from Polars

    df_for_sql.write_database(
        table_name="data_jobs_raw",
        connection=engine,
        if_table_exists="replace",  
    )

    # Export skill dimensions directly from Polars

    dim_skill.write_database(
        table_name="dim_skill",
        connection=engine,
        if_table_exists="replace",
    )

    bridge_job_skill.write_database(
        table_name="bridge_job_skill",
        connection=engine,
        if_table_exists="replace",
    )

    # verify load (no pandas)
    with engine.connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) AS n FROM data_jobs_raw")).scalar_one()
        print(f"Rows in data_jobs_raw: {n}")


if __name__ == "__main__":
    main()