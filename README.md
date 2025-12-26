# Netflix Data Engineering Pipeline: AWS, Snowflake, dbt, Python, PowerBI

## Overview
This project is an end-to-end cloud-based data engineering pipeline that extracts insights on the highest performing movies and genres by decade. The pipeline ingests raw data into AWS S3, loads it into Snowflake, transforms it using dbt, normalizes genres in Python, and visualizes insights through a PowerBI dashboard. This project demonstrates current data engineering practices including ELT modeling, cloud warehousing, data transformation, documentation, and analytics engineering.

## Data Visualization
![Netflix Dashboard](https://github.com/user-attachments/assets/6f3d30e7-7835-4c40-bc02-26a0b1e2d822)

## Data Architecture
```mermaid
graph TD
    %% Nodes
    S3[AWS S3]
    SF[(Snowflake)]
    DBT[dbt]

    PBI[Power BI]

    %% Flow with Labels
    S3 -- "Raw CSV files" --> SF
    SF -- "Staging, Intermediate, Marts" --> DBT
    DBT -- "Models, tests, snapshots, documentation" --> PY
    PY -- "Genre normalization & preprocessing" --> PBI
    PBI -- "Dashboards & insights" --> Finish((End))

    %% Styling
    style S3 fill:#FF9900,stroke:#232F3E,color:#fff
    style SF fill:#29B5E8,stroke:#11567F,color:#fff
    style DBT fill:#FF694B,stroke:#ad1c00,color:#fff
    style PY fill:#3776AB,stroke:#1e3d59,color:#fff
    style PBI fill:#F2C811,stroke:#a68900,color:#000
    style Finish fill:#666,stroke:#333,color:#fff
