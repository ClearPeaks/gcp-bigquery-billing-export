# GCP BigQuery billing export

This script stores in BigQuery tables billing information and BigQuery detailed cost information from a project in Google Cloud Platform.

Find the related blog in our website [to be published].

## Context

This script has been coded considering that you have multiple Projects, one for each department for instance, and one "core" project that contains all your project data in:

    - A single Cloud Storage bucket, divided in folders (one for each department).
    - One BigQuery dataset for each department.
    - One BigQuery table in each dataset.

## Description

The tables created with this script are the following:

- storage_usage: contains size in bytes used in Cloud Storage and BigQuery.
- bq_jobs_costs_detail: generated executing a BQ query. Execution time, bytes billed and cost in dollars of each user job in each project.
- bq_jobs_costs_per_project: generated executing a BQ query. Number of queries, bytes billed and cost in dollars for each user in each project.

The script runs every month in a Cloud Function. Duplicates are avoided (idempotent).

## Contact

In ClearPeaks we are aware of the importance of delivering clean and readable code to our clients. Therefore, we develop our code under strict code guidelines and following the best practices to deliver quality products.

You can read more about us in our [website](https://www.clearpeaks.com/) were you will be able to see what [services](https://www.clearpeaks.com/bi-services/) we are offering, the [solutions](https://www.clearpeaks.com/bi-solutions-analytic-applications/) we are currently deliverying, and you will be able to read a vast of [blogs](https://www.clearpeaks.com/cp_blog/) where we discuss about many Big Data and BI topics and technologies. Furthermore, you can check our [GitHub](https://github.com/ClearPeaksSL) where we sometimes share with the community some interesting content.

Script developed by:

- Víctor Colomé | victor.colome@clearpeaks.com
