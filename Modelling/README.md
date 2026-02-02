## Visual Summary

<p align="center">
  <img src="https://github.com/Tramnddle/Glamira-Batch-Processing/blob/c5895aa9e28508685a95fd0da1af659a9cfed9c2/images/drawSQL-image-export-2026-01-22.png"
       alt="Dimensional Data Model"
       width="900">
</p>


*End-to-end batch analytics architecture using BigQuery and dbt  
(Raw → Staging → Snapshot → Mart).*

<p align="center">
  <img src="https://github.com/Tramnddle/Glamira-Batch-Processing/blob/b5b7d79d2cfa6f22dddc94495189c8e91c3affe6/images/glamira_datamodel.jpg"
       alt="dbt model lineage"
       width="900">
</p>


<p align="center">
## **Transformation Overview**

  | Layer    | SQL File                 | Responsibility                                      |
  |----------|--------------------------|----------------------------------------------------|
  | Raw      | `raw.*`                  | Source-aligned ingestion, no transformation         |
  | Staging  | `stg_customer.sql`       | Email normalization, user ID validation, deduplication |
  | Staging  | `stg_order.sql`          | Order deduplication, timestamp normalization        |
  | Staging  | `stg_product.sql`        | Flatten nested product options, clean attributes    |
  | Snapshot | `customer_email_scd.sql` | Track historical email changes (SCD Type 2)         |
  | Mart     | `mart_dim_customer.sql`  | Latest email selection, surrogate key generation    |
  | Mart     | `mart_dim_product.sql`   | Business-ready product attributes                   |
  | Mart     | `mart_dim_location.sql`  | Standardized location dimension                     |
  | Mart     | `mart_fact_order.sql`    | Transactional fact table with normalized metrics    |

</p>
