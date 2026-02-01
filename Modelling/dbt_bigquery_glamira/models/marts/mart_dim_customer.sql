{{ config(materialized='table') }}

WITH
  customer_emails AS (
    -- Use stg_customer as the primary source of emails to preserve as many as possible
    SELECT
      CAST(user_db_id AS int64) AS user_db_id,
      lower(trim(CAST(email_address AS string))) AS email_address
    FROM `solid-transport-479213-h5`.`raw_staging`.`stg_customer`
    WHERE email_address IS NOT NULL AND trim(email_address) != ''
  ),
  email_events AS (
    -- Snapshot/history tells us recency to resolve email->user conflicts
    SELECT
      CAST(user_db_id AS int64) AS user_db_id,
      lower(trim(CAST(email_address AS string))) AS email_address,
      time
    FROM `solid-transport-479213-h5`.`snapshots`.`customer_email_scd`
    WHERE
      user_db_id IS NOT NULL
      AND email_address IS NOT NULL
      AND trim(email_address) != ''
  ),
  latest_seen_per_email_user AS (
    -- for each (email, user), get last time seen
    -- get the most recent event_ts per (email_address, user_db_id) using ROW_NUMBER
    SELECT email_address, user_db_id, time AS last_seen_ts
    FROM
      (
        SELECT
          email_address,
          user_db_id,
          time,
          row_number()
            OVER (PARTITION BY email_address, user_db_id ORDER BY time DESC)
            AS rn
        FROM email_events
      )
    WHERE rn = 1
  ),
  merge_back_to_customer AS (
    SELECT
      ce.user_db_id,
      ce.email_address AS original_email_address,  -- Renamed for clarity
      ls.email_address AS latest_seen_email_address,
      ls.last_seen_ts
    FROM `solid-transport-479213-h5`.`raw_staging`.`stg_customer` ce
    LEFT JOIN latest_seen_per_email_user ls
      ON ce.user_db_id = ls.user_db_id
  ),
  final_email_selection AS (
    SELECT
      mb.user_db_id,
      CASE
        WHEN mb.latest_seen_email_address IS NOT NULL
          THEN mb.latest_seen_email_address
        ELSE mb.original_email_address
        END
        AS email_address_final,
      mb.last_seen_ts
    FROM merge_back_to_customer mb
  )
SELECT
  fes.user_db_id,
  fes.email_address_final,
  fes.last_seen_ts,
  FARM_FINGERPRINT(fes.email_address_final) AS customer_key
FROM final_email_selection fes
