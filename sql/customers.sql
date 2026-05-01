SELECT customers.customer_id
     , customers.first_name
     , customers.last_name
     , customers.email
     , customers.phone
     , customers.city
     , customers.state
     , customers.zip_code
     , customers.loyalty_tier
     , customers.joined_date
     , customers.is_active
     , CURRENT_TIMESTAMP() AS promoted_at       
FROM raw.customers 
WHERE customers.ingested_date = '{{ ti.xcom_pull(task_ids="metadata")["ingested_date"] }}'
AND customer_id IS NOT NULL -- drop null customer ids
