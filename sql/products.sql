SELECT products.product_id
     , products.product_name
     , products.category
     , products.unit_price
     , products.weight_kg
     , products.stock_quantity
     , products.is_active
     , products.created_date 
     , CURRENT_TIMESTAMP() AS promoted_at
FROM raw.products
WHERE products.ingested_date = '{{ ti.xcom_pull(task_ids="metadata")["ingested_date"] }}'
AND product_id IS NOT NULL -- product id cannot be null
AND unit_price >= 0 -- unit price cannot be negative