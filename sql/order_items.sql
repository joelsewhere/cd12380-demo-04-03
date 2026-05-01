SELECT items.item_id
     , items.order_id
     , items.quantity
     , items.unit_price
     , items.discount
     , items.line_total
     , orders.order_date
FROM raw.order_items items
JOIN raw.orders  -- drop order items without a known order record
    USING(order_id)
JOIN raw.products -- drop order items without a known product record
    USING(product_id)
WHERE items.ingested_date = '{{ ti.xcom_pull(task_ids="metadata")["ingested_date"] }}'
-- drop price mismatch
AND ABS(items.unit_price - products.unit_price) / NULLIF(products.unit_price, 0) <= 0.01