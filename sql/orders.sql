WITH item_sums AS (
    SELECT
        order_id,
        SUM(line_total) AS computed_total
    FROM raw.order_items
    WHERE ingested_date = '{{ ti.xcom_pull(task_ids="metadata")["ingested_date"] }}'
    GROUP BY order_id
)
SELECT orders.order_id
     , orders.customer_id
     , orders.email_snapshot
     , orders.order_date
     , orders.status
     , orders.payment_method
     , orders.declared_total
     , orders.currency
     , orders.source_system
     , orders.ingested_at
     , orders.billing_address
     , orders.shipping_address
     , orders.promo_code
     , orders.session_id
     , orders.ip_address
     , orders.raw_status_code
     , orders.user_agent
FROM raw.orders
INNER JOIN raw.customers -- drop orders with unknown customer ids
    USING(customer_id)
INNER JOIN item_sums 
    USING(order_id)
WHERE orders.ingested_date = '{{ ti.xcom_pull(task_ids="metadata")["ingested_date"] }}'
AND orders.declared_total >= 0 -- drop negative order totals
-- drop mismatching order totals
AND ABS(orders.declared_total - item_sums.computed_total) / NULLIF(item_sums.computed_total, 0) <= 0.30