CREATE TABLE order_products_prior WITH (external_location = 's3://airflow-alan/features/order_products_prior/', format = 'parquet') as 
(SELECT a.*, b.product_id,
b.add_to_cart_order,
b.reordered FROM orders a
JOIN order_products b
ON a.order_id = b.order_id
WHERE a.eval_set = 'prior');

CREATE TABLE user_features_1 WITH (external_location = 's3://airflow-alan/features/user_features_1/', format = 'parquet') as 
(select user_id, max(order_number) AS max_ord_num, sum(days_since_prior_order) AS sum_of_days, round(AVG(days_since_prior_order),2) AS average_days_since_prior_order
from orders
group by user_id);

CREATE TABLE user_features_2 WITH (external_location = 's3://airflow-alan/features/user_features_2/', format = 'parquet') as 
(select user_id, count(product_id) AS total_products, count(distinct product_id) AS total_distinct_products, 
round(CAST(sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) AS double)/count(CASE 
                   WHEN order_number > 1 THEN 1
                   END),4) AS reordered_ratio
from order_products_prior
group by user_id);

CREATE TABLE up_features WITH (external_location = 's3://airflow-alan/features/up_features/', format = 'parquet') as
(select user_id, product_id, count(distinct order_id) AS total_number_of_orders, min(order_number) AS min_order_number, 
max(order_number) AS max_order_number, round(avg(add_to_cart_order),2) AS avg_add_to_cart_order
from order_products_prior
group by user_id, product_id);

CREATE TABLE prd_features WITH (external_location = 's3://airflow-alan/features/prd_features/', format = 'parquet') as
(select product_id, count(*) AS cnt_prod_orders, sum(reordered) AS cnt_prod_reorders, 
count(case when product_seq_time = 1 then 1 end) AS cnt_product_first_orders,
count(case when product_seq_time = 2 then 1 end) AS cnt_product_second_orders
from 
(select user_id, order_number, product_id, reordered,
sum(1) over (partition by user_id, product_id order by order_number) AS product_seq_time
from order_products_prior) a
group by product_id);

select *
from user_features_1 t1 inner join user_features_2 t2 using(user_id)
inner join up_features t3 using(user_id)
inner join prd_features t4 using(product_id);
