import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
  
def main():
    sc = SparkContext.getOrCreate()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

    # Read data dynamic frames from data catalog and s3 parquet files and create orders and 
    # order_products_prior TempView to allow sql implementation

    orders = glueContext.create_dynamic_frame.from_catalog(database="prd",table_name="orders")
    order_products_prior = glueContext.create_dynamic_frame.from_options(connection_type = "parquet", 
                        connection_options = {"paths":["s3://imba-alan/features/order_products_prior/"]})

    orders.toDF().createOrReplaceTempView("orders")
    order_products_prior.toDF().createOrReplaceTempView("order_products_prior")

    # Create relevant dataframes for user_features_1, user_features_2, up_features and prd_features

    spark.sql("select user_id, max(order_number) AS max_ord_num, sum(days_since_prior_order) AS sum_of_days, round(AVG(days_since_prior_order),2) AS average_days_since_prior_order from orders group by user_id").createOrReplaceTempView("user_features_1")

    spark.sql('''
    select user_id, count(product_id) AS total_products, count(distinct product_id) AS total_distinct_products, 
    round(CAST(sum(CASE WHEN reordered = 1 THEN 1 ELSE 0 END) AS double)/count(CASE 
                    WHEN order_number > 1 THEN 1
                    END),4) AS reordered_ratio
    from order_products_prior
    group by user_id
    ''').createOrReplaceTempView("user_features_2")

    spark.sql('''
    select user_id, product_id, count(distinct order_id) AS total_number_of_orders, min(order_number) AS min_order_number, 
    max(order_number) AS max_order_number, round(avg(add_to_cart_order),2) AS avg_add_to_cart_order
    from order_products_prior
    group by user_id, product_id
    ''').createOrReplaceTempView("up_features")

    spark.sql('''
    select product_id, count(*) AS cnt_prod_orders, sum(reordered) AS cnt_prod_reorders, 
    count(case when product_seq_time = 1 then 1 end) AS cnt_product_first_orders,
    count(case when product_seq_time = 2 then 1 end) AS cnt_product_second_orders
    from 
    (select user_id, order_number, product_id, reordered,
    sum(1) over (partition by user_id, product_id order by order_number) AS product_seq_time
    from order_products_prior) a
    group by product_id
    ''').createOrReplaceTempView("prd_features")

    # Create the final data set by joinning all features together

    final_features = spark.sql('''
    select *
    from user_features_1 t1 inner join user_features_2 t2 using(user_id)
    inner join up_features t3 using(user_id)
    inner join prd_features t4 using(product_id)
    ''')

    #Export it the correct s3 output location as a single CSV file

    final_features.repartition(1).write.mode('overwrite').format('csv').save("s3://imba-alan/glue_output", header = 'true')

if __name__ == '__main__':
    main()