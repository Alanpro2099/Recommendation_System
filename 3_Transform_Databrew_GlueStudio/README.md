# ETL process using `AWS DataBrew` and `AWS glue studio`
### `AWS DataBrew` can conviniently clean and normalise data for ananlytics tasks
- ### Step 1 Create datasets using relevant s3 buckets
![](assets/images/p11.png)
- ### Step 2 Create projects and associative recipes
![](assets/images/p12.png)
1. up_features recipe:

<img src="assets/images/p4.png" width="50%"/>

2. user_features_1 recipe:

<img src="assets/images/p2.png" width="50%"/>

3. user_features_2 recipe:

<img src="assets/images/p3.png" width="50%"/>

4. prd_features recipe:

<img src="assets/images/p1.png" width="50%"/>

- ### Step 3 Create jobs to generate parquet files for each feature table and store them into S3 folders
![](assets/images/p13.png)

- ### Step 4 Use glue studio to read the parquet files and join the four relational feature tables into one single table. Then export the dataframe as a single csv into s3 bucket. Please check the glue-job.ipynb notebook for the  codes.
![](assets/images/p14.png)