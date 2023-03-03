import psycopg2
import boto3

def Read_Parquet_File():
    s3 = boto3.resource('s3')
    bucket=s3.Bucket("bucket_name")
    contents = [_.key for _ in bucket.objects.all() if "sub_path/" in _.key]
    print(contents)
    return contents[0]

def Write_to_Redshift():
    
    path_to_s3_parquet_file = 'bucket_name/' + Read_Parquet_File()
    
    print(path_to_s3_parquet_file)
    
    con=psycopg2.connect("dbname=database_name host=host_name port=5439 user=USER_NAME password=PASSWORD")
    cur = con.cursor()
    print("Started")
    
    sql_command = "COPY schema.table_name from 's3://" + path_to_s3_parquet_file + '\' ' + "iam_role 'arn:read_s3_bucket' format as PARQUET;"
    print(sql_command)
    
    cur.execute("truncate table schema.table_name")
    
    cur.execute(sql_command)
    cur.execute("commit;")
    
    print("Done- Commit")

if __name__ == "__main__":
    Write_to_Redshift()
