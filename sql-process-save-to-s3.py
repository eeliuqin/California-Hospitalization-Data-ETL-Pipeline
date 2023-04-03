import boto3

# create athena client
def get_athena_client() -> boto3.client:
    client = boto3.client("athena")
    
    return client

# get response from athena client after executing query
def get_query_response(
    client: boto3.client, 
    query: str, 
    database: str, 
    output_location: str
) -> dict:
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        
        # the query result will be saved to S3_OUTPUT_LOCATION/<QueryExecutionId>.csv
        ResultConfiguration={
            "OutputLocation": output_location,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}
        }
    )
    return response

# rename the query result file from <QueryExecutionId>.csv to make it more readable
def rename_query_results(
    query_response: dict,
    bucket_name: str,
    directory: str,
    new_name: str
):
    if query_response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise ValueError('Response code is not 200')
        
    # the query result is automatically named as <QueryExecutionId>.csv
    old_key = f"{directory}/{query_response['QueryExecutionId']}.csv"
    new_key = f"{directory}/{new_name}"
    s3 = boto3.client("s3")    

    # rename it
    s3.copy_object(Bucket=bucket_name, 
                   CopySource={'Bucket': bucket_name, 'Key': old_key}, 
                   Key=new_key)

    # delete the old file
    s3.delete_object(Bucket=bucket_name, Key=old_key)

    # Return a success message
    return {
        'statusCode': 200,
        'body': f'Successfully renamed: {bucket_name}/{new_key}'
    }

  
def lambda_handler(event, context):
    # the SQL query joins 3 tables: hospitalizations, population, and income
    query = """
        WITH county_hospitalization AS (
            SELECT year, county, ROUND(SUM(obsrate), 2) AS observed_rate
            FROM "california-hospitalizations-adverse-events"."hospitalization" h
            GROUP BY 1, 2
            ORDER BY 1, 2
        )
    
        SELECT h.county,
              p.popestimate as population,
              h.observed_rate,
              ROUND(cast(age65plus_tot as double)/cast(age18plus_tot as double)*100, 2) AS age65_adults_percent,
              i.median_family_income
        FROM county_hospitalization h
        LEFT JOIN "california-hospitalizations-adverse-events"."population" p
            ON p.ctyname = concat(h.county, ' County')
        LEFT JOIN "california-hospitalizations-adverse-events"."income" i
            ON i.county = h.county
        WHERE h.year=2014 
              AND p.year=6
        ORDER BY 1
    """
    
    # query from athena database
    athena_database = 'california-hospitalizations-adverse-events'
    save_to_s3_bucket = 'california-data'
    s3_output_dir = 'output'
    # the output location of query results
    s3_output_location = f"s3://{save_to_s3_bucket}/{s3_output_dir}/"
    new_file_name = "ca-hospitalization-population-income.csv"

    athena_client = get_athena_client()
    response = get_query_response(athena_client,
                                  query=query,
                                  database=athena_database,
                                  output_location=s3_output_location)
    result = rename_query_results(response, 
                                  bucket_name=save_to_s3_bucket, 
                                  directory=s3_output_dir,
                                  new_name=new_file_name)
                         
    print(result)
    
if __name__ == "__main__":
    lambda_handler(None, None)
