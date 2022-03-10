import json
import boto3

client = boto3.client('databrew')

def lambda_handler(event, context):
    try:
        bucket_name=event['Records'][0]['s3']['bucket']['name']
        print('bucket name is :{}'.format(bucket_name))
        csv_key=event['Records'][0]['s3']['object']['key']
        print('key name is :{}'.format(csv_key))
        #Create DataSet
        client.create_dataset(
        Name='test2',
        Format='CSV',
        FormatOptions={
            'Csv': {
                'Delimiter': ',',
                'HeaderRow': True
            }
        },
        Input={
            'S3InputDefinition': {
                'Bucket': 'lilly-landing',
                'Key': 'edu.csv'
            }})
        
        #Create Recipe : https://docs.aws.amazon.com/databrew/latest/dg/recipes.html
        response = client.create_recipe(
        Description='string',
        Name='dstest2',
        Steps=[
            {
                'Action': {
                    'Operation': 'string',
                    'Parameters': {
                        'string': 'string'
                    }
                },
                'ConditionExpressions': [
                    {
                        'Condition': 'string',
                        'Value': 'string',
                        'TargetColumn': 'string'
                    },
                ]
            },
        ],
        Tags={
            'string': 'string'
        }
    )
            
            
        #Create Project
        response = client.create_project(
        DatasetName='test2',
        Name='dstest2',
        RecipeName='dstest2',
        RoleArn='arn:aws:iam::337769067848:role/service-role/AWSGlueDataBrewServiceRole-test'
    )
    
    response = client.create_recipe_job(
    DatasetName='string',
    Name='string',
    Outputs=[
        {
            'CompressionFormat': 'GZIP'|'LZ4'|'SNAPPY'|'BZIP2'|'DEFLATE'|'LZO'|'BROTLI'|'ZSTD'|'ZLIB',
            'Format': 'CSV'|'JSON'|'PARQUET'|'GLUEPARQUET'|'AVRO'|'ORC'|'XML',
            'PartitionColumns': [
                'string',
            ],
            'Location': {
                'Bucket': 'string',
                'Key': 'string'
            },
            'Overwrite': True|False,
            'FormatOptions': {
                'Csv': {
                    'Delimiter': 'string'
                }
            }
        },
    ],
    ProjectName='string',
    RecipeReference={
        'Name': 'string',
        'RecipeVersion': 'string'
    },
    RoleArn='string'
)
    
    except Exception as e:
        return {
        'statusCode': 400,
        'body': json.dumps('Hello from Lambda!  with error ' + str(e))
    }
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
