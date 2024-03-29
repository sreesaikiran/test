import boto3

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Specify your table name
table_name = 'your_table_name'
table = dynamodb.Table(table_name)

# Define the primary key of the item you want to check/update
primary_key = {'your_primary_key_name': 'your_primary_key_value'}

# Define the new version value to append
new_version = 'version_value'

# Update the item in the table
response = table.update_item(
    Key=primary_key,
    UpdateExpression="SET versions_list = list_append(if_not_exists(versions_list, :empty_list), :new_version)",
    ExpressionAttributeValues={
        ':new_version': [new_version],
        ':empty_list': [],
    },
    ReturnValues="UPDATED_NEW"
)

print(response)
