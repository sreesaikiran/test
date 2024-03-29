import boto3

# Initialize a DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Specify the table name and primary key name
table_name = 'your_table_name'
primary_key_name = 'database_name'

# The value of the primary key for the item you want to check
primary_key_value = 'your_database_name'

# The dictionary you want to append to the versions_list attribute
new_version_dict = {'version': '1.0', 'date': '2023-03-29'}

# Access the table
table = dynamodb.Table(table_name)

# Check if the item with the specified primary key exists
response = table.get_item(Key={primary_key_name: primary_key_value})

# Check if the item exists
if 'Item' in response:
    # Item exists, append the new dictionary to the versions_list attribute
    update_response = table.update_item(
        Key={primary_key_name: primary_key_value},
        UpdateExpression='SET versions_list = list_append(versions_list, :val)',
        ExpressionAttributeValues={':val': [new_version_dict]},
        ReturnValues='UPDATED_NEW'
    )
    print("Updated item:", update_response)
else:
    # Item does not exist, create a new item
    new_item = {
        primary_key_name: primary_key_value,
        'versions_list': [new_version_dict]
    }
    put_response = table.put_item(Item=new_item)
    print("Created new item:", put_response)
