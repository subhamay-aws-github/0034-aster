#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 28 08:41:01 2022

@author: Subhamay Bhattacharyya
"""

import json
import logging
import boto3
import os
import base64
import time

# Load the exceptions for error handling
from botocore.exceptions import ClientError, ParamValidationError
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

dynamodb_table = os.environ.get("DYNAMODB_TABLE")
dynamodb_client = boto3.client('dynamodb', region_name=os.environ.get("AWS_REGION"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def python_obj_to_dynamo_obj(python_obj: dict) -> dict:
    serializer = TypeSerializer()
    return {
        k: serializer.serialize(v)
        for k, v in python_obj.items()
    }
    
def dynamodb_obj_to_python_obj(dynamodb_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v)
        for k, v in dynamodb_obj.items()
    }

def dynamodb_item_exists(partitionKey, sortKey):
    try:
        response = dynamodb_client.get_item(
                            TableName=dynamodb_table,
                            Key={
                                'partitionKey': {'S': partitionKey},
                                'sortKey': {'S': sortKey}
                            }
                    )

        if not response.get('Item'):
            logger.info(f"The item {dict(partitionKey=partitionKey,sortKey=sortKey)} does not exist.")
            return False
        else:
            dynamodb_item = dynamodb_obj_to_python_obj(response['Item'])
            logger.info(f"dynamodb_item = {dynamodb_item}")
            return True
 
        
    # An error occurred
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}") 
    except ClientError as e:
        logger.error(f"Client error: {e}")
        
def dynamo_db_put_item(data):
    try:
        response = dynamodb_client.put_item(
            TableName = dynamodb_table,
            Item=python_obj_to_dynamo_obj(dict(
                                                partitionKey = f"{data['Customer']}_{data['InvoiceNo']}",
                                                sortKey = data['StockCode'],
                                                Description = data['Description'],
                                                Quantity = data['Quantity'],
                                                InvoiceDate = data['InvoiceDate'],
                                                UnitPrice = data['UnitPrice'],
                                                Country = data['Country']
                                                )
                                            )
        )
        return response
    # An error occurred
    except ParamValidationError as e:
        logger.error(f"Parameter validation error: {e}")
    except ClientError as e:
        logger.error(f"Client error: {e}")
        

def lambda_handler(event, context):
    logger.info(f"event = {json.dumps(event)}")
    records_processed = 0
    try:
        logger.info(f"Total number records in the event : {len(event['Records'])}")
        if event['Records'][0]['eventSource'] == 'aws:kinesis':
            logger.info(f"Number of records  = {len(event['Records'])}")
            for record in event['Records']:
                data = json.loads(base64.b64decode(record['kinesis']['data']).decode("UTF-8"))
                partitionKey = f"{data['Customer']}_{data['InvoiceNo']}"
                sortKey = data['StockCode']
                
                item_exists = dynamodb_item_exists(partitionKey=partitionKey,sortKey=sortKey)
                response = dynamo_db_put_item(data)
                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    records_processed += 1
                    if item_exists:
                        logger.info("Updated the existing item successfully")
                    else:
                        logger.info("Inserted the new item successfully")
        else:
            logger.info("Unknown Event Source")
    except Exception as e:
        logger.error(f"Error in lambda_handler: {e}")
        raise
    
    logger.info(f"Total number records processed : {records_processed} ")
    return "success"