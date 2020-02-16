import boto3
from dynamodb_json import json_util as json2
import json 
import re

def search_using_bsb(bsb):
    if re.fullmatch("\d{3}(-|)\d{3}", bsb) == None:
        response = "BSB " + bsb + " is Invalid"
    else:
        if(len(bsb) == 6):
            bsb = bsb[:3] + "-" + bsb[3:]
        search_key = {"bsb"}
        boto_session = boto3.Session(profile_name='rkoi2318')
        dynamo_client = boto_session.client('dynamodb')
        result = dynamo_client.get_item(
            TableName = 'bsb-checker.bsb_list',
            Key = search_key
        )