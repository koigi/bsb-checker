import boto3
import csv
import json
from dynamodb_json import json_util as json2
from datetime import datetime , timezone

def read_file(input="../data/sample.csv"):
    with open(input, "r") as csv_file:
        column_names = ['bsb', 'fi_code', 'name', 'address', 'suburb', 'state','postcode', 'payment_systems']
        csvReader = csv.DictReader(csv_file, fieldnames=column_names)

        last_edited_timestamp = int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)
        
        final_output = []
        for rows in csvReader:
            rows["last_updated"] = last_edited_timestamp
            status = "Active"
            if("Merged" == rows["name"]):
                status = "Merged"
            elif ("Closed" == rows["name"] ):
                status = "Closed"

            if("" == rows["payment_systems"]):
                rows["payment_systems"] = None

            rows["status"] = status
            final_output.append(rows)

        return json.dumps(final_output)        

def write_log(update_results):
    with open("log.txt", "a") as log_file:        
        log_file.write(update_results)

def update_table(json_data=None):
    log_entry = ""
    if(None == json_data):
        log_entry = "No JSON DATA ENTERED"
    else:
        responses =[]
        write_log("\n \tNEW RUN STARTING\n") 
        for row in json.loads(json_data):
            if ("" != row): #sneaky new line in the csv file
                input_data = json2.dumps(row, as_dict=True)
                boto_session = boto3.Session(profile_name='rkoi2318')
                dynamo_client = boto_session.client('dynamodb')
                a_response = dynamo_client.put_item(
                    TableName = 'bsb-checker.bsb_list',
                    Item = input_data
                )
                responses.append(a_response)
                log_entry = log_entry + "Results of adding BSB " + row["bsb"] + " into Dynamo DB table 'bsb-checker.bsb_list': Response Code: "
                log_entry = log_entry + str(a_response["ResponseMetadata"]["HTTPStatusCode"]) + " , Number of attempts: " + str(a_response["ResponseMetadata"]["RetryAttempts"]) + "\n"
            else:
                log_entry = "Skipping Empty Row"
        write_log(log_entry)


if __name__ == "__main__":
    #file_content = read_file("../data/BSBDirectoryJan20-286.csv") 
    file_content = read_file()
    update_table(file_content)
    

        