"__author__ = 'Leo Chan'"
"__credits__ = 'Keboola 2017'"
"__project__ = 'roti/ultipro'"

"""
Python 3 environment 
Custom Integration specifically for Employee Termination Endpoint
"""


#import click
import sys
import os
import time
import datetime
import decimal
import json
import csv
import pandas as pd
import logging
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker

import configparser
from client import UltiProClient
import helpers as helpers
from services import employee_termination
from services import login


### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
"""
logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])
"""

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
base_url = cfg.get_parameters()["base_url"]
username = cfg.get_parameters()["username"]
password = cfg.get_parameters()["#password"]
customer_api_key = cfg.get_parameters()["#customer_api_key"]
api_key = cfg.get_parameters()["#api_key"]


### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

timestr = time.strftime("%Y%m%d-%H%M%S")

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"

class UltiProEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)
        if isinstance(obj, decimal.Decimal):
            return float(obj)

        #return json.UltiProEncoder.default(self, obj)
        return UltiProEncoder.default(self, obj)

def create_client():
    client = UltiProClient(
        username,
        password,
        customer_api_key,
        api_key,
        base_url
    )
    return client

def write_json(r):
    #outfile = ctx.obj['outfile'] + '.json'
    outfile = "out.json"
    #click.echo(f"JSON saved to: {outfile}")
    json_str = json.dumps(r, cls=UltiProEncoder, ensure_ascii=False, indent=4, sort_keys=True)

    with open(outfile, 'w') as f:
        f.write(json_str)

    return json_str

def mapping(data):
    """
    Mapping Output to desired manner
    """

    out = []
    for i in data["EmployeeTerminationInfo"]:
        entry = {}
        entry["CompanyCode"] = i["CompanyCode"]
        entry["EmployeeNumber"] = i["EmployeeNumber"]
        entry["FirstName"] = i["FirstName"]
        entry["LastName"] = i["LastName"]
        entry["EligibleForRehire"] = i["TerminationInfo"]["TerminationInfo"][0]["EligibleForRehire"]
        entry["LastDayWorked"] = i["TerminationInfo"]["TerminationInfo"][0]["LastDayWorked"]
        entry["Notes"] = i["TerminationInfo"]["TerminationInfo"][0]["Notes"]
        entry["PaidThroughDate"] = i["TerminationInfo"]["TerminationInfo"][0]["PaidThroughDate"]
        entry["Status"] = i["TerminationInfo"]["TerminationInfo"][0]["Status"]
        entry["TerminationDate"] = i["TerminationInfo"]["TerminationInfo"][0]["TerminationDate"]
        entry["TerminationReason"] = i["TerminationInfo"]["TerminationInfo"][0]["TerminationReason"]
        entry["TerminationType"] = i["TerminationInfo"]["TerminationInfo"][0]["TerminationType"]
        entry["TimeClock"] = i["TerminationInfo"]["TerminationInfo"][0]["TimeClock"]

        out.append(entry)

    return out

def get_tables(in_tables):
    """
    Evaluate input and output table names.
    Only taking the first one into consideration!
    """

    ### input file
    table = in_tables[0]
    in_name = table["full_path"]
    in_destination = table["destination"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))
    
    return in_name

def get_output_tables(out_tables):
    """
    Evaluate output table names.
    Only taking the first one into consideration!
    """

    ### input file
    table = out_tables[0]
    in_name = table["full_path"]
    in_destination = table["source"]
    logging.info("Data table: " + str(in_name))
    logging.info("Input table source: " + str(in_destination))

    return in_name

def produce_manifest(file_name, primary_key):
    """
    Dummy function to return header per file type.
    """

    file = "/data/out/tables/"+str(file_name)+".manifest"
    destination_part = file_name.split(".csv")[0]

    manifest_template = {#"source": "myfile.csv"
                         #,"destination": "in.c-mybucket.table"
                         #"incremental": True
                         #,"primary_key": ["VisitID","Value","MenuItem","Section"]
                         #,"columns": [""]
                         #,"delimiter": "|"
                         #,"enclosure": ""
                        }

    column_header = []

    manifest = manifest_template
    manifest["primary_key"] = primary_key
    manifest["incremental"] = True

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output manifest file produced.")
    except Exception as e:
        logging.error("Could not produce output file manifest.")
        logging.error(e)
    
    return

def main():
    """
    Main execution script.
    """

    client = create_client()
    login.authenticate(client)
    logging.info("Login Successful.")

    query = {}
    query["PageSize"]="100"
    pagenumber = 1
    pagetotal = 100
    #query["PageNumber"] = "1"

    response = []

    while pagetotal == 100:
        query["PageNumber"] = str(pagenumber)

        data = employee_termination.find_terminations(client, query)
        data_serial = helpers.serialize(data)
        #print(data)
        if data_serial == None:
            pagetotal = 0
        else:
            data_mapped = mapping(data_serial)
            response += data_mapped

            pagetotal = len(data_serial["EmployeeTerminationInfo"])
            logging.info("Request Parameter - Current PageSize: {0}, Current PageNumber: {1}".format(pagetotal,pagenumber))
            pagenumber += 1
        #print("NEXT")

    #write_json(response)
    ### Converting to CSV
    out = pd.DataFrame(response)
    out_columns = ["CompanyCode","EmployeeNumber","FirstName","LastName","EligibleForRehire","LastDayWorked","Notes","PaidThroughDate","Status","TerminationDate","TerminationReason","TerminationType","TimeClock"]

    out.to_csv(DEFAULT_FILE_DESTINATION+"EmployeeTermination.csv", columns=out_columns, index=False)
    out_pk = ["EmployeeNumber"]
    produce_manifest("EmployeeTermination.csv", out_pk)



    return


if __name__ == "__main__":

    main()

    logging.info("Done.")
