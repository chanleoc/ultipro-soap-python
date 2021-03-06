from zeep import Client as Zeep
from zeep import xsd
import logging

endpoint = 'EmployeeTermination'

def find_terminations(client, query):
    logging.info("SOAP endpoint: {0}{1}".format(client.base_url, endpoint))
    zeep_client = Zeep("{0}{1}".format(client.base_url, endpoint))
    response = zeep_client.service.FindTerminations(
        _soapheaders=[client.session_header],
        query=query)

    return response['Results']

def get_termination_by_employee_identifier(client, employee_identifier):
    zeep_client = Zeep("{0}{1}".format(client.base_url, endpoint))
    if 'EmployeeNumber' in employee_identifier:
        element = zeep_client.get_element('ns6:EmployeeNumberIdentifier')
        obj = element(**employee_identifier)
    elif 'EmailAddress' in employee_identifier:
        element = zeep_client.get_element('ns6:EmailAddressIdentifier')
        obj = element(**employee_identifier)

    response = zeep_client.service.GetTerminationByEmployeeIdentifier(
        _soapheaders=[client.session_header],
        employeeIdentifier=obj)

    return response['Results']
