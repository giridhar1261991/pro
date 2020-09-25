'''

import os
import boto3
import json

IDW_CONFIG_PREFIX = os.getenv('IDWPV_CONFIG_PREFIX')
if not IDW_CONFIG_PREFIX:
    raise Exception('The IDWPV_CONFIG_PREFIX environment variable is not set. Set this value to continue.')


if not IDW_CONFIG_PREFIX == "idwplanvw/tfs":
    ssm = boto3.client('ssm')

    # Note: run this code here rather than within my_handler so that the config
    # settings are only gotten on startup and not with every execution
    app_config_parameter_name = f'{IDW_CONFIG_PREFIX}/config'
    response = ssm.get_parameter(Name=app_config_parameter_name, WithDecryption=True)
    app_config_json = response['Parameter']['Value']
else:
    app_config_json = os.getenv('IDWPV_CONFIG')

app_config = json.loads(app_config_json)

planview_service_user = app_config['pv_service_user']
planview_service_user_pwd = app_config['pv_service_user_pwd']
planview_url = app_config['pv_url']

datawarehouse_name = app_config['datawarehouse_name']
password = app_config['dbuser_password']
datawarehouse_host = app_config['db_host']

user_name = 'svc_etl_user'
datawarehouse_schema_name = 'idw'
staging_schema_name = 'birepusr' '''

#planview_service_user = 'webservicesapi'
#planview_service_user_pwd = 'Snomed123!'
#planview_url = "https://e-imo.ppmpro.com/services/MainService?wsdl"

planview_service_user = 'webservicesapi_sb'
planview_service_user_pwd = 'Snomed123'
planview_url = "https://e-imo-sb.ppmpro.com/services/MainService?wsdl"



datawarehouse_name = 'idwdev'
password = 'pwd'
datawarehouse_host = 'idwdev.cm0t4637eidl.us-east-1.rds.amazonaws.com'

user_name = 'svc_etl_user'
datawarehouse_schema_name = 'idw'
staging_schema_name = 'birepusr'