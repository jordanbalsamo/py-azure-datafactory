from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import os

#TODO: implement check in ADL to see if FS/containers/directories exist. ADD TO REQUIREMENTS .txt.
#from azure.storage.filedatalake import DataLakeServiceClient

from dotenv import load_dotenv
## Private/Internal API - liable to change.
from dotenv.main import dotenv_values

DOTENV_PATH = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(DOTENV_PATH)

class AdfInterface(object):
    
    def __init__(self):
        """Initialise class variables by pulling from environ.

        :param SUBSCRIPTION_ID: The subscription identifier for target subscription.
        :type SUBSCRIPTION_ID: str
        :param RG_NAME: The target resource group name.
        :type RG_NAME: str
        :param DF_NAME: The target data factory name.
        :type DF_NAME: str
        :param ARM_CLIENT_ID: Service principal client identifier.
        :type ARM_CLIENT_ID: str
        :param ARM_CLIENT_SECRET: Service principal client secret.
        :type ARM_CLIENT_ID: str
        :param ARM_TENANT_ID: Service principal tenant identifier.
        :type ARM_TENANT_ID: str
        :param PIPELINE_NAME: Target pipeline name.
        :type PIPELINE_NAME: str
        :param PIPELINE_PARAMS: Additional pipeline parameters at time of invocation.
        :type ARM_CLIENT_ID: str
        """
        print('\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('~ {0} began at: {1} ~'.format(os.path.basename(__file__),datetime.now()))
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
        print('\nInitialising environment variables...\n')
        self.subscription_id = os.environ.get('SUBSCRIPTION_ID')
        self.rg_name = os.environ.get('RG_NAME')
        self.df_name = os.environ.get('DF_NAME')
       
        self.pipeline_name = os.environ.get('PIPELINE_NAME')
        self.pipeline_params =  {} if not os.environ.get('PIPELINE_PARAMS') else os.environ.get('PIPELINE_PARAMS')

        #Construct credentials and clients:
        self.credentials = ServicePrincipalCredentials(client_id=os.environ.get('ARM_CLIENT_ID'), secret=os.environ.get('ARM_CLIENT_SECRET'), tenant=os.environ.get('ARM_TENANT_ID'))
        self.resource_client = ResourceManagementClient(self.credentials, self.subscription_id)
        self.adf_client = DataFactoryManagementClient(self.credentials, self.subscription_id)

    def show_env(self):
        """Prints .env vars to console for debug."""

        dotEnvDict = dotenv_values(DOTENV_PATH)
        print('\nDOTENV VARS:\n')
        
        for key, value in dotEnvDict.items():
            print('{0} = {1}'.format(key, value)) 
        print('\n')
        print('DOTENV VAR count: {0}'.format(len(dotEnvDict)))


    def env_to_dict(self):
        """Returns .env file vars to Python dictionary."""

        dotEnvDict = dotenv_values(DOTENV_PATH)

        return dotEnvDict

    def print_item(self, group):
        """Print an Azure object instance."""

        print("\tName: {}".format(group.name))
        print("\tId: {}".format(group.id))
        if hasattr(group, 'location'):
            print("\tLocation: {}".format(group.location))
        if hasattr(group, 'tags'):
            print("\tTags: {}".format(group.tags))
        if hasattr(group, 'properties'):
            self.print_properties(group.properties)


    def print_properties(self, props):
        """Print a ResourceGroup properties instance."""
        if props and hasattr(props, 'provisioning_state') and props.provisioning_state:
            print("\tProperties:")
            print("\t\tProvisioning State: {}".format(props.provisioning_state))
        print("\n\n")

    def print_activity_run_details(self, activity_run):
        """Print activity run details."""

        print("\nPipeline name: {0}\n".format(activity_run[0].pipeline_name)) if activity_run[0].pipeline_name else None
        print('Activity plan:')
        for idx, a in enumerate(activity_run):
            print("\t\tStep in Pipeline: ({0})".format(idx + 1))
            print("\t\tActivity: {0} of type {1} began.".format(a.activity_name, a.activity_type)) if a.activity_name and a.activity_type else None
            
            ##TODO: Logic to determine table names...
            # if a.output and a.output != None and a.activity_type == "Lookup":
            #     for l in a.output.value.items():
            #         print('\t\t\t{0}'.format(a.output.value[l]))
            print("\t\tActivity run status: {0}".format(a.status)) if a.status else None
            print("\t\tActivity duration: {0}ms".format(a.duration_in_ms)) if a.duration_in_ms else None 
        print('\n')

    def create_factory(self, factoryName):
        """Create a data factory. Takes .env as self-initialised input + factory name. Outputs a data factory in RG determined by .env."""

        # rg_params = {'location':'uksouth'}
        # df_params = {'location':'uksouth'}  

        df_resource = Factory(location='uksouth')

        df = self.adf_client.factories.create_or_update(self.rg_name, factoryName, df_resource)
        
        self.print_item(df)

        while df.provisioning_state != 'Succeeded':
            df = self.adf_client.factories.get(self.rg_name, factoryName)
            time.sleep(1)

    def start_pipeline_run(self):
        """Initiate a pipeline run. Inputs come from the .env file. Output can be monitored via console and Azure portal."""
        
        adf = self.adf_client
        
        #Run
        run_response = adf.pipelines.create_run(self.rg_name, self.df_name, self.pipeline_name, parameters=self.pipeline_params)

        #Monitor the pipeline run

        pipeline_run = adf.pipeline_runs.get(self.rg_name, self.df_name, run_response.run_id)

        remaining_retries = 5
        retry_buffer = 15
       
        #Ping status:
        print("\nPipeline run status: \n")
        while pipeline_run.status != 'Succeeded':
            pipeline_run = adf.pipeline_runs.get(self.rg_name, self.df_name, run_response.run_id)
            print("\n\t {0}: {1}".format(datetime.now().strftime("%H:%M:%S"), pipeline_run.status))
            if pipeline_run.status == "InProgress":
                time.sleep(3)
            elif pipeline_run.status == "Failed":
                remaining_retries -= 1
                print('\nPipeline failed... Script run has {0} remaining retries, before posting failure. \n\nWaiting {1} seconds before retry...'.format(remaining_retries, retry_buffer)) if remaining_retries >= 1 else None
                time.sleep(retry_buffer)
                run_response = adf.pipelines.create_run(self.rg_name, self.df_name, self.pipeline_name, parameters=self.pipeline_params)
    
                if remaining_retries <= 0:
                    print('\nFor more information on why the ADF pipeline failed, check ADF Monitor logs: \n\n\thttps://adf.azure.com/monitoring/pipelineruns?factory=%2Fsubscriptions%2F{0}%2FresourceGroups%2F{1}%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2F{2}'.format(self.subscription_id, self.rg_name, self.df_name))
                    exit(1)
            elif pipeline_run.status == "Cancelled":
                print("\nLooks like a process or operator forcibly cancelled the pipeline run... posting failure.")
                exit(1)
                
                
        #Get Activity plan:
        print('\nPreparing post-execution activity plan...\n')

        #Give rest service time to collate results
        time.sleep(8)

        filter_params = RunFilterParameters(last_updated_after=datetime.now() - timedelta(1), last_updated_before=datetime.now() + timedelta(1))
        
        query_response = adf.activity_runs.query_by_pipeline_run(
             self.rg_name, self.df_name, pipeline_run.run_id, filter_params)
        
        self.print_activity_run_details(query_response.value)

def main ():
    
    ai = AdfInterface()

    #Debug:
    print('\nEnvironment variables:\n')
    print('SUBSCRIPTION_ID: {0}'.format(ai.subscription_id))
    print('RG_NAME: {0}'.format(ai.rg_name))
    print('DF_NAME: {0}'.format(ai.df_name))
    print('CREDENTIALS: {0}'.format(ai.credentials))
    print('ARM_CLIENT: {0}'.format(ai.resource_client))
    print('ADF_CLIENT: {0}'.format(ai.adf_client))
    print('PIPELINE_NAME: {0}'.format(ai.pipeline_name))
    print('PIPELINE_PARAMS: {0}'.format(ai.pipeline_params))

    #Create Factory
    #ai.create_factory()

    #Pipeline run
    ai.start_pipeline_run()


if __name__ == "__main__":

    main()

    
    