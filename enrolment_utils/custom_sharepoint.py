from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import os
# import python_utils
from enrolment_utils import global_params, python_utils

from shareplum import Office365
from shareplum import Site
from shareplum.site import Version

from pathlib import Path

import pandas as pd
import io
from io import StringIO 

#Constrtucting SharePoint URL and credentials 
def sharepoint_download(sharepoint_base_url,
                        local_folder,
                        file_name,
                        col2_name = 'School'):
    """
    This function downloads a two column file from sharepoint (useful for order, target or capacity files)
    
    Args: 
        sharepoint_base_url (str): Sharepoint base url
        local_folder (str): folder within sharepoint containing file of interest
        file_name (str): name of file to download
        col2_name (str): Name to be given to second column (school set by default)
    
    Returns: 
        pd.DataFrame containing the data of interest (tipically, two columns)
    
    Example Usage: 
         sharepoint_download(sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/',
                                    local_folder = 'orders',
                                    file_name = 'order.txt')
    """
	# Getting credentials
    sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
    
    # Root location for project
    folder_in_sharepoint = '/sites/EnrolmentDashboard/Shared%20Documents/'+local_folder+'/'

    #Constructing Details For Authenticating SharePoint
    auth = AuthenticationContext(sharepoint_base_url)
    auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()

    #Reading File from SharePoint Folder
    sharepoint_file = folder_in_sharepoint+file_name
    response = File.open_binary(ctx, sharepoint_file)
    
    # Wrangling file into a file, reading line by line 
    s = response.text
    order = pd.DataFrame()
    for i in range(len(s.splitlines())):
        order.loc[i,'Program'] = s.splitlines()[i].split('\t')[0]
        order.loc[i,col2_name] = s.splitlines()[i].split('\t')[1]
    order = order.drop(labels=0, axis=0)
    
    
    return order;

def sharepoint_download_excel(sharepoint_base_url, 
                              file_name, 
                              sheet_name = None,
                              folder = None):
    """
    This function downloads a dataframe embedded into a excel file directly from sharepoint.
    
    Args: 
        sharepoint_base_url (str): Sharepoint base url 
        name (str): name of file to download
        folder (str): target folder (if available)
    
    Returns: 
        pd.DataFrame containing the data of interest 
    
    Example Usage: 
         sharepoint_download_excel(sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/',
                                    name = 'order.txt')
    """
	# Getting credentials
    sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
    
    # Root location for project
    if folder is None: 
        folder_in_sharepoint = '/sites/EnrolmentDashboard/Shared%20Documents/'
    else:
        folder_in_sharepoint = f'/sites/EnrolmentDashboard/Shared%20Documents/{folder}/'

    #Constructing Details For Authenticating SharePoint
    auth = AuthenticationContext(sharepoint_base_url)
    auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    
    #Reading File from SharePoint Folder
    sharepoint_file = folder_in_sharepoint+file_name
    response = File.open_binary(ctx, sharepoint_file)
    
	#save data to BytesIO stream
    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0) #set file object to start
    
	#read excel file and each sheet into pandas dataframe 
    if sheet_name is None: 
        df = pd.read_excel(bytes_file_obj, 
                           engine='openpyxl', 
                           keep_default_na=False)
    if sheet_name is not None: 
        df = pd.read_excel(bytes_file_obj, 
                           engine='openpyxl', 
                           sheet_name = sheet_name,
                           keep_default_na=False)

    return df



def sharepoint_upload(sharepoint_base_url,
                        root, 
                        report_name,
                        file_name = 'DashboardInput.xlsx'):
        """
        This function uploads a dataframe embedded into a excel file directly to sharepoint.
        
        Args: 
            sharepoint_user (str): Sharepoint user
            sharepoint_password (str): Sharepoint password
            sharepoint_base_url (str): Sharepoint base url 
            root (str): local root path
            report_name (str): Name of the report to be uploaded
            file_name (str): name given to the upload file in Sharepoint
        
        Returns: 
            pd.DataFrame containing the data of interest 
        
        Example Usage: 
            sharepoint_upload(sharepoint_user = sharepoint_user,
                                sharepoint_password = sharepoint_password,
                                sharepoint_base_url = sharepoint_base_url,
                                root = root,
                                report_name = report_name)
        """
        # Getting credentials
        sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
        
        # Folder URL relative to the site URL where the file will be stored
        folder_url = 'Shared Documents/'
        

        # Authenticate to SharePoint
        auth_ctx = AuthenticationContext(sharepoint_base_url)
        if auth_ctx.acquire_token_for_user(sharepoint_user, sharepoint_password):
            client_ctx = ClientContext(sharepoint_base_url, auth_ctx)

            # Get the target folder
            target_folder = client_ctx.web.get_folder_by_server_relative_url(folder_url)

            # Read file and upload
            report_name = report_name+".xlsx"
            file_path = Path(root /'reports'/ report_name)

            with open(file_path, 'rb') as content_file:
                file_content = content_file.read()
            target_file = target_folder.upload_file(file_name, file_content)
            client_ctx.execute_query()

            print(f"[Info] {file_name} successfully uploaded to SharePoint.")
        else:
            print(f"Failed to authenticate to SharePoint for user {sharepoint_user}.")
            
        return None;


# def upload_dataframe_as_txt_to_sharepoint(sharepoint_url, 
#                                           sharepoint_user, 
#                                           sharepoint_password, 
#                                           dataframe, 
#                                           destination_folder_url, 
#                                           file_name):
#     ctx_auth = AuthenticationContext(sharepoint_url)
#     ctx_auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
#     ctx = ClientContext(sharepoint_url, ctx_auth)

#     # Convert DataFrame to text (CSV format in this example)
#     csv_data = dataframe.to_csv(index=False)

#     # Get the destination folder
#     destination_folder = ctx.web.get_folder_by_server_relative_url(destination_folder_url)

#     # Upload the CSV data as a text file
#     target_file = destination_folder.upload_file(file_name, StringIO(csv_data))

#     ctx.execute_query()
#     return None; 

def upload_dataframe_as_txt_to_sharepoint(sharepoint_base_url, 
                                          dataframe, 
                                          file_name,
                                          destination_folder_url = '/sites/EnrolmentDashboard/Shared%20Documents/'):
    """
    This function uploads a given dataframe to a txt file within sharepoint. Handy for side files on various projects (like enrolment report)
    
    Args: 
        sharepoint_base_url (str): Sharepoint base url 
        dataframe (pd.DataFrame): DataFrame of interest
        file_name (str): name of file to download
        destination_folder_url (str): target location
    
    Returns: 
        None
    
    Example Usage: 
         upload_dataframe_as_txt_to_sharepoint( sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/',
                                                dataframe = dataframe, 
                                                file_name = 'file.txt')
    """
	# Getting credentials
    sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
    
    # Creating connection 
    ctx_auth = AuthenticationContext(sharepoint_base_url)
    ctx_auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, ctx_auth)

    # Convert DataFrame to text (CSV format in this example)
    csv_data = dataframe.to_csv(index=False, sep='\t')

    # Get the destination folder
    destination_folder = ctx.web.get_folder_by_server_relative_url(destination_folder_url)

    # Upload the CSV data as a text file
    target_file = destination_folder.upload_file(file_name, StringIO(csv_data))

    ctx.execute_query()
    return None


def list_files_in_sharepoint(term, 
                             folder):
    """
    Lists files within a SharePoint folder.

    Args:
		- term (str): term of interest
		- folder (str): folder to pull data

    Returns:
		- List of file names within the specified SharePoint folder.
		- Connection to folder within Sharepoint
    """
    # Loading credentials
    sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
    
    # helper dict
    intake_dict = global_params.intake_dict()
    # {'F':'fall',
    #                'W':'winter',
    #                'S':'spring'}
    # setting locations
    
    sharepoint_url = 'https://mylambton.sharepoint.com'
    folder_url  = f'/sites/EnrolmentDashboard/Shared%20Documents/program_history/{folder}/{intake_dict[term[-1]]}'
    
    # Authenticate with SharePoint
    authcookie = Office365(sharepoint_url, 
                           username=sharepoint_user, 
                           password=sharepoint_password).GetCookies()
    site = Site(sharepoint_url+'/sites/EnrolmentDashboard/',  
                version=Version.v2016,
                authcookie=authcookie)

    # Get folder
    folder = site.Folder(folder_url)

    # List files
    files = folder.files
    files = [file['Name'] for file in files]
    return files, folder
