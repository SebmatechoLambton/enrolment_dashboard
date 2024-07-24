from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from shareplum import Office365
from shareplum import Site
from shareplum.site import Version
from enrolment_utils import global_params
import numpy as np
from typing import List
import pandas as pd
import io
# import python_utils
from enrolment_utils import custom_sharepoint, python_utils
def sharepoint_download_excel_OCAS(sharepoint_base_url:str, 
                                   report_name:str):
    """
    Importing OCAS College Counts report from Sharepoint
    
    This function performs the regular data extraction from OCAS report involving 
    applications, offers and confirmations, tipically including -College Counts-
    in name. This file is to be pulled out of Sharepoint. 
    
    The file was scheduled to be sent directly from OCAS to email every day. Then, using power
    automate tools, the process of embedding it into sharepoint was automate
    
    
    Args:
        sharepoint_base_url: str
            Sharepoint location of file
        report_name: str
            Report name to file given within sharepoint repository

    Returns:
        df: pd.DataFrame
            data frame containing raw data from OCAS 

    
    Example: 
        OCAS_utils.OCAS_enrolment_report(report_name = report_name,
                                        allapplicants_flag = True)
    
     """
	# Getting credentials
    sharepoint_user, sharepoint_password =  python_utils.load_credentials(sharepoint = True)
    

    folder_in_sharepoint = '/sites/EnrolmentDashboard/Shared%20Documents/OCAS/'

    #Constructing Details For Authenticating SharePoint

    auth = AuthenticationContext(sharepoint_base_url)
    auth.acquire_token_for_user(sharepoint_user, sharepoint_password)
    ctx = ClientContext(sharepoint_base_url, auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()

    # #Reading File from SharePoint Folder
    sharepoint_file = folder_in_sharepoint+report_name
    response = File.open_binary(ctx, sharepoint_file)

    #save data to BytesIO stream
    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0) #set file object to start

    #read excel file and each sheet into pandas dataframe 
    df = pd.read_excel(bytes_file_obj)
    return df

def OCAS_enrolment_report(dataset: pd.DataFrame,
                          allapplicants_flag: bool = True):
    """
    Transforming data from OCAS College Counts report (no previous handling required)
    
    This function performs the regular data wrangling for OCAS report involving 
    applications, offers and confirmations, tipically including -College Counts-
    in name.
    
    This function is aimed to take the dataframe containing the raw data pulled from OCAS
    
    Args:
        dataset: pd.DataFrame
            Dataset name to be imported
        allapplicants_flag: bool = True 
            Flag for only exporting all applicants columns (Direct/Non-direct). Set as TRUE by default

    Returns:
        df: pd.DataFrame
            Cleaned data frame containing what initially contained in the input file 

    
    Example: 
        OCAS_utils.OCAS_enrolment_report(report_name = report_name,
                                        allapplicants_flag = True)
    
     """
    # Creating file name and inputing it

    df = dataset.iloc[2: , :]
    # df = pd.read_excel(file,skiprows=[0,1])


    # Fill all horizontal spaces (filling blanks) on first five rows (names) 
    df.iloc[:5,:] =df.iloc[:5,:].ffill(axis = 1)

    # Creating columns names summarizing the multi index column naming   
    df.iloc[5,:] =df.iloc[2,:].str.title()+'_'+df.iloc[0,:]

    # Using data of generation to name output file
    date_string = df.iloc[-2,0]

    # Taking only columns with All Applicants (flag set by default)
    if allapplicants_flag: 
        columns = list(df.iloc[4,:] == 'All Applicants')
        columns[1] = True
        df = df.loc[:, columns]

    # Removing first five rows 
    df = df.iloc[5: , :]

    # Renaming the first column (program)
    df.iloc[0,0]= 'Program'

    # Setting first row as column names
    df.columns = df.iloc[0]
    df = df.reset_index(drop = True)
    df = df.drop(0)

    # Remove last row and filling blanks
    df = df[~df['Program'].isnull()].fillna(0)
    return df


def OCAS_cleaning_process(order: pd.DataFrame, 
                          dataset: pd.DataFrame):
    """
    Splitting OCAS data into Apps, offers and confs merged with order data
    
    This function will take the order dataframe and cleaned OCAS dataframe to merge them 
    and to split them by applications, offers and confirmation
    
    Args:
        order: pd.DataFrame
            order file (including schools) to used to select programs to be displayed 
        dataset: pd.DataFrame 
            Cleaned version of OCAS data. 

    Returns:
        df_apps: pd.DataFrame
            Cleaned data frame containing historical OCAS data on applications
        df_offs: pd.DataFrame
            Cleaned data frame containing historical OCAS data on offers
        df_cons: pd.DataFrame
            Cleaned data frame containing historical OCAS data on confirmations. 

    
    Example: 
         df_apps, df_offs, df_cons = OCAS_data.OCAS_cleaning_process(order = order,
                                                                     dataset = dataset)
    
    """
    # merging clean data with order
    df_aux = order.merge(dataset, on = 'Program', how = 'left' )
    
    # Putting aside apps, offs and confs on separate and cleaned dataframes
    df_apps = df_aux.filter(regex = r'App|Program|School').fillna(0)
    df_offs = df_aux.filter(regex = r'Off|Program|School').fillna(0)
    df_cons = df_aux.filter(regex = r'Con|Program|School').fillna(0)
    
    # return
    return df_apps, df_offs, df_cons


def OCAS_enrolment_report_province(dataset: pd.DataFrame):
    """
    Transforming data from OCAS College Province report (no previous handling required)
    
    This function performs the regular data wrangling for OCAS report involving 
    applications, offers and confirmations, tipically for every college in the province. 
    This is an open comprehensive report. 
    
    This function is aimed to take the dataframe containing the raw data pulled from OCAS
    
    Args:
        dataset: pd.DataFrame
            Dataset name to be imported

    Returns:
        df: pd.DataFrame
            Cleaned data frame containing what initially contained in the input file 

    
    Example: 
        OCAS_utils.OCAS_enrolment_report_province(dataset = report_name,)
    
    """
    # Catchments dictionary 
    # catchment_dict = {'algo_catchment': 'Algonquin', 
    #                   'gbtc_catchment': 'gbtc_catchment', 
    #                   'sene_catchment': 'Seneca', 
    #                   'sher_catchment': 'Sheridan',  
    #                   'slaw_catchment': 'St. Lawrence', 
    #                   'ssfl_catchment': 'ssfl_catchment', 
    #                   'unknown catchment name eng': 'Unknown', 
    #                   'cons_catchment': 'Conestoga', 
    #                   'durh_catchment': 'Durham', 
    #                   'fans_catchment': 'Fanshawe',
    #                   'geor_catchment': 'Georgian', 
    #                   'moha_catchment': 'Mohawk', 
    #                   'nort_catchment': 'Northern', 
    #                   'cent_catchment': 'Centennial', 
    #                   'lamb_catchment': 'Lambton', 
    #                   'stcl_catchment': 'St. Clair', 
    #                   'camb_catchment': 'Cambrian', 
    #                   'cana_catchment': 'Canadore', 
    #                   'conf_catchment': 'Confederation', 
    #                   'humb_catchment': 'Humber', 
    #                   'loyt_catchment': 'Loyalist', 
    #                   'niag_catchment': 'Niagara', 
    #                   'saul_catchment': 'Sault'}
    

    # Creating file name and inputing it
    df = dataset.iloc[1: , :]
    # df = pd.read_excel(file,skiprows=[0,1])

    # Fill all horizontal spaces (filling blanks) on first five rows (names) 
    df.iloc[:1,:] =df.iloc[:1,:].ffill(axis = 1)

    # drop null columns
    df = df.drop(columns = df.columns[[3,4,5]])

    # Creating columns names summarizing the multi index column naming   
    df.iloc[3,3:] = df.iloc[0,3:].str.lower()+'_'+df.iloc[2,3:].astype(int).astype(str)

    # Removing first three and last two rows 
    df = df.iloc[3:-2, :]

    # Setting first row as column names
    df.columns = df.iloc[0].str.lower().str.strip().str.replace(' ','_')
    df = df.reset_index(drop = True)
    df = df.drop(0)

    # Filling blanks on first two columns
    df.iloc[:,0:2] = df.iloc[:,0:2].ffill(axis = 0)

    # Setting columns to be lowercase
    df.iloc[:,0] = df.iloc[:,0].str.title()
    df.iloc[:,1] = df.iloc[:,1].str.title()
    df.iloc[:,2] = df.iloc[:,2].str.lower()

    # Keeping Apps, offers and confs
    df = df[df.columns.drop(list(df.filter(regex='reg|enro')))]

    # Filling blanks
    df = df.fillna(0)

    # Dropping blank catchments
    df = df[df['college_catchment']!=0]

    # Columns to integer
    for column in df.columns[3:]:
        df[column] = df[column].astype(int)


    # Renaming college catchments
    catchment_dict = global_params.catchment_convention()
    df['college_catchment'] = df['college_catchment'].map(catchment_dict)
    

    return df 


def OCAS_enrolment_report_province_format(dataset: pd.DataFrame, 
                                          terms: List[str]):
    """
    This function transform OCAS provincial report data into the same data representation found on the portal 
    
    Args: 
        dataset (pd.DataFrame): dataset with OCAS comprehensive report cleaned data
        terms (List): List of terms for current reporting cycle
    
    Returns 
        pd.DataFrame with final number representation of OCAS data
    """
    # Same classification as OCAS webpage
    # college_size_dict = {'Algonquin':'Large',
    # 'Cambrian':'Small',
    # 'Canadore':'Small',
    # 'Centennial':'Large',
    # 'Collège Boréal':'Small',
    # 'Conestoga':'Medium',
    # 'Confederation':'Small',
    # 'Durham':'Medium',
    # 'Fanshawe':'Large',
    # 'Fleming':'Medium',
    # 'George Brown':'Large',
    # 'Georgian':'Medium',
    # 'Humber':'Large',
    # 'La Cité Collégiale':'Medium',
    # 'Lambton':'Small',
    # 'Loyalist':'Small',
    # 'Michener Institute':'Not Classified',
    # 'Mohawk':'Large',
    # 'Niagara':'Medium',
    # 'Niagara Parks School Of Hortic':'Not Classified',
    # 'Northern':'Small',
    # 'Ridgetown Campus':'Not Classified',
    # 'Sault':'Small',
    # 'Seneca':'Large',
    # 'Sheridan':'Large',
    # 'St. Clair':'Medium',
    # 'St. Lawrence':'Medium'}
    
    if terms[-1][-1] == 'W':
        dataset.columns = [update_colname(col) for col in dataset.columns]
    if terms[-1][-1] == 'S':
        dataset.columns = [update_colname(col) for col in dataset.columns]

    columns_of_interest = ['college_name',f'application_{terms[-3][:4]}',f'application_{terms[-2][:4]}',
                       f'application_{terms[-1][:4]}',f'confirmation_{terms[-3][:4]}',
                       f'confirmation_{terms[-2][:4]}', f'confirmation_{terms[-1][:4]}']
    
    dataset = ensure_columns_exist(dataset = dataset, columns_of_interest = columns_of_interest)
    
    # Building same data presentation as in OCAS webpage
    dataset = dataset[columns_of_interest].groupby('college_name').sum().reset_index()
    dataset.insert(3, f'apps_difference_{terms[-2][:4]}_{terms[-3][:4]}', 
                   np.round(((dataset[f'application_{terms[-2][:4]}'] - dataset[f'application_{terms[-3][:4]}']) / dataset[f'application_{terms[-3][:4]}']) * 100,2))
    dataset.insert(5, f'apps_difference_{terms[-1][:4]}_{terms[-2][:4]}', 
                   np.round(((dataset[f'application_{terms[-1][:4]}'] - dataset[f'application_{terms[-2][:4]}']) / dataset[f'application_{terms[-2][:4]}']) * 100,2))
    
    dataset.insert(8, f'confs_difference_{terms[-2][:4]}_{terms[-3][:4]}', 
                   np.round(((dataset[f'confirmation_{terms[-2][:4]}'] - dataset[f'confirmation_{terms[-3][:4]}']) / dataset[f'confirmation_{terms[-3][:4]}']) * 100,2))
    dataset.insert(10, f'confs_difference_{terms[-1][:4]}_{terms[-2][:4]}', 
                   np.round(((dataset[f'confirmation_{terms[-1][:4]}'] - dataset[f'confirmation_{terms[-2][:4]}']) / dataset[f'confirmation_{terms[-2][:4]}']) * 100,2))
        
    # Adding college size flag
    college_size_dict = global_params.colleges_sizes()
    dataset['college_size'] = dataset['college_name'].map(college_size_dict)

    return dataset;

def update_colname(colname):
    """this function updates columns names for winter term data
    
    So, as OCAS would report full cycles as F, W, S under the same year, winter terms are vaguely defined. 
    For example, 2024W  would show up as being 2023. Which makes no sense for college reporting. 
    This function updates such naming. 

	Args:
		colname (str): column name to be updated. 

	Returns:
		colname (str) with additional year
	"""
    # Split column name by "_"
    parts = colname.split("_")
    
    # If the last part is a year, add one year to it
    if parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
    
    # Join the parts back together
    return "_".join(parts)


# def OCAS_net_movement(dataframe:pd.DataFrame, 
#                       terms: List[str]):
#     """
#     This function gets the net movement throughout the provice of Ontario based on MCTU number and matching
#     it with those of the program offered by Lambton
    
#     Args: 
#         dataframe (pd.DataFrame): processed OCAS data (output of OCAS_data.OCAS_enrolment_report_province)
#     """
#     if terms[-1][-1] == 'W':
#         dataframe.columns = [update_colname(col) for col in dataframe.columns]
#     if terms[-1][-1] == 'S':
#         dataframe.columns = [update_colname(col) for col in dataframe.columns]
#     # Programs data hosted in sharepoint
#     sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/'
#     df_programs = custom_sharepoint.sharepoint_download_excel(sharepoint_base_url = sharepoint_base_url,
#                                                file_name = 'programs.xlsx')
    
#     # Keeping those programs with MCTU only
#     df_programs = df_programs[['XPGM_PROGRAM', 'MTCU_Code']]
#     df_programs = df_programs[~df_programs['MTCU_Code'].isnull()]
#     df_programs.columns = ['program', 'mctu']
#     df_programs['mctu'] = df_programs['mctu'].astype(int)
    
#     # Filling OCAS data with MCTU data
#     programs_dict = df_programs.set_index('mctu')['program'].to_dict()
#     dataframe['mctu'] = dataframe['mtcu_code_and_title'].str.split('-').str[0]
#     dataframe['lambton_program'] = dataframe['mctu'].astype(int).map(programs_dict)
#     return dataframe


def OCAS_net_movement(dataframe:pd.DataFrame, 
                      terms: List[str], 
                      cnxn = None):
    """
    This function gets the net movement throughout the provice of Ontario based on MCTU number and matching
    it with those of the program offered by Lambton
    
    Args: 
        dataframe (pd.DataFrame): processed OCAS data (output of OCAS_data.OCAS_enrolment_report_province)
        terms (List[str]): list of terms of interest (would take the last letter of last element, as proxy to intake)
    """
    # if terms[-1][-1] == 'W':
    #     dataframe.columns = [update_colname(col) for col in dataframe.columns]
    # if terms[-1][-1] == 'S':
    #     dataframe.columns = [update_colname(col) for col in dataframe.columns]
    

    query = """ select XPGM.XPGM_PROGRAM as program_code
            --,ACPG_TITLE as program_title
            --,ACPG_ACAD_LEVEL as academic_level
            --,ACPG_STATUS as status
            ,XPGM_MTCU_CODE as mctu_code
            --,XPGM_APS_SEQ_NUM as aps_seq
            --,XPGM_BOARD_APPROVED as board_appro
            --,XPGM_COST_RECOVERY as cost_recovery
            --,XPGM_CIP as cip
            --,ACPG_LOCATIONS as location
            --,CCDS_ID + ' ' + CCD_DESC AS program_credential,
            -- Getting the WILL Program identifiers
                --REPLACE(REPLACE(REPLACE((CAST(
                --    (SELECT XPLS.XPGM_KPI_WORK_INTEGRATED AS X 
                 --       FROM XPROGRAM_LS AS XPLS
                   --         WHERE XPLS.XPGM_PROGRAM = XPGM.XPGM_PROGRAM
                   --     FOR XML PATH('')
                   -- )AS VARCHAR(4000))),'</X><X>', ' / '),'<X>', ''),'</X>','') AS WILL
            -- End WILL Selection
        from XPROGRAM XPGM left join PROGRAM_STATUS on XPGM.XPGM_PROGRAM = ACAD_PROGRAMS_ID 
                LEFT JOIN ACAD_PROGRAMS on ACAD_PROGRAMS.ACAD_PROGRAMS_ID = XPGM.XPGM_PROGRAM 
                LEFT JOIN ACAD_PROGRAMS_LS ON ACAD_PROGRAMS_LS.ACAD_PROGRAMS_ID = ACAD_PROGRAMS.ACAD_PROGRAMS_ID 
                LEFT JOIN CCDS ON ACPG_CCDS = CCDS_ID
            where PROGRAM_STATUS.POS = 1 and XPGM.XPGM_PROGRAM not like '%.%' AND ACAD_PROGRAMS_LS.POS = 1
        """ 
    df_programs = pd.read_sql(query, cnxn )
    file_name = 'order_fall.txt' ## This is the most comprehensive list of programs out of the three intakes, useful for ocas net movement tab.
    program_school_dict = global_params.program_school_dict(file_name = file_name)
        
    # # Keeping programs offered in current term with MCTU only
    df_programs = df_programs[df_programs['program_code'].isin(list(program_school_dict.keys()))]
    df_programs = df_programs[~df_programs['mctu_code'].isnull()]
    df_programs['mctu_code'] = df_programs['mctu_code'].astype(int)
    
    # # Filling OCAS data with MCTU data
    programs_dict = df_programs.set_index('mctu_code')['program_code'].to_dict()
    
    # Pulling school/program data
    dataframe['mctu_code'] = dataframe['mtcu_code_and_title'].str.split('-').str[0]
    dataframe['lambton_program'] = dataframe['mctu_code'].astype(int).map(programs_dict)
    
    return dataframe

def ensure_columns_exist(dataset, columns_of_interest):
    """
    Ensure that the dataframe has all the columns of interest. 
    If any column is missing, add it and fill with zeros.

    Args:
    - dataset (pd.DataFrame): Input dataframe
    - columns_of_interest (list): List of columns to ensure existence

    Returns:
    - pd.DataFrame: Dataframe with ensured columns
    """
    for col in columns_of_interest:
        if col not in dataset.columns:
            dataset[col] = 0
    return dataset