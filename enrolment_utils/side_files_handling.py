import pandas as pd
from enrolment_utils import custom_sharepoint, global_params, python_utils
import enrolment_utils.python_utils as utils_geral
from typing import List



def setting_order_budget(terms: List[str], 
                         sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard'): 
    """
    This function takes projections file and extract list of programs to be reported for such intake (by keeping 
    only programs where there is a non-zero budget flag and are active programs). These are the 'order' files providing
    the whole project with the list of programs of interest and name of school. Also, it sets apart a file with 
    information about budget for every program. 
    
    After getting that information. This function updates the sharepoint version of them. This is handy as small changes 
    in the projection file (which is not an uncommon thing), will reflect on the final report much faster. 
    
    
    Args: 
        terms (List[str]): List of terms to be reported. (this function is going to take the letter only, which flags the intake)
    
    returns: 
        None, but updates two files within sharepoint: order and budget
    
    last update: Oct 5, 2023
    """
    # Gets data from projection file (so, everytime there are new projections, it should be updated on sharepoint)
    projections = custom_sharepoint.sharepoint_download_excel(sharepoint_base_url = sharepoint_base_url,
                                                                file_name = 'projections.xlsx', 
                                                                folder = 'budgets')



    # The projections file is messy, so grab the rigth term and navigate through it, is fundamental 
    t = projections.isin([terms[-1]]).loc[0,].to_list()

    column = [i for i, x in enumerate(t) if x]

    df1 = projections[['School', 'Program', 'Previous Program Codes', 'Active']]
    df2 = projections.iloc[:,column[0]:column[0]+12]
    projections = pd.concat([df1, df2], axis = 1)

    # Columns of interest
    projections.columns=['School', 'Program', 'Previous Program Codes', 'Active', 'Term',
                        'Level', 'Domestic', 'International', 'Total FT',
                        'Second Career', 'COD   A', 'WSIB', 'Total WFU Eligible',
                        'WFU', 'Duration', 'WFU Total']

    # Only active programs with and actual intake (non-zero budget) are going to be kept. 
    projections = projections.loc[(projections['Active']=='Y')&((projections['Level']==1)|
                                ((projections['Level']==4) & (projections['Program']=='FIRE'))|
                                ((projections['Level']==3) & (projections['Program']=='TREX'))),
                                  ['School','Program','Total FT']]
    projections['Total FT'] = projections['Total FT'].replace({'':'0'}).astype(int)
    projections = projections[projections['Total FT']> 0].rename(columns = {'Total FT':'Budget'})
    projections['Budget'] = projections['Budget'].astype(int)

    projections['Program'] = projections['Program'].str[:4]
    # projections['School'] = projections['School'].str.replace(' ', '')
    # Setting file names based on intake
    file_naming = global_params.naming_files()
    if terms[-1][-1] == 'F': 
        file_name_order = file_naming['fall']['file_name_order']
        file_name_budget = file_naming['fall']['file_name_budget']
    elif terms[-1][-1] == 'W': 
        file_name_order = file_naming['winter']['file_name_order']
        file_name_budget = file_naming['winter']['file_name_budget']
    elif terms[-1][-1] == 'S': 
        file_name_order = file_naming['summer']['file_name_order']
        file_name_budget = file_naming['summer']['file_name_budget']
	

    # Upload order file
    custom_sharepoint.upload_dataframe_as_txt_to_sharepoint(sharepoint_base_url = sharepoint_base_url, 
                                                            dataframe = projections[['Program','School']],
                                                            file_name = file_name_order, 
                                                            destination_folder_url = '/sites/EnrolmentDashboard/Shared%20Documents/orders'
                                                           )
    # Upload budget file
    custom_sharepoint.upload_dataframe_as_txt_to_sharepoint(sharepoint_base_url = sharepoint_base_url, 
                                                            dataframe = projections[['Program','Budget']],
                                                            file_name = file_name_budget, 
                                                            destination_folder_url = '/sites/EnrolmentDashboard/Shared%20Documents/budgets'
                                                           )
    return None; 


def program_information(order: pd.DataFrame,
                        cnxn = None):
    """
    This functions creates a spreadsheet with all program names and codes

    Args:
        order (pd.DataFrame): order dataframe containing programs of interest
        cnxn (None): connection.

    Returns:   
        pd.DataFrame with two columns (program code and program title)
    """

    if cnxn is None: 
        cnxn = python_utils.get_connection()

    query = """
    SELECT XPGM.XPGM_PROGRAM as program
    	,ACPG_TITLE as program_title
    	--,ACPG_ACAD_LEVEL AS program_level
    	--,ACPG_STATUS AS status
    	--,XPGM_MTCU_CODE AS mctu_Code
    	--,XPGM_APS_SEQ_NUM AS aps_seq
    	--,XPGM_BOARD_APPROVED AS board_appr
    	--,XPGM_COST_RECOVERY AS cost_recovery
    	--,XPGM_CIP AS cip
    	--,ACPG_LOCATIONS AS location
    	--,CCDS_ID + ' ' + CCD_DESC AS program_credentials
    	--,
    	-- Getting the WILL Program identifiers
    	--REPLACE(REPLACE(REPLACE((
    	--				CAST((
    	--						SELECT XPLS.XPGM_KPI_WORK_INTEGRATED AS X
    	--						FROM XPROGRAM_LS AS XPLS
    	--						WHERE XPLS.XPGM_PROGRAM = XPGM.XPGM_PROGRAM
    	--						FOR XML PATH('')
    	--						) AS VARCHAR(4000))
    	--				), '</X><X>', ' / '), '<X>', ''), '</X>', '') AS experiential_learning
    -- End WILL Selection
    FROM XPROGRAM XPGM
    LEFT JOIN PROGRAM_STATUS ON XPGM.XPGM_PROGRAM = ACAD_PROGRAMS_ID
    LEFT JOIN ACAD_PROGRAMS ON ACAD_PROGRAMS.ACAD_PROGRAMS_ID = XPGM.XPGM_PROGRAM
    LEFT JOIN ACAD_PROGRAMS_LS ON ACAD_PROGRAMS_LS.ACAD_PROGRAMS_ID = ACAD_PROGRAMS.ACAD_PROGRAMS_ID
    LEFT JOIN CCDS ON ACPG_CCDS = CCDS_ID
    WHERE PROGRAM_STATUS.POS = 1
    	AND XPGM.XPGM_PROGRAM NOT LIKE '%.%'
        -- AND ACPG_STATUS = 'A' -- only keep active programs
    	AND ACAD_PROGRAMS_LS.POS = 1
    
    """
    dataframe = pd.read_sql(query, cnxn)
    dataframe = dataframe[dataframe['program'].isin(list(order['Program'].unique()))]
    return dataframe


def setting_order_budget_ottawa(terms: List[str], 
                                cnxn = None,
                                sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard'): 
    """
    This function takes projections file and extract list of programs to be reported for such intake (by keeping 
    only programs where there is a non-zero budget flag and are active programs) for Ottawa. These are the 'order' 
    files providing the whole project with the list of programs of interest and name of school. Also, it sets apart a 
    file with information about budget for every program. 
    
    Args: 
        terms (List[str]): List of terms to be reported. (this function is going to take the letter only, which flags the intake)
    
    returns: 
        pd.DataFrame containing budget and registration count for Ottawa programs. 
    
    last update: June 13, 2024.
    """
    # Creating connection if none is provided
    if cnxn is None: 
        cnxn = python_utils.get_connection()

    # Retrieving Ottawa projections from projections file
    projections = custom_sharepoint.sharepoint_download_excel(sharepoint_base_url = sharepoint_base_url,
                                            sheet_name = 'Ottawa',
                                            file_name = 'projections.xlsx', 
                                            folder = 'budgets')

    # Keeping columns of interest and renaming them
    t = projections.isin([terms[-1]]).loc[0,].to_list()
    column = [i for i, x in enumerate(t) if x]
    df1 = projections[['School', 'Program', 'Previous Program Codes', 'Active']]
    df2 = projections.iloc[:,column[0]:column[0]+3]
    projections = pd.concat([df1, df2], axis = 1)
    projections.columns=['school', 'program', 'previous program codes', 'active', 'term',
                            'level', 'international']
    
    # Only active programs with and actual intake (non-zero budget) are going to be kept. 
    projections = projections.loc[(projections['active']=='Y')&(projections['level']==1),['school','program','international']]
    
    # Retrieving Ottawa registration numbers
    regs = python_utils.xstl_query_term_level_campus(term = terms[-1], 
                                                    campus = 'OTT',
                                                    cnxn  = cnxn)

    # Putting it all together
    df_final = projections[projections['international']!=0].merge(regs.loc[regs['AAL']=='01',['program','student_id']].groupby('program').count().reset_index(), 
                                                      on = 'program', 
                                                      how = 'left')
    df_final.columns = ['school', 'program','projection', 'student_count']
    df_final['term'] = terms[-1]
    df_final = df_final.fillna(0)
    return df_final

def registration_counts(terms:List[str], 
                        cnxn)-> pd.DataFrame:
    """
    This function provides a dataframe with desaggregated registrations counts, by registration type (F, P, O, T, C ), 
    student type (domestic, international) and campus (Sarnia and Ottawa). 

    Output: 
        Dataframe containing columns: location, program, current_load, imm_status, student_id

    Example Usage: 
        registration_counts(terms = ['2023F','2024F'], 
                            cnxn = cnxn )
                            
    """
    # Compiling overall registrations. 
    total_registrations = utils_geral.xstl_query_term_level_campus(term = terms[-1], 
    																 campus = 'MAIN',
    																 cnxn  = cnxn).append(utils_geral.xstl_query_term_level_campus(term = terms[-1], 
    																 campus = 'OTT',
    																 cnxn  = cnxn))
    # Keeping PS programs only. 
    total_registrations = total_registrations[(total_registrations['acad_level']=='PS')]

    # Creating counts
    total_registrations = total_registrations[['student_id', 'imm_status', 'location',
           'program', 'current_load']].groupby(['location','program','current_load','imm_status']).count().reset_index()

    # Re-wording 
    total_registrations['current_load'] = total_registrations['current_load'].map({'F':'Full-time regs', 
                                             'T': 'Potential regs', 
                                             'P':'Part-time regs', 
                                             'O':'Full-time regs', 
                                            'C':'Coop'})
    
    total_registrations['imm_status'] = total_registrations['imm_status'].map({
        'CA':'Domestic', 
        'NA': 'Domestic', 
        'SV': 'International', 
        'PR':'Domestic', 
        'CR':'Domestic'
    })
    
    total_registrations['location'] = total_registrations['location'].map({'MAIN':'Sarnia','OTT':'Ottawa'})
    
    return total_registrations