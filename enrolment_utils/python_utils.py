import pyodbc
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os

def load_credentials(production:bool = False,
                     sharepoint:bool = False):
    """
    This function load credentials from .env file

    Args: 
        production (bool): credentials to access production (set as default False),
        sharepoint (bool): credentials to access sharepoint (set as default False),
        chatgpt (bool): credentials to use ChatGPT API (set as default False)
    
    Returns:
        dictionaries with credentials within. 

    Example usage:
       user, pass = utils.load_credentials()
        
    """
    # load_dotenv('C:/Users/c0846720/AppData/Roaming/Python/Python38/site-packages/python_utils/.env')  # take environment variables from .env.
    load_dotenv()

    if production: 
        user = os.getenv('production_user')
        password = os.getenv('production_password')
        return user, password
    
    if sharepoint: 
        user = os.getenv('sharepoint_user')
        password = os.getenv('sharepoint_password')
        return user, password
    print('[Info] Credentials imported successfully')


prod_user, prod_password = load_credentials(production = True)

def get_connection(user:str = prod_user,
                    password:str = prod_password, 
                    database:str = 'production', 
                    server:str = 'CISSQL-live01',
                    driver:str = '{SQL Server}'):
                    # driver:str = '{SQL Server Native Client 11.0}'):
    """
    This functions provides a secure connection to a database of interest within CISSQL-live01 server
    
    Args:
        user (str): username 
        password (str): password 
        database (str): Database of interest (set in production by default)
        server (str): Server to access (set in CISSQL-live01 by default)
        
    Returns
        Functioning conection to que database of interest with given credentials
    
    Example usage: 
        cnxn = get_connection(user = user,
                                    password = password, 
                                    database = 'production', 
                                    server = 'CISSQL-live01')
    
    """
    # Creating connection string 
    try: 
        cnxn_str = ("Driver="+driver+";"
                "Server="+server+";"
                "Database="+database+";"
                "UID="+str(user)+";"
                "PWD="+str(password)+";"
                "Trusted_connection=yes;")

        # Creating conection
        cnxn = pyodbc.connect(cnxn_str)
        create_at = datetime.today().strftime(format = '%Y-%m-%d at %H:%M:%S')
        print(f'[Info] Connection built successfully. Server: {server}, database: {database}. Created on {create_at}')
    
        return cnxn
    except Exception as e: 
        print(f"""[Info] Error while trying to build connection:
-----------------------------------------------------------------
{e}""")
        
def xstl_query_term_level_campus(term: str, 
                                 campus: str = 'MAIN', 
                                 cnxn = None): 
    """
    This query pulls the exact same information as a XSTL report

    Args: 
        term (str): Term of interest
        level (str): Academic level of interest (PS set as default)
        campus (str): Campus of interest (MAIN set as defaul)

    Returns: 
        query (str): Output raw query to be passed through connection

    Example usage: 
        utils.xstl_query_term_level_campus(term = '2022F',
                                            level = 'PS', 
                                            campus = 'MAIN')
    """
    if cnxn is None: 
        cnxn = get_connection()

    query = """
    DECLARE @STAT_DATE AS DATETIME = GETDATE();
	
    SELECT STC_PERSON_ID AS student_id
        ,FIRST_NAME as first_name
        ,LAST_NAME as last_name
        ,BIRTH_DATE as birth_date
        ,STC_TERM as term 
        ,GENDER as gender
        ,STC_ACAD_LEVEL as acad_level
        ,IMMIGRATION_STATUS AS imm_status
        ,CITY AS city
        ,ADDRESS.ZIP AS postal_code
        ,SCS_LOCATION AS location
        ,SUBSTRING(STC_COURSE_NAME, 6, 4) AS program
        ,STC_SECTION_NO AS AAL
        ,STTR_STUDENT_LOAD AS current_load
        ,STTR_USER1 AS tenth_day_load
        ,STC_STATUS AS curr_status
        ,BB.STC_STATUS_DATE AS status_date
    FROM STUDENT_ACAD_CRED AA
    JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
        AND POS = (
            SELECT TOP 1 POS
            FROM STC_STATUSES
            WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
                AND STC_STATUS_DATE <= @STAT_DATE
            )
    JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
    JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
    JOIN PERSON P ON STC_PERSON_ID = P.ID
    JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
    WHERE STC_TERM = '"""+term+"""'
        AND STC_SUBJECT = 'CTRL'
        AND SCS_LOCATION = '"""+campus+"""'
        AND STC_STATUS IN ('A','D','N')
    ORDER BY STC_PERSON_ID
        ,STC_COURSE_NAME
    """

    return pd.read_sql(query, cnxn)