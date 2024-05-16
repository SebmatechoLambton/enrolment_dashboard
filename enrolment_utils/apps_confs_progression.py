import pandas as pd
from typing import List
import pyodbc
# import python_utils
from tqdm import tqdm
from io import StringIO
from enrolment_utils import probs_target_utils, apps_confs_progression, custom_sharepoint, global_params, python_utils

def retrieving_apps_program_per_term_per_date(program:str, 
                      term:str,
                      date:str, 
                      cnxn: pyodbc.Connection = None):
    """Query to extract relevant data about applications.

    Args:
        term (string):term to be used when retreiving information
        number (string): Number of years to go back so numbers can be as of this day in previous years
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns:
		dataframe with data of interest
        
    Example Usage:
    	retrieving_apps_program_per_term_per_date(term = '2023F', 
													date = '2023-01-03', 
													program = 'CDAS', 
													cnxn = cnxn )
                                                    
	last update: oct 3, 2023.
    """
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    query = """
    DECLARE @CURRENT_DATE AS DATETIME = '"""+date+"""'

SELECT     @CURRENT_DATE as ds, 
        COUNT(APPL_APPLICANT) as y
        --APPL_ACAD_PROGRAM AS program
--,APPL_APPLICANT as applicant_id

	--,REPLACE(REPLACE(REPLACE((
					--CAST((
					--		SELECT stat.APPL_STATUS AS X
					--		FROM APPL_STATUSES AS stat
					--		WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
					--			AND stat.APPL_STATUS IS NOT NULL
					--		FOR XML PATH('')
					--		) AS VARCHAR(2048))
					--), '</X><X>', ' '), '<X>', ''), '</X>', '') AS previous_statuses
	--,APPL_STATUS AS current_status
	--,APPL_PRIORITY AS level
    --,APPL_STATUS_DATE as date
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
			AND APPL_STATUS_DATE <= @CURRENT_DATE
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
AND APPL_ACAD_PROGRAM = '"""+program+"""'
        
    """
    query = pd.read_sql(query, cnxn)
    query['term'] = term
    query = query[['ds','y','term']]
    return query



def building_program_record_apps(program:str, 
                                term:str, 
                                start_year:int, 
                                end_year:int, 
                                cnxn: pyodbc.Connection = None): 
    """
    This function creates the history of applications to a given program within specific timeframe 
    
    Args: 
        program (str): Program of interest
        start_year (int): start year to start tracking historical data
        end_year (int): start year to start tracking historical data
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with day by day number of registrations
        
    Example Usage: 
        building_program_record_apps(program = 'ACTG', 
                                term = '2023F',
                                start_year = 2019, 
                                end_year = 2023)
    """
    # Creating connection if none is provided
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    
    # Creating dates dictionary 
    dates_dict = probs_target_utils.creating_dates_per_term(start_year = start_year, 
                                              end_year = end_year, 
                                              term = term)
    
    # Creating empty dataframe 
    dataframe = pd.DataFrame()
    
    # Retrieving daily number of registrations for the timeframe given
    for i, term in enumerate(dates_dict.keys()):
        
        # print what's going on
        print(f'Working on historical applications: {term} data for {program} ({i+1}/{len(dates_dict.keys())})')
        
        # Extracting individual dates
        dates = probs_target_utils.getting_individual_dates(start_date = dates_dict[term]['start_date'],
                                         					end_date = dates_dict[term]['end_date'])
        
        # For each date, pull numbers and append them to main dataframe
        for date in tqdm(dates): 
            df_aux = apps_confs_progression.retrieving_apps_program_per_term_per_date(program = program, 
                                                              term = term, 
                                                              date = date, 
                                                              cnxn = cnxn)
            dataframe = dataframe.append(df_aux)
    
    # Setting column names
    dataframe.columns = ['ds','y', 'term']
            
    return dataframe;

def retrieving_confs_program_per_term_per_date(program:str, 
                                                term:str,
                                                date:str, 
                                                cnxn: pyodbc.Connection = None):
    """Query to extract relevant data about confirmations.

    Args:
        term (string):term to be used when retreiving information
        number (string): Number of years to go back so numbers can be as of this day in previous years
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns:
		dataframe with data of interest
        
    Example Usage:
    	retrieving_confs_program_per_term_per_date(term = '2023F', 
													date = '2023-01-03', 
													program = 'CDAS', 
													cnxn = cnxn )
    
    """
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    query = """
    DECLARE @CURRENT_DATE AS DATETIME = '"""+date+"""'

SELECT     @CURRENT_DATE as date, 
        APPL_APPLICANT as applications
        ,APPL_START_TERM as term
        --APPL_ACAD_PROGRAM AS program
--,APPL_APPLICANT as applicant_id

	,REPLACE(REPLACE(REPLACE((
					CAST((
							SELECT stat.APPL_STATUS AS X
							FROM APPL_STATUSES AS stat
							WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
								AND stat.APPL_STATUS IS NOT NULL
							FOR XML PATH('')
							) AS VARCHAR(2048))
					), '</X><X>', ' '), '<X>', ''), '</X>', '') AS previous_statuses
	--,APPL_STATUS AS current_status
	--,APPL_PRIORITY AS level
    --,APPL_STATUS_DATE as date
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
			AND APPL_STATUS_DATE <= @CURRENT_DATE
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
AND APPL_ACAD_PROGRAM = '"""+program+"""'
        
    """
    dataframe = pd.read_sql(query, cnxn)
    # dataframe['term'] = term
    
    # Confirmations are defined as statuses CCC, CUC and MTS!
    y = dataframe[dataframe['previous_statuses'].str.contains(r'\b(CCC|CUC|MTS)\b')].shape[0]
    dataframe = pd.DataFrame({'ds': date,
                          'y': y, 
                          'term': term}, index = [0])
    return dataframe



def building_program_record_confs(program:str, 
                                term:str, 
                                start_year:int, 
                                end_year:int, 
                                cnxn: pyodbc.Connection = None): 
    """
    This function creates the history of confirmations to a given program within specific timeframe 
    
    Args: 
        program (str): Program of interest
        start_year (int): start year to start tracking historical data
        end_year (int): start year to start tracking historical data
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with day by day number of registrations
        
    Example Usage: 
        building_program_record_apps(program = 'ACTG', 
                                term = '2023F',
                                start_year = 2019, 
                                end_year = 2023)
    """
    # Creating connection if none is provided
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    
    # Creating dates dictionary 
    dates_dict = probs_target_utils.creating_dates_per_term(start_year = start_year, 
                                              				end_year = end_year, 
                                              				term = term)
    
    # Creating empty dataframe 
    dataframe = pd.DataFrame()
    
    # Retrieving daily number of registrations for the timeframe given
    for i, term in enumerate(dates_dict.keys()):
        
        # print what's going on
        print(f'Working on historical confirmations: {term} data for {program} ({i+1}/{len(dates_dict.keys())})')
        
        # Extracting individual dates
        dates = probs_target_utils.getting_individual_dates(start_date = dates_dict[term]['start_date'],
                                         end_date = dates_dict[term]['end_date'])
        
        # For each date, pull numbers and append them to main dataframe
        for date in tqdm(dates): 
            df_aux = apps_confs_progression.retrieving_confs_program_per_term_per_date(program = program, 
																						term = term, 
																						date = date, 
																						cnxn = cnxn)
            dataframe = dataframe.append(df_aux)
    
    # Setting column names
    dataframe.columns = ['ds','y', 'term']
            
    return dataframe;


def retrieving_regs_program_per_term_per_date(program:str, 
                                         term:str,
                                         date:str, 
                                         cnxn: pyodbc.Connection = None):
    """
    This functions gets the number of registered students on the program of interest, for the term of interest
    on the date of interest. 
    
    Args: 
        program (str): Program of interest, 
        term (str): Term of enrollment cycle of interest,
        date (str): Date to take data as of, of interest, 
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with number of registrations as of date for program and enrolment cycle of interest
        
    Example Usage: 
        retrieving_program_per_term_per_date(program = 'ACTG', 
                                             term = '2022F', 
                                            date = '2022-12-02')
    
    """
    # If connection is not given, create one
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    
    # Query to run
   
    # DECLARE @CURRENT_DATE AS DATETIME = '"""+date+"""'
    #     SELECT 
    #         @CURRENT_DATE as enroll_date,
    #         COUNT(STC_PERSON_ID) AS student_count
    #     FROM STUDENT_ACAD_CRED AA
    #     JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
    #         AND POS = (
    #             SELECT TOP 1 POS
    #             FROM STC_STATUSES
    #             WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
    #                 AND STC_STATUS_DATE <= @CURRENT_DATE
    #             )
    #     WHERE STC_TERM = '"""+term+"""'
    #         AND SUBSTRING(STC_COURSE_NAME, 6, 4)='"""+program+"""'
    #         AND STC_SECTION_NO = '01'
    #         AND STC_SUBJECT = 'CTRL'
    #     GROUP BY SUBSTRING(STC_COURSE_NAME, 6, 4);

    
    query = """
    
    DECLARE @CURRENT_DATE AS DATETIME = '"""+date+"""'
SELECT 
    @CURRENT_DATE as ds,
    COUNT(STC_PERSON_ID) AS y
FROM STUDENT_ACAD_CRED AA
JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
    AND POS = (
        SELECT TOP 1 POS
        FROM STC_STATUSES
        WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
            AND STC_STATUS_DATE <= @CURRENT_DATE
    )
WHERE STC_TERM = '"""+term+"""'
    AND SUBSTRING(STC_COURSE_NAME, 6, 4)='"""+program+"""'
    AND STC_SECTION_NO = CASE 
                            WHEN SUBSTRING(STC_COURSE_NAME, 6, 4) = 'FIRE' THEN '04'
                            WHEN SUBSTRING(STC_COURSE_NAME, 6, 4) = 'TREX' THEN '03'
                            ELSE '01'
                         END
    AND STC_SUBJECT = 'CTRL'
GROUP BY SUBSTRING(STC_COURSE_NAME, 6, 4);
        
    """
    
    # Running query and setting datatypes 
    dataframe = pd.read_sql(query, cnxn)
    
    # If there is no record, fill it with 0
    if dataframe.shape[0] == 0: 
        dataframe = pd.DataFrame({'ds': date, 'y': 0}, index = [0])
    
    # Fixing date format 
    dataframe['ds'] = pd.to_datetime(dataframe['ds'], format = '%Y-%m-%d')
    
    return dataframe;

def compiling_historical_college(terms: List[str], 
                                 folder: str):
    """
    This function compiles all information based on historical daily reports. 
    Aimed to be used for apps and confs. 
    
    Args: 
        - terms (List[str]): List of terms of interest
        - folder (str): folder name of interest (applications or confirmations)
    
    Returns: 
        - compiled pd.DataFrame with data coming from folder of interest
        
    Example Usage: 
		compiling_historical_college(terms = ['2018F','2019F','2020F','2021F','2022F','2024F'],
        							folder ='confirmations')
    
    last update: Oct 5, 2023
    """
    # sharepoing folder contains individual lists
    files, folder = custom_sharepoint.list_files_in_sharepoint(term = terms[-1], 
                                                               folder = folder)

    dataframe = pd.DataFrame()
    for program_file in files:

        # Get the file from SharePoint
        byte_content = folder.get_file(program_file)

        # Convert byte string to a regular string
        content_str = byte_content.decode('utf-8')

        # Convert string to a pandas DataFrame
        df_aux = pd.read_csv(StringIO(content_str), delimiter='\t')

        # Adding program name
        df_aux['program'] = program_file.split('.')[0]

        # All programs together
        dataframe = dataframe.append(df_aux)
    
    testing_dict = global_params.naming_files()
    file_name = [data['file_name_order'] for data in testing_dict.values() if data['terms'] == terms][0]
    program_school_dict = global_params.program_school_dict(file_name = file_name)
    dataframe['lambton_school'] = dataframe['program'].map(program_school_dict)
    
    return dataframe



def building_program_record(program:str, 
                            term:str, 
                            start_year:int, 
                            end_year:int, 
                            cnxn: pyodbc.Connection = None): 
    """
    This function creates the history of registrations to a given program within specific timeframe 
    
    Args: 
        program (str): Program of interest
        term (str): term of interest
        start_year (int): start year to start tracking historical data
        end_year (int): start year to start tracking historical data
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with day by day number of registrations
        
    Example Usage: 
        building_program_record(program = 'ACTG', 
        						term = '2023F',
                                start_year = 2019, 
                                end_year = 2023)
    """
    # Creating connection if none is provided
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    
    # Creating dates dictionary 
    dates_dict = probs_target_utils.creating_dates_per_term(start_year = start_year, 
                                              end_year = end_year, 
                                              term = term)
    
    # Creating empty dataframe 
    dataframe = pd.DataFrame()
    
    # Retrieving daily number of registrations for the timeframe given
    for i, term in enumerate(dates_dict.keys()):
        
        # print what's going on
        print(f'Working on historical registrations: {term} data for {program} ({i+1}/{len(dates_dict.keys())})')
        
        # Extracting individual dates
        dates = probs_target_utils.getting_individual_dates(start_date = dates_dict[term]['start_date'],
                                         end_date = dates_dict[term]['end_date'])
        
        # For each date, pull numbers and append them to main dataframe
        for date in tqdm(dates): 
            df_aux = retrieving_regs_program_per_term_per_date(program = program, 
                                                              term = term, 
                                                              date = date, 
                                                              cnxn = cnxn)
            dataframe = dataframe.append(df_aux)
    
    # Setting column names
    dataframe.columns = ['ds','y']
            
    return dataframe;
