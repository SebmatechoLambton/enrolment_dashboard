from enrolment_utils import custom_sharepoint, apps_confs_progression, global_params, python_utils
from prophet import Prophet
# import python_utils
import pandas as pd
import pyodbc
from tqdm import tqdm
from scipy.stats import norm
from io import StringIO

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File

from datetime import datetime, timedelta

from shareplum import Office365
from shareplum import Site
from shareplum.site import Version
import logging 
import cmdstanpy
# cmdstanpy.install_cmdstan()
cmdstanpy.install_cmdstan(compiler=True) # only valid on Windows

# Set the logging level to WARNING to suppress informational messages
logging.getLogger('cmdstanpy').setLevel(logging.WARNING)

def getting_individual_dates(start_date: str, 
                             end_date: str):
    """
    This function creates the dates within a dates range given in %yyyy-%mm-%dd format 
    
    Args: 
        start_date (str): Start date in %yyyy-%mm-%dd format 
        end_date (str): End date in %yyyy-%mm-%dd format 
        
    Returns: 
        list of dates between start and end dates
    
    Example Usage: 
        getting_individual_dates(start_date = '2020-01-01', 
                         end_date = '2021-01-01')
    """
    
    # Setting input dates
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')

    # Creating and arranging dates
    date_list = [start_date_obj + timedelta(days=x) for x in range(0, (end_date_obj - start_date_obj).days + 1)]
    date_list = [date.strftime('%Y-%m-%d') for date in date_list]

    return date_list;


def creating_dates_per_term(start_year:int, end_year:int, term:str) -> dict:
    """
    This function would build dictionaries containing dates for enrolment cycles. 
    For past years, it would provide the end and start dates, for current cycle, will get dates as of today.
    
    args: 
        start_year (int): Start year
        end_year (int): End year
        term (str): current enrolment term
    
    Returns: 
        dictionary containing start and end dates for enrolment cycles
        
    Example Usage: 
        creating_dates_per_term(start_year = 2020, 
                                end_year = 2023, 
                                term = '2023F')
    """
    # Getting current date and year
    current_year = datetime.now().year
    current_date = datetime.now().date().isoformat()
    
    
    # end_date_map = {
    #     'F': "-09-20",
    #     'W': "-01-06",
    #     'S': "-05-06"
    # }
    
    # start_date_map = {
    #     'F': "-09-21",
    #     'W': "-01-05",
    #     'S': "-05-05"
    # }

    # Creating dictionary through dict comprehension
    end_date_map, start_date_map = global_params.start_end_term_dates()
    dates_dict = {
        f"{year+1}{term[-1]}": {
            "start_date": f"{year}{start_date_map[term[-1]]}",
            "end_date": f"{year+1}{end_date_map[term[-1]]}"
        }
        for year in range(start_year, end_year)
    }

    # Adjust the 'end_date' for the term year
    if int(term[:-1]) == current_year + 1:
        dates_dict[term]['end_date'] = current_date

    # Ensure the 'end_date' for the year before the term year matches the start of the term year
    if int(term[:-1]) - 1 in [year for year in range(start_year, end_year)]:
        prev_term = str(int(term[:-1]) - 1) + term[-1]
        dates_dict[prev_term]['end_date'] = dates_dict[term]['start_date']

    return dates_dict;



def final_enrolment_day_be_term(term: str): 
    """
    This function computes the number of days before hitting day 10th of enrolment cycle
    
    Args: 
        term (str): Term of interest
    
    Returns: 
        Integer number of days until day 10th of enrolment cycle
    """
    # Tipically, day 10th will be on the 20th of the starting month 
    if term[-1]=='F':
        end_date = datetime(int(term[:4]), 9, 20)
    elif term[-1]=='W': 
        end_date = datetime(int(term[:4]), 1, 20)
    elif term[-1]=='S': 
        end_date = datetime(int(term[:4]), 5, 20)
    
    # All reporting should be made based on the current day the code is run. 
    periods = (end_date - datetime.now()).days
    
    # In case number is not positive, correct that so it would not crash 
    if periods<= 0:
        periods = 1
        
    return periods
    

def getting_target_probabilities_program(program:str, 
                                         start_year:int, 
                                         end_year:int,
                                         term: str, 
                                         file_name_budget: str,
                                         cnxn: pyodbc.Connection = None):
    """
    This function creates a forecasted value for target value and a probability to reach target for a given program
    
    Args: 
        program (str): Program of interest to retrieve data.
        start_year (int): start year to start tracking historical data
        end_year (int): start year to start tracking historical data
        term (str): Term of interest
        file_name_budget (str): file containing budget numbers of interest
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with program and probability of reaching target value
    
    Example Usage: 
        getting_target_probabilities_program(program = 'BGEN',
                                    start_year = 2018, 
                                    end_year = 2024,
                                    term = '2024F',
                                    file_name_budget = 'budget_fall.txt')
    

    """

    # Importing budget data (sharepoint)
    target  = custom_sharepoint.sharepoint_download(sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/',
                                                 local_folder = 'budgets', 
                                                 file_name = file_name_budget, 
                                                 col2_name = 'target')
    
    # If no connection is provided, create one
    if cnxn is None: 
        cnxn = python_utils.get_connection()

    # Getting programs historical data
    dataframe = program_full_data(program = program, 
                                  start_year = start_year, 
                                  end_year = end_year,
                                  term = term,
                                  cnxn = cnxn,
                                  folder_name = 'registrations')
    # apps 
    program_full_data(program = program, 
                                  start_year = start_year, 
                                  end_year = end_year,
                                  term = term,
                                  cnxn = cnxn,
                                  folder_name= 'applications')
    
	# confs
    program_full_data(program = program, 
                                  start_year = start_year, 
                                  end_year = end_year,
                                  term = term,
                                  cnxn = cnxn,
                                  folder_name= 'confirmations')
    # Creating and fitting model 
    
    model = Prophet() # Thanks facebook! 
    model.fit(dataframe)

    # Counting number of days to reach end of cycle + day 10th (more or less 14 days more )
    periods = final_enrolment_day_be_term(term = term) + 15

	# Forecasting to the data up to end of enrolment cycle (september 20)
    future = model.make_future_dataframe(periods = periods)
    forecast = model.predict(future)

	# Keeping forecasted values
    today = datetime.now().date()
    if periods < 30: 
        two_months_ago = today - timedelta(days=60)
        mask = (forecast['ds'] > two_months_ago.strftime(format = '%Y-%m-%d'))
    else:
        mask = (forecast['ds'] > today.strftime(format = '%Y-%m-%d'))

	# Importing target values from target file
    target_value = int(float(target.loc[target['Program']==program, 'target'].values[0]))

	# If current registrations is greater than target value, prob of reaching target is trivially 1
    if dataframe.iloc[-1]['y'] >= target_value: 
        prob = 1
        
    else:
	    # Creating probability based on distribution of forecasted values (assumption: normality)
        # prob =0.5 
        prob = 1-norm.cdf(target_value, loc=forecast.loc[mask, 'yhat'].mean(), scale=forecast.loc[mask, 'yhat'].std())
    
	    # Projected number of registrations
        # projected_regs = dataframe.iloc[-1]['y']

    # forecast
    baseline = int((int(forecast.loc[mask, 'yhat'].max())+int(forecast.loc[mask, 'yhat'].min()))/2)
    # baseline = int(forecast.loc[mask, 'yhat'].max())
    if baseline < dataframe.iloc[-1]['y']: 
        projected_regs =  dataframe.iloc[-1]['y']
    else: 
        projected_regs =  baseline        


	# Building dataframe
    dataframe = pd.DataFrame({'program': program, 
                              'probability': prob, 
                              'projected_regs': projected_regs }, 
                              index = [0])
    
    dataframe = dataframe.fillna(0)
		
    return dataframe;

def target_probability_report(term: str, 
                              file_name_budget: str, 
                              file_name: str,
                              start_year:int = 2019,
                              end_year:int = 2023, 
                              cnxn: pyodbc.Connection = None):
    """
    This function gets the final version of hitting the target probability report for all programs on current reporting list
    
    Args: 
        start_year (int): start year to start tracking historical data (set on 2019 by default)
        end_year (int): start year to start tracking historical data (set on 2023 by default)
        file_name (str): order file for enrolment cycle of interest
        term (str): Enrollment term of interest 
        file_name_budget (str): name of file containing budget numbers of interest 
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        dataframe (pd.DataFrame) with all programs and probabilities of reaching target
    
    Example usage: 
        target_probability_report(term = '2023F')
    """
    # If no connection is provided, create one
    if cnxn is None: 
        cnxn = python_utils.get_connection()
    
    
    # Importing current reporting data list (sharepoint)
    order = custom_sharepoint.sharepoint_download(sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/',
                                    local_folder = 'orders',
									file_name = file_name)
    # Creating empty dataframe
    dataframe = pd.DataFrame()
    
    # Iterating over programs in current reporting program list
    for i, row in order.iterrows(): 
        dataframe = dataframe.append(getting_target_probabilities_program(program = row['Program'],
                                                                          start_year = start_year, 
                                                                          end_year = end_year,
                                                                          file_name_budget = file_name_budget,
                                                                          term = term,                                                                          
                                                                          cnxn = cnxn))
        
    return dataframe;


def program_full_data(program:str, 
                      start_year:int, 
                      end_year:int, 
                      term: str,
                      cnxn: pyodbc.Connection = None, 
                      folder_name = 'registrations'):
    """
    This function gets full historical data for a given program. If there is no program file with data, creates it. 
    It there is program file, it updates it with missing dates. 
    
    Args: 
        program (str): Program of interest to retrieve data.
        start_year (int): start year to start tracking historical data
        end_year (int): start year to start tracking historical data
        term (str): Term of interest 
        cnxn (pyodbc.Connection): Connection to retrieve data from (set as None by default)
        
    Returns: 
        Dataframe (pd.DataFrame) with dates and number of registrations. 
    
    Example Usage:     
        program_full_data(program = 'ACTG', term = '2023F)
    """
    
    # Creating connection if none is provided
    if cnxn is None: 
        cnxn = python_utils.get_connection()
        
    files, folder = custom_sharepoint.list_files_in_sharepoint(term = term, 
                                                               folder = folder_name)

    program_file = program + '.txt'
    if program_file in files: 
        # Get the file from SharePoint
        byte_content = folder.get_file(program_file)

        # Convert byte string to a regular string
        content_str = byte_content.decode('utf-8')

        # Convert string to a pandas DataFrame
        dataframe = pd.read_csv(StringIO(content_str), delimiter='\t')

        # Collecting missing dates
        missing_dates = getting_individual_dates(start_date = dataframe['ds'].dropna().iloc[-1], 
                                                end_date = datetime.today().date().strftime(format = '%Y-%m-%d'))
	


        # Retrieving data from missing dates
        df_update = pd.DataFrame()
        for date in missing_dates:
            # Making sure only missing dates are added
            if date not in [x for x in dataframe['ds']]:
                if folder_name == 'registrations':
                    df_update = df_update.append(apps_confs_progression.retrieving_regs_program_per_term_per_date(program = program, 
                                                                                          term = term, 
                                                                                          date = date, 
                                                                                          cnxn = cnxn))
                if folder_name == 'applications':
                    df_update = df_update.append(apps_confs_progression.retrieving_apps_program_per_term_per_date(program = program, 
                                                                                          term = term, 
                                                                                          date = date, 
                                                                                          cnxn = cnxn))
                if folder_name == 'confirmations':
                    df_update = df_update.append(apps_confs_progression.retrieving_confs_program_per_term_per_date(program = program, 
                                                                                          term = term, 
                                                                                          date = date, 
                                                                                          cnxn = cnxn))
                    
        # putting it all together
        dataframe = dataframe.append(df_update)

        # Fixing date format 
        dataframe['ds'] = pd.to_datetime(dataframe['ds'], format = '%Y-%m-%d')

        # Convert DataFrame to CSV in-memory and then upload
        output = StringIO()
        dataframe.to_csv(output, sep = '\t', index=False)
        csv_content = output.getvalue().encode('utf-8') # Convert to bytes-like object

        # Upload/update the document
        folder.upload_file(csv_content, program_file)

    if program_file not in files: 
        # Building program information
        if folder_name == 'registrations':
            df_final = apps_confs_progression.building_program_record(program = program,
                                            start_year = start_year, 
                                            end_year = end_year,
                                            term = term,
                                            cnxn = cnxn)
        if folder_name == 'applications':
            df_final = apps_confs_progression.building_program_record_apps(program = program,
                                            start_year = start_year, 
                                            end_year = end_year,
                                            term = term,
                                            cnxn = cnxn)
        if folder_name == 'confirmations':
            df_final = apps_confs_progression.building_program_record_confs(program = program,
                                            start_year = start_year, 
                                            end_year = end_year,
                                            term = term,
                                            cnxn = cnxn)
            
        # Convert DataFrame to CSV in-memory and then upload
        output = StringIO()
        df_final.to_csv(output, sep = '\t', index=False)
        csv_content = output.getvalue().encode('utf-8') # Convert to bytes-like object

        # Upload/update the document
        folder.upload_file(csv_content, program_file)

    # Get the file from SharePoint
    byte_content = folder.get_file(program_file)

    # Convert byte string to a regular string
    content_str = byte_content.decode('utf-8')

    # Convert string to a pandas DataFrame
    dataframe = pd.read_csv(StringIO(content_str), delimiter='\t')

    return dataframe