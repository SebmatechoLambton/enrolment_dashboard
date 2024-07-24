
import pandas as pd 
import numpy as np
import datetime as dt
from typing import List
import pyodbc
from enrolment_utils import queries_historical, custom_sharepoint

def AppsEndofCycle(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This function takes the order dataframe and adds the number of applications for each program in the terms specified.
    The function uses the queries_historical.AppsEndofCycleQuery function to get the data from the database.
    The function returns the order dataframe with the new columns added.

    """
    for term in terms: 
        k = int(terms[-1][0:4]) - int(term[0:4])
        dataset = queries_historical.AppsEndofCycleQuery(term, cnxn)
        dataset['Level'] = dataset['Level'].fillna(1) # assumption. If level is missing, assume as AAL01
        dataset['Level'] = dataset['Level'].astype(int)
        dataset = dataset[dataset['status']!='DLT'] # Removing deleted applications
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                      (dataset['Level'] == 1),'new','returning')
        dataset = dataset[dataset['Student']=='new']
        dataset = dataset[['Applicant_ID','Program']].groupby('Program').count().reset_index().rename(columns= {'Program':'Program','Applicant_ID':'Applications '+term })
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order


def OffersEndofCycle(order:pd.DataFrame, 
                     terms:List[str], 
                     cnxn:pyodbc.connect)->pd.DataFrame:
    """
    Retrieves and manipulates historical enrollment data to calculate the number of offers made for each program at the end of a cycle.

    Parameters:
    order (pd.DataFrame): The input DataFrame containing program information.
    terms (List[str]): A list of terms for which the offers need to be calculated.
    cnxn (pyodbc.connect): The database connection object.

    Returns:
    pd.DataFrame: The modified DataFrame with the number of offers made for each program.

    """
    for term in terms: 
        dataset = queries_historical.OffersEndofCycleQuery(term, cnxn)
        # 'dataset' holds the input data for this script
        dataset['Level'] = dataset['Level'].fillna(1) # assumption. If level is missing, assume as AAL01
        dataset['Level'] = dataset['Level'].astype(int)
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        dataset = dataset[dataset['Student']=='new']
        if dataset['APPL_START_TERM'].unique()[0] is not ['2022F','2023F']:
            cond2 = (dataset['PreviousStatuses'].str.contains('WAC|WCF|MTS|MVD|WMS|ACC|ACU|HMS|WTN|AOF'))
            cond3 = (dataset['Curr_Status']=='DNA')&(dataset['PreviousStatuses'].str.contains('ACC|ACU'))
            dataset = dataset[cond2|cond3]
        elif dataset['APPL_START_TERM'].unique()[0]=='2022F':
            cond1 = dataset['Curr_Status'].isin(['WAC','WCF','MTS','MVD','WMS','ACC','ACU','HMS','WTN','DNO','AOF'])
            dataset = dataset[cond1]
        elif dataset['APPL_START_TERM'].unique()[0]=='2023F':
            cond0 = dataset['Curr_Status'].isin(['WAC','WCF','MTS','MVD','WMS','ACC','ACU','HMS','WTN','DNO','AOF','DLT'])
            dataset = dataset[cond0]
        dataset = dataset[['Applicant_ID','Program']].groupby('Program').count().reset_index().rename(columns= {'Program':'Program','Applicant_ID':'Offers '+term})
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order


def ConfirmationsEndofCycle(order: pd.DataFrame, 
                            terms: List[str], 
                            cnxn: pyodbc.connect) -> pd.DataFrame:
    """
    Retrieves confirmation data for the specified terms and updates the 'order' DataFrame with the results.

    Args:
        order (pd.DataFrame): The DataFrame containing the order data.
        terms (List[str]): A list of terms for which confirmation data needs to be retrieved.
        cnxn (pyodbc.connect): The pyodbc connection object used to connect to the database.

    Returns:
        pd.DataFrame: The updated 'order' DataFrame with confirmation data.

    """
    for term in terms: 
        dataset = queries_historical.ConfirmationsEndofCycleQuery(term, cnxn)
        dataset['Level'] = dataset['Level'].fillna(1) # assumption. If level is missing, assume as AAL01
        dataset['Level'] = dataset['Level'].astype(int)
        dataset['Choice'] = dataset['Choice'].fillna(1) # assumption. If level is missing, assume as AAL01
        dataset['Choice'] = dataset['Choice'].astype(int)
        dataset = dataset[(dataset['Curr_Status'].isin(['CCC','CUC','MTS','MVD','HMS','WCF','WMS','WTN']))&(dataset['Level'] == 1) &(dataset['Choice']!=6)]
        dataset = dataset.value_counts('Program').reset_index().rename(columns={1: "Program", 0: "Confirmations "+term })
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order



def RegistrationsEndofCycle(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    Retrieves registration data for the specified terms and performs data manipulation.

    Parameters:
    order (pd.DataFrame): The input DataFrame representing the order of programs.
    terms (List[str]): A list of terms for which registration data needs to be retrieved.
    cnxn (pyodbc.connect): The connection object for the database.

    Returns:
    pd.DataFrame: The modified order DataFrame with additional columns representing the count of registrations for each program in the specified terms.

    """

    for term in terms:
        dataset = queries_historical.RegistrationsEndofCycleQuery(term, cnxn)
        # dataset['AAL'] = dataset['AAL'].astype(int)
        dataset = dataset[(dataset['10th Load'].isin(['F','O']))&(dataset['Imm. Status']!='SV') & (~dataset['Curr_Status'].isin(['X','C']))]
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03'))| (dataset['AAL'] == '01'),'new','returning')
        dataset = dataset[(dataset['student'] == 'new')]
        dataset = dataset.value_counts('Program').to_frame().reset_index()
        dataset = dataset.rename(columns={0: 'Count '+term})
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order


def RegistrationHistorical(order,
                           sharepoint_user,
                           sharepoint_password,
                           sharepoint_base_url,
                           terms = ['2020F','2021F','2022F','2022']):
    final_df = pd.DataFrame()
    for term in terms:
        dataset = custom_sharepoint.sharepoint_download_excel(sharepoint_user = sharepoint_user,
                                                              sharepoint_password = sharepoint_password,
                                                              sharepoint_base_url = sharepoint_base_url,
                                                              name = term)
        dataset['AAL'] = dataset['AAL'].fillna(1).astype(int).astype(str)
        dataset['Program'] = dataset['Program'].str[0:4]
        if '10th Day Load' in dataset.columns:  
            dataset = dataset[dataset['10th Day Load'].isin(['F','O'])]
        else:     
            dataset = dataset[dataset['Student Load'].isin(['F','O'])]
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '4')) | ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '3'))| (dataset['AAL'] == '1'),'new','returning')
        dataset = dataset[(dataset['student'] == 'new')]
        domestic = dataset[dataset['Student Visa'] !='SV']
        domestic = domestic[['Student ID','Program']].groupby('Program').count().reset_index().rename(columns = {'Student ID':'Count'})
        domestic = order.merge(domestic, on = 'Program', how = 'left')
        domestic['Count'] = domestic['Count'].fillna(0)
        domestic['flag'] = 'Domestic'

        international = dataset[dataset['Student Visa'] =='SV']
        international = international[['Student ID','Program']].groupby('Program').count().reset_index().rename(columns = {'Student ID':'Count'})
        international = order.merge(international, on = 'Program', how = 'left')
        international['Count'] = international['Count'].fillna(0)
        international['flag'] = 'International'

        output = domestic.append(international)
        output = output.rename(columns = {'Count':term})
        if final_df.empty:
            final_df = output
        else:
            final_df = final_df.merge(output, on =['Program','School','flag'], how = 'left')
    return final_df