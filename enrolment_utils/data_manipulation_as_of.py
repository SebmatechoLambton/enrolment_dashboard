import pandas as pd 
import numpy as np
import datetime as dt
import pyodbc
from typing import List
from enrolment_utils import queries_as_of
# from tqdm import tqdm


def Applications(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """This function transforms the ingested data regarding applications
        - If level is missing is filled as 01 
        - AAL 04 for FIRE, AALs 01 and 03 for TREX and AAL 01 for the rest of the programs
        - new students only (level 01)
        - counting applications per program for each term in terms.
        - k is the difference of years, so numbers can be compared directly (everything is as of today on other years)

    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database

    Returns:
        _type_: Full ordered dataframe containing application numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.ApplicationsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as a integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Removing deleted applications
        dataset = dataset[dataset['status']!='DLT'] 
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                      (dataset['Level'] == 1),'new','returning')
        # Keeping new students
        dataset = dataset[dataset['Student']=='new']
        
        # Count students per program
        dataset = dataset[['Applicant_ID','Program']].groupby('Program').count().reset_index().rename(columns= {'Program':'Program','Applicant_ID':'Applications '+term })
        
        # merge it with main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order



def DeletedApplications(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """This function transforms the ingested data regarding applications
        - If level is missing is filled as 01 
        - AAL 04 for FIRE, AALs 01 and 03 for TREX and AAL 01 for the rest of the programs
        - new students only (level 01)
        - counting applications per program for each term in terms.
        - k is the difference of years, so numbers can be compared directly (everything is as of today on other years)

    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database

    Returns:
        _type_: Full ordered dataframe containing application numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.ApplicationsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # keeping deleted applications only
        dataset = dataset[dataset['status']=='DLT'] 
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                      (dataset['Level'] == 1),'new','returning')
        
        # Keeping new students
        dataset = dataset[dataset['Student']=='new']
        
        # Count students per program
        dataset = dataset[['Applicant_ID','Program']].groupby('Program').count().reset_index().rename(columns= {'Program':'Program','Applicant_ID':'Applications '+term })
        
        # merge it with main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0)
    return order

def Offers(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """This function transforms the ingested data regarding offers
        - same items as applications
        - prior to 2022F, the statuses WAC, WCF, MTS, MVD, WMS, ACC, ACU, HMS, WTN, AOF where track in previous statuses, if status is DNA is current status, ACC and ACU are required as previous status
        - in 2022F the statuses WAC, WCF, MTS, MVD, WMS, ACC, ACU, HMS, WTN, DNO, AOF were used
        - in 2023F the statuses WAC, WCF, MTS, MVD, WMS, ACC, ACU, HMS, WTN, DNO, AOF, DLT were used


    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database

    Returns:
        _type_: Full ordered dataframe containing offers numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
         # Running query
        dataset = queries_as_of.OffersQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Keeping new students only
        dataset = dataset[dataset['Student']=='new']
        
        # For terms other than 2022F, 2023F and 2024F, check for list of current statuses or if current status is DNA, look for previous statuses
        if dataset['APPL_START_TERM'].unique()[0] is not ['2022F','2023F','2024F']:
            cond2 = (dataset['Curr_Status'].str.contains('WAC|WCF|MTS|MVD|WMS|ACC|ACU|HMS|WTN|AOF'))
            cond3 = (dataset['Curr_Status']=='DNA')&(dataset['PreviousStatuses'].str.contains('ACC|ACU'))
            dataset = dataset[cond2|cond3]
            
        # For 2022F, check specific list of statuses
        elif dataset['APPL_START_TERM'].unique()[0]=='2022F':
            cond1 = dataset['Curr_Status'].isin(['WAC','WCF','MTS','MVD','WMS','ACC','ACU','HMS','WTN','DNO','AOF'])
            dataset = dataset[cond1]
            
        # For 2023F, check specific list of statuses
        elif dataset['APPL_START_TERM'].unique()[0] in ['2023F','2024F']:
            cond0 = dataset['Curr_Status'].isin(['WAC','WCF','MTS','MVD','WMS','ACC','ACU','HMS','WTN','DNO','AOF','DLT'])
            dataset = dataset[cond0]
            
        # Count number of students per program 
        dataset = dataset[['Applicant_ID','Program']].groupby('Program').count().reset_index().rename(columns= {'Program':'Program','Applicant_ID':'Offers '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0) 

    return order


def TableauOutstandingOffers(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of oustanding offers per program per reporting term 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing Outstanding offers numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.TableauQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Create pivot table so statuses will be columns and values will be students
        dataset= dataset.pivot_table( index ='Program', columns = 'Curr_Status', aggfunc= len)
        dataset= dataset.reset_index()
        dataset= dataset.fillna(0)
        
        # Creating target dataframe 
        df1 = pd.DataFrame(dataset['Program'])
        df2 = dataset['Choice']
        dataset = pd.concat([df1, df2], axis = 1)
        
        # Keeping statuses of interest only and filling target dataframe
        Outstanding = list(set(list(dataset.columns)) & set(['ACC','ACU']))
        dataset['Outstanding Offers'] = dataset[Outstanding].sum(axis=1)
        dataset = dataset[['Program','Outstanding Offers']]
        dataset = dataset.rename(columns={'Program':'Program',
                                                      'Outstanding Offers':'Outstanding Offers '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    
    # There were no Outstanding Offers in 2019F
    order['Outstanding Offers 2019F']=0
    order = order.fillna(0)
    return order


def TableauConfirmations(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of confirmations per program per reporting term, according to the Tableau report (Heather would know)
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing confirmation numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.TableauQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Create pivot table so statuses will be columns and values will be students
        dataset= dataset.pivot_table( index ='Program', columns = 'Curr_Status', aggfunc= len)
        dataset= dataset.reset_index()
        dataset= dataset.fillna(0)
        
        # Creating target dataframe 
        df1 = pd.DataFrame(dataset['Program'])
        df2 = dataset['Choice']
        dataset = pd.concat([df1, df2], axis = 1)
        
        # Keeping statuses of interest only and filling target dataframe
        confirmations = list(set(list(dataset.columns)) & set(['CCC','CUC','MTS','MVD','WCF']))
        dataset['Confirmations'] = dataset[confirmations].sum(axis=1)
        dataset = dataset[['Program','Confirmations']]
        dataset = dataset.rename(columns={'Program':'Program',
                                          'Confirmations':'Confirmations '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order

def TableauHolds(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of holds per program per reporting term, according to the Tableau report (Heather would know)
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing hold numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.TableauQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1)
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Create pivot table so statuses will be columns and values will be students
        dataset= dataset.pivot_table( index ='Program', columns = 'Curr_Status', aggfunc= len)
        dataset= dataset.reset_index()
        dataset= dataset.fillna(0)
        
         # Creating target dataframe 
        df1 = pd.DataFrame(dataset['Program'])
        df2 = dataset['Choice']
        dataset = pd.concat([df1, df2], axis = 1)
        
        # Keeping statuses of interest only and filling target dataframe
        hold = list(set(list(dataset.columns)) & set(['HDE','HLCI','HLD','HLF','HLG','HLM',
                                                      'HLN','HLR','HLS','HLT','HLX','HMD',
                                                      'HME','HMM','HMS','HLS','HLB','HLA']))
        dataset['Hold'] = dataset[hold].sum(axis=1)
        dataset = dataset[['Program','Hold']]
        dataset = dataset.rename(columns={'Program':'Program',
                                          'Hold':'Hold '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order

def TableauWithdrawals(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of Withdrawals per program per reporting term, according to the Tableau report (Heather would know)
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing withdrawal numbers as of day of execution for various terms
    """
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.TableauQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Create pivot table so statuses will be columns and values will be students
        dataset= dataset.pivot_table( index ='Program', columns = 'Curr_Status', aggfunc= len)
        dataset= dataset.reset_index()
        dataset= dataset.fillna(0)
        
        # Creating target dataframe 
        df1 = pd.DataFrame(dataset['Program'])
        df2 = dataset['Choice']
        dataset = pd.concat([df1, df2], axis = 1)
        
        # Keeping statuses of interest only and filling target dataframe
        withdrawals = list(set(list(dataset.columns)) & set(['RFQ','RST','WAC','WAP','WCF','WMS']))
        dataset['Withdrawals and Refused'] = dataset[withdrawals].sum(axis=1)
        dataset = dataset[['Program','Withdrawals and Refused']]
        dataset = dataset.rename(columns={'Program':'Program',
                                          'Withdrawals and Refused':'Withdrawals and Refused '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order

def TableauWaitlist(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of waitlisted per program per reporting term, according to the Tableau report (Heather would know)
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing waitlisted numbers as of day of execution for various terms
    """
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.TableauQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1)
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
        
        # Create pivot table so statuses will be columns and values will be students
        dataset= dataset.pivot_table( index ='Program', columns = 'Curr_Status', aggfunc= len)
        dataset= dataset.reset_index()
        dataset= dataset.fillna(0)
        
        # Creating target dataframe 
        df1 = pd.DataFrame(dataset['Program'])
        df2 = dataset['Choice']
        dataset = pd.concat([df1, df2], axis = 1)
        
        # Keeping statuses of interest only and filling target dataframe
        waitlist = list(set(list(dataset.columns)) & set(['WTL','WTN']))
        dataset['Waitlisted'] = dataset[waitlist].sum(axis=1)
        dataset = dataset[['Program','Waitlisted']]
        dataset = dataset.rename(columns={'Program':'Program',
                                          'Waitlisted':'Waitlisted '+term})
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
        
    order = order.fillna(0)
    return order


def confirmations(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of confirmations per program per reporting term
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing confirmations numbers as of day of execution for various terms
    """
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.ConfirmationsQuery(term,str(k), cnxn)

        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')

        # Keeping status of interest
        dataset= dataset[dataset['Curr_Status'].isin(['CCC','CUC','MTS','MVD'])]
        
        # Count confirmations for programs
        dataset= dataset.value_counts('Program').reset_index().rename(columns={1: "Program", 0: "Confirmations "+term })
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    
    order = order.fillna(0)
    return order

def FirstApplications(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of first choice applications per program per reporting term
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full ordered dataframe containing first choice applications numbers as of day of execution for various terms
    """
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.FirstApplicationsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['Level'] = dataset['Level'].fillna(1) 
        dataset['Level'] = dataset['Level'].astype(int)
        
        # Adding new/returning flag
        dataset['Student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['Level'] == 4)) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['Level'].isin([1,3]))) | 
                                       (dataset['Level'] == 1),'new','returning')
    
        # Keeping students with 1st choice only
        dataset = dataset[dataset['Choice'] ==1][['Program','Applicant_ID']].groupby('Program').count().reset_index().rename(columns={'Program':'Program','Applicant_ID':'First Choice Applicants '+term})
        dataset = dataset.fillna(0)
        
        # merge to main order file
        order = order.merge(dataset, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order


# In this section, be mindful of correcting also the age factor when computing age in the python section  
def MapInfo(terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions summerizes data regarding the enrolment trends to be visualized within a map. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full dataframe containing data of application such as program, gender, location and age group for all term 
    """
    
    # Creating target dataframes
    dataset_final = pd.DataFrame()
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.MapInfoQuery(term,str(k), cnxn)
        
        # Dropping ducplicates. 
        dataset = dataset.drop_duplicates(['Applicant_ID','Indigenous Status','Program'])
        dataset = dataset.drop('Applicant_ID', axis =1 )
        # Creating date as of today for previous years
        date = (dt.datetime.now()- dt.timedelta(days=k*365.2425)).strftime("%m/%d/%Y")
        ref = dt.datetime.strptime(date, '%m/%d/%Y')
        
        # Transforming birth date and classifying applicants into groups. 
        dataset['BIRTH_DATE'] = pd.to_datetime( dataset['BIRTH_DATE'], format='%Y-%m-%d' )  
        dataset['BIRTH_DATE'] = dataset['BIRTH_DATE'].where(dataset['BIRTH_DATE'] < ref, dataset['BIRTH_DATE'] -  np.timedelta64(100, 'Y'))  
        dataset['age'] = (ref - dataset['BIRTH_DATE']).astype('<m8[Y]') 
        dataset['age_group'] = dataset['age'].apply(lambda x:  '<18' if x<19 else 
                                                           '18-21' if (x>18)&(x<22) else
                                                            '22-25' if (x>21)&(x<26) else
                                                            '26-30' if (x>24)&(x<31) else
                                                            '31-40' if (x>29)&(x<41) else
                                                            '41-50' if (x>39)&(x<51) else '>50' )
        
        ## Adding term 
        dataset['Term'] = term
        
        # Compiling data from all terms 
        dataset_final = pd.concat([dataset_final,dataset])
    return dataset_final


def DomesticRegs(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of domestic registrations per program per reporting term
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full dataframe containing data of domestic registrations numbers as of day of execution for various terms
    """
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.DomesticRegistrationsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Keeping new domestic full-time students
        dataset = dataset[(dataset['student'] == 'new') & 
                          (dataset['Current Load'].isin(['O','F']))&
                          (dataset['Imm. Status'] != 'SV')]
        
        # Drop duplicates
        dataset = dataset.drop_duplicates('Applicant_ID')
        
        # Counting students per program 
        dataset = dataset.value_counts('Program').to_frame().reset_index()
        dataset = dataset.rename(columns={0: 'Count '+term})
        
        # Merging to main orger file 
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0)
    return order



def InternationalRegs(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of international registrations per program per reporting term
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full dataframe containing data of international registrations numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.InternationalRegistrationsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Keeping new international full-time students
        dataset = dataset[(dataset['student'] == 'new') & (dataset['Current Load'].isin(['O','F']))&(dataset['Imm. Status'] == 'SV')]
        
        # Drop duplicates
        dataset = dataset.drop_duplicates('Applicant_ID')
        
        # Counting students per program 
        dataset = dataset.value_counts('Program').to_frame().reset_index()
        dataset = dataset.rename(columns={0: 'Count '+term})
        
        # Merging to main orger file 
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0)
    return order


def RegistrationsRates(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of registrations per program per reporting term for rates reporting purposes
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full dataframe containing data of international registrations numbers as of day of execution for various terms
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.RegistrationsRatesQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # Keeping domestic full-time students (day 10th load)
        dataset = dataset[(dataset['10th Load'].isin(['F','O']))&(dataset['Imm. Status']!='SV')]
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) | 
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Keeping new full-time students (current load)
        dataset = dataset[(dataset['student'] == 'new') & (dataset['Current Load'].isin(['O','F']))]
        
        # Counting students per program 
        dataset = dataset.value_counts('Program').to_frame().reset_index()
        dataset = dataset.rename(columns={0: 'Count '+term})
        
        # Merging to main orger file 
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0)
        
    return order


def RegistrationsBudget(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of registrations per program per reporting term for tracking budget accomplishments
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    Returns: 
        Full dataframe containing data of registrations such as program to be compared with budget. 
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # Running query
        dataset = queries_as_of.RegistrationsBudgetQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) | 
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Keeping new full-time students
        dataset = dataset[(dataset['student'] == 'new') & (dataset['Current Load'].isin(['O','F']))]
        
        # Drop duplicates
        dataset = dataset.drop_duplicates('Applicant_ID')
        
        # Counting students per program 
        dataset = dataset.value_counts('Program').to_frame().reset_index()
        dataset = dataset.rename(columns={0: 'Count '+term})
        
        # Merging to main orger file 
        order = order.merge(dataset, on = 'Program',how = 'left')
        order = order.fillna(0)
    return order



def term01deposits(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect, 
                 sv_flag = False)->pd.DataFrame:
    """
    This functions computes the number of returnings per program per reporting term for tracking budget accomplishments
    
    Returning students are defined as students who made some money compromise (at least $10 CAD), fee-waivers (tipically OCAS)
    or some sponsorships (if someone else that is not the student or OSAP is paying for his/her education) to the college
    and thus, college knows those students are extremely likely to show up when term starts, so it can count these students
    for registrations and whatever other related purposes. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
        sv_flag (bool): Flag to compute returning students for international or domestic students (set at False by default)
    
    Returns: 
        Full dataframe containing data of returning students such as program to be compared with budget. 
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # running the query 
        dataset = queries_as_of.ReturningStudentsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Checking international/domestic student flag and filtering data accordingly
        if sv_flag == True: 
            dataset = dataset[dataset['IMMIGRATION_STATUS'] =='SV']
        if sv_flag == False: 
            dataset = dataset[dataset['IMMIGRATION_STATUS'] !='SV']
        
        # Computing today's date as of other years (365.2524 acounts for bisiesto (spanish word of the day :) ) years)
        date = dt.datetime.now()-dt.timedelta(days=k*365.2524)
        date = date.strftime("%Y/%m/%d")
        
        ## Sponsopships
        # flagging students with active sponsorships as of date of reporting (as of today for previous years)
        cond = (pd.notnull(dataset['SPONSORSHIP'])) 
        dataset.loc[cond,'Pay filter']=dataset.loc[cond,'SPONSOR_APPLIED'].apply(lambda x : 1 if x.strftime("%Y/%m/%d") < date else 0)
        
        ## RO flags (tipically OSAP)
        # Transforming RO flag date
        dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'] = dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'].apply(lambda x:dt.datetime.strptime(x[0:19], "%Y-%m-%dT%H:%M:%S"))
        
        # Flagging students with RO flag in the system as of date of reporting (as of today for previous years)
        cond3 = (dataset['STNT'].str.contains("RO",na=False)) 
        dataset.loc[cond3,'Pay filter']=dataset.loc[cond3,'STNT_Date'].apply(lambda x : 1 if x.strftime("%Y/%m/%d")<= date else 0)
        
        ## Money 
        # Flagging students that would make a deposit of $10 CAD or more. 
        dataset.loc[dataset['Pay Amt']>=10,'Pay filter'] = 1
        
        ## Keeping returning students with pay filter and dropping duplicates
        aux_2 = dataset.loc[(dataset['student'] == 'returning') &(dataset['Pay filter'] == 1)]
        aux_2 = aux_2.drop_duplicates('Student ID',keep='first')
        
        # Counting paid students per term 
        table2 = aux_2[['Program','Pay filter']].value_counts('Program').to_frame().reset_index()
        table2.columns = ['Program','Returning students '+term]
        order = order.merge(table2, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order
    
    
def NewStudents(order:pd.DataFrame, 
                 terms:List[str], 
                 cnxn:pyodbc.connect)->pd.DataFrame:
    """
    This functions computes the number of new per program per reporting term for tracking budget accomplishments
    
    Returning students are defined as students who made some money compromise (at least $10 CAD), fee-waivers (tipically OCAS)
    or some sponsorships (if someone else that is not the student or OSAP is paying for his/her education) to the college
    and thus, college knows those students are extremely likely to show up when term starts, so it can count these students
    for registrations and whatever other related purposes. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
    
    Returns: 
        Full dataframe containing data of returning students such as program to be compared with budget. 
    """
    
    # Sweeping through terms
    for term in terms: 
        
        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # running the query 
        dataset = queries_as_of.ReturningStudentsQuery(term,str(k), cnxn)
        
        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # Adding new/returning flag
        dataset['student'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                      ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                      (dataset['AAL'] == '01'),'new','returning')
        
        # Computing today's date as of other years (365.2524 acounts for bisiesto (spanish word of the day :) ) years)
        date = dt.datetime.now()-dt.timedelta(days=k*365.2524)
        date = date.strftime("%Y/%m/%d")
        
        ## Sponsopships
        # flagging students with active sponsorships as of date of reporting (as of today for previous years)
        cond = (pd.notnull(dataset['SPONSORSHIP'])) 
        dataset.loc[cond,'Pay filter']=dataset.loc[cond,'SPONSOR_APPLIED'].apply(lambda x : 1 if x.strftime("%Y/%m/%d") < date else 0)
        
        ## RO flags (tipically OSAP)
        # Transforming RO flag date
        dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'] = dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'].apply(lambda x:dt.datetime.strptime(x[0:19], "%Y-%m-%dT%H:%M:%S"))
        
        # Flagging students with RO flag in the system as of date of reporting (as of today for previous years)
        cond3 = (dataset['STNT'].str.contains("RO",na=False)) 
        dataset.loc[cond3,'Pay filter']=dataset.loc[cond3,'STNT_Date'].apply(lambda x : 1 if x.strftime("%Y/%m/%d")<= date else 0)
        
        ## Money 
        # Flagging students that would make a deposit of $10 CAD or more. 
        dataset.loc[dataset['Pay Amt']>=10,'Pay filter'] = 1
        
        ## Keeping new students with pay filter and dropping duplicates
        aux_1 = dataset.loc[(dataset['student'] == 'new') & (dataset['Pay filter'] == 1)] 
        aux_1 = aux_1.drop_duplicates('Student ID',keep='first')
        
        # Counting paid students per term 
        table1 = aux_1[['Program','Pay filter']].value_counts('Program').to_frame().reset_index()
        table1.columns = ['Program','New students '+term]
        
        order = order.merge(table1, on = 'Program',how = 'left')
    order = order.fillna(0)
    return order

def deposist_process(dataset, k): 
    """
    This function adds the correspondent payment flag to students based on payments (more than $10), OCAS (RO flag) or
    sponsorship. 
    
    Args: 
        dataset (pd.DataFrame): input dataframe (output of ReturningStudentsQuery )
        k (str): number of years to go back, so past years data would be as of today 
    
    returns: 
        pd.DataFrame with program, student type (domestic/international) and student count
        
    Example usage: 
        deposist_process(dataset = dataset)
    """
    # Computing today's date as of other years (365.2524 acounts for bisiesto (spanish word of the day :) ) years)
    date = dt.datetime.now()-dt.timedelta(days=k*365.2524)
    date = date.strftime("%Y/%m/%d")
    ## Sponsopships
    # flagging students with active sponsorships as of date of reporting (as of today for previous years)
    cond = (pd.notnull(dataset['SPONSORSHIP'])) 
    dataset.loc[cond,'Pay filter']=dataset.loc[cond,'SPONSOR_APPLIED'].apply(lambda x : 1 if x.strftime("%Y/%m/%d") < date else 0)
    #
    ## RO flags (tipically OSAP)
    # Transforming RO flag date
    dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'] = dataset.loc[dataset['STNT'].str.contains("RO",na=False),'STNT_Date'].apply(lambda x:dt.datetime.strptime(x[0:19], "%Y-%m-%dT%H:%M:%S"))
    #
    # Flagging students with RO flag in the system as of date of reporting (as of today for previous years)
    cond3 = (dataset['STNT'].str.contains("RO",na=False)) 
    dataset.loc[cond3,'Pay filter']=dataset.loc[cond3,'STNT_Date'].apply(lambda x : 1 if x.strftime("%Y/%m/%d")<= date else 0)
    #
    ## Money 
    # Flagging students that would make a deposit of $10 CAD or more. 
    dataset.loc[dataset['Pay Amt']>=10,'Pay filter'] = 1
    #
    ## Keeping returning students with pay filter and dropping duplicates
    aux_2 = dataset.loc[dataset['Pay filter'] == 1]
    aux_2 = aux_2.drop_duplicates('Student ID', keep='first')
    return aux_2

def term_deposits(order: pd.DataFrame, 
                  terms: List[str], 
                  cnxn: pyodbc.connect,
                  aal01:bool = True):
    """
    This functions computes the number of returnings per program per reporting term for tracking budget accomplishments
    
    Returning students are defined as students who made some money compromise (at least $10 CAD), fee-waivers (tipically OCAS)
    or some sponsorships (if someone else that is not the student or OSAP is paying for his/her education) to the college
    and thus, college knows those students are extremely likely to show up when term starts, so it can count these students
    for registrations and whatever other related purposes. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
        aal01 (bool): Flag to compute new and returning students (set at True by default)
    
    Returns: 
        Full dataframe containing data of returning students such as program to be compared with budget. 
    """

    # Duplicate the dataframe
    order_copy = order.copy()

    # Add a flag to the original dataframe
    order['student'] = 'domestic'
    order_copy['student'] = 'international'

    # Concatenate both dataframes
    order = pd.concat([order, order_copy], ignore_index=True)


    for term in terms: 

        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        # running the query 
        dataset = queries_as_of.ReturningStudentsQuery(term,str(k), cnxn)

        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')

        # Adding new/returning flag
        dataset['new_student_flag'] = np.where(((dataset['Program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                              ((dataset['Program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                              (dataset['AAL'] == '01'),'new','returning')

        if aal01: 
            dataset = dataset[dataset['new_student_flag'] == 'new']
        else: 
            dataset = dataset[dataset['new_student_flag'] == 'returning']
            
        dataset_international = dataset[dataset['IMMIGRATION_STATUS']=='SV']
        dataset_international = deposist_process(dataset = dataset_international, k = k)
        # Counting paid students per term
        dataset_international = dataset_international[['Program','Pay filter']].value_counts('Program').to_frame().reset_index()
        dataset_international.columns = ['Program','Returning students '+term]
        dataset_international['student'] = 'international'

        dataset_domestic = dataset[dataset['IMMIGRATION_STATUS']!='SV']
        dataset_domestic = deposist_process(dataset = dataset_domestic, k = k)
        # Counting paid students per term
        dataset_domestic = dataset_domestic[['Program','Pay filter']].value_counts('Program').to_frame().reset_index()
        dataset_domestic.columns = ['Program','Returning students '+term]
        dataset_domestic['student'] = 'domestic'
        dataset = dataset_international.append(dataset_domestic, ignore_index = True)
        order = order.merge(dataset, on = ['Program', 'student'], how = 'left')

    payments = order.fillna(0)

    return payments


def registrations(order: pd.DataFrame, 
                  terms: List[str], 
                  cnxn: pyodbc.connect,
                  aal01:bool = True):
    """
    This function retrieves total number of registrations per program. It allows splitting such number by first term or
    upper year students for both, domestic and international students. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
	Returns: 
        Full dataframe containing total registrations per program for terms of interest filtered by new or returning students
        
    """
    # Duplicate the dataframe
    order_copy = order.copy()

    # Add a flag to the original dataframe
    order['student'] = 'domestic'
    order_copy['student'] = 'international'

    # Concatenate both dataframes
    order = pd.concat([order, order_copy], ignore_index=True)

    for term in terms: 

        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        # running the query 
        dataset = queries_as_of.xstl_query_term_level_campus(term,str(k), cnxn)
        
        # not interested in coop students
        dataset = dataset[dataset['current_load'].isin(['F','O'])]

        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')

        # Adding new/returning flag
        dataset['new_student_flag'] = np.where(((dataset['program'] == 'FIRE') & (dataset['AAL'] == '04')) | 
                                                ((dataset['program'] == 'TREX') & (dataset['AAL'] == '03')) |
                                              (dataset['AAL'] == '01'),'new','returning')

        if aal01: 
            dataset = dataset[dataset['new_student_flag'] == 'new']
        else: 
            dataset = dataset[dataset['new_student_flag'] == 'returning']


        dataset_international = dataset[dataset['imm_status']=='SV']
        dataset_international = dataset_international[['student_id','program']].value_counts('program').to_frame().reset_index()
        dataset_international.columns = ['Program','Registrations '+term]
        dataset_international['student'] = 'international'

        dataset_domestic = dataset[dataset['imm_status']!='SV']
        dataset_domestic = dataset_domestic[['student_id','program']].value_counts('program').to_frame().reset_index()
        dataset_domestic.columns = ['Program','Registrations '+term]
        dataset_domestic['student'] = 'domestic'

        dataset = dataset_international.append(dataset_domestic, ignore_index = True)
        order = order.merge(dataset, on = ['Program', 'student'], how = 'left')
    regs = order.fillna(0)
    return regs


def total_registrations(order:pd.DataFrame, 
                        terms: List[str], 
                        cnxn: pyodbc.connect):
    """
    This function retrieves total number of registrations per program. It allows splitting such number by first term or
    upper year students for both, domestic and international students. 
    
    Args:
        order (DataFrame): Reference dataframe to fill numbers. This way, all tables will have a 1-1 relation. 
        terms (List): List of terms to be used. 
        cnxn (pyodbc.connect): Conection string to access the database
    
	Returns: 
        Full dataframe containing total registrations per program for terms of interest. 
    
    """

    for term in terms: 

        # k is the number to pass to the query, it tells the query how many years back it should go to retrieve data as of today
        k = int(terms[-1][0:4]) - int(term[0:4])
        
        # running the query 
        dataset = queries_as_of.xstl_query_term_level_campus(term,str(k), cnxn)
        
        # not interested in coop students
        dataset = dataset[dataset['current_load'].isin(['F','O','P'])]

        # assumption. If level is missing, assume as AAL01. Setting it as integer
        dataset['AAL'] = dataset['AAL'].astype(str)
        dataset['AAL'] = dataset['AAL'].fillna('01')
        
        # 
        dataset = dataset[['student_id','program']].value_counts('program').to_frame().reset_index()
        dataset.columns = ['Program', 'Total Registrations '+term]
        
        # 
        order = order.merge(dataset, on = 'Program', how = 'left')
    regs = order.fillna(0)
    return regs