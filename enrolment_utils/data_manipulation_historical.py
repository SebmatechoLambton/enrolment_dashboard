import pandas as pd 
import numpy as np
import datetime as dt
from enrolment_utils import queries_historical, custom_sharepoint

def AppsEndofCycle(order, terms, cnxn):
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


def OffersEndofCycle(order, terms, cnxn):
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


def ConfirmationsEndofCycle(order, terms, cnxn):
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



def RegistrationsEndofCycle(order, terms, cnxn):
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
                           terms = ['2017F','2018F','2019F','2020F','2021F','2022F']):
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