from enrolment_utils import data_manipulation_as_of, data_manipulation_historical, global_params
from enrolment_utils import custom_sharepoint, apps_confs_progression
# from utils import credentials
from enrolment_utils import OCAS_data
from enrolment_utils import side_files_handling
import enrolment_utils.python_utils as utils_geral
import enrolment_utils.probs_target_utils as utils_prob

from os.path import isfile, join
# from decouple import config
from pathlib import Path
from os import listdir
import datetime as dt
import time

# import schedule
import time as tm
# from schedule import every, repeat
# from datetime import time, timedelta, datetime


import pandas as pd
import numpy as np
import warnings
import logging
import shutil
import os

warnings.filterwarnings("ignore")


def enroment_dashboard_update(intake:str):
	# Measuring time of execution
	start_time = time.perf_counter()

	# Creating log setting
	if not os.path.exists('Logs'):
		os.makedirs('Logs')
	logfile = 'Logs/EnrolmentReport2.log'
	logging.basicConfig(
		filename = logfile,
		level=logging.DEBUG, 
		format = '%(asctime)s-%(levelname)s-%(name)s-%(message)s',
		datefmt = '%Y-%m-%d %H:%M:%S')
    
	# Creating logging file
	logger = logging.getLogger('EnrolmentReport2')


	# Importing auxiliary files (order and order end of cycle)
	sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard/'

	# Initializing global parameters
	intake_mapping = global_params.naming_files()
	if intake in intake_mapping:
		config = intake_mapping[intake]
		terms = config['terms']
		file_name_order = config['file_name_order']
		file_name_budget = config['file_name_budget']
		report_name_OCAS = config['report_name_OCAS']
		print('-' * 25, config['message'], '-' * 25)
	else:
		print(f"Invalid intake value: {intake}")

	# Pulling data from Projections file
	side_files_handling.setting_order_budget(terms = terms, 
										  sharepoint_base_url = sharepoint_base_url)

	# Setting the list of programs of interest
	order = custom_sharepoint.sharepoint_download(sharepoint_base_url = sharepoint_base_url,
													local_folder = 'orders',
													file_name = file_name_order)

    # Creating conextion to production 
	cnxn = utils_geral.get_connection()
        
    # Handling with paths and file naming 
	root = Path.cwd()
	# date_report = dt.datetime.now().strftime('%m%d%Y_%H%M%S')
	report_name = terms[-1] +'_EnrolmentReport'
	report_path = 'reports/'+report_name+'.xlsx'
	output_root = Path(root/report_path)
        
	# if path does not exist, create it
	report_path = root/'reports'
	if not Path.exists(report_path):
		report_path.mkdir(parents= True, exist_ok= True)


    #Creating overall output file 
	with pd.ExcelWriter(output_root, engine='xlsxwriter') as writer:
		data_manipulation_as_of.FirstApplications(order = order, 
                                                terms = terms, 
                                                cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='FirstChoiceApplications',
                                                                        index=False) # ok
		print('[Info] First Choice Applications compiled successfully.')
		logger.info('FirstApplications compiled successfully.')
		data_manipulation_as_of.Applications(order = order, 
                                            terms = terms, 
                                            cnxn = cnxn).to_excel(excel_writer = writer,
                                                                sheet_name='Applications',
                                                                index=False)# ok
		print('[Info] Applications compiled successfully.')
		logger.info('Applications compiled successfully.')
		data_manipulation_as_of.DeletedApplications(order = order, 
                                                    terms = terms, 
                                                    cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='DeletedApplications',
                                                                        index=False)# ok
		print('[Info] Deleted Applications compiled successfully.')                                                                          
		logger.info('Deleted Applications compiled successfully.')
		data_manipulation_as_of.Offers(order = order, 
                                    terms = terms, 
                                    cnxn = cnxn).to_excel(excel_writer = writer, 
                                                            sheet_name='Offers',
                                                            index=False)# ok
		print('[Info] Offers compiled successfully.') 
		logger.info('offers compiled successfully.')
		data_manipulation_as_of.confirmations(order = order, 
                                            terms = terms, 
                                            cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                    sheet_name='Confirmations',
                                                                    index=False)# ok
		print('[Info] Confirmations compiled successfully.') 
		logger.info('confirmations compiled successfully.')

		data_manipulation_as_of.TableauOutstandingOffers(order = order, 
                                                terms = terms, 
                                                cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='OutstandingOffers',
                                                                        index=False)#ok
		print('[Info] Tableau Outstanding Offers compiled successfully.') 
		logger.info('TableauOutstandingOffers compiled successfully.')
		# TableauConfirmations().to_excel(writer, sheet_name='Confirmations',index=False)
		data_manipulation_as_of.TableauHolds(order = order, 
                                                terms = terms, 
                                                cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='Holds',
                                                                        index=False)# ok
		print('[Info] Tableau Holds compiled successfully.') 
		logger.info('TableauHolds compiled successfully.')
		data_manipulation_as_of.TableauWithdrawals(order = order, 
                                                terms = terms, 
                                                cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='Withdrawals',
                                                                        index=False)# ok
		print('[Info] Tableau Withdrawals compiled successfully.') 
		logger.info('TableauWithdrawals compiled successfully.')
		data_manipulation_as_of.TableauWaitlist(order = order, 
                                                terms = terms, 
                                                cnxn = cnxn).to_excel(excel_writer = writer, 
                                                                        sheet_name='Waitlist',
                                                                        index=False)# ok
		print('[Info] Tableau Waitlist compiled successfully.') 
		logger.info('TableauWaitlist compiled successfully.')

		data_manipulation_as_of.MapInfo(terms = terms, 
                                        cnxn = cnxn).to_excel(excel_writer = writer, 
                                                            sheet_name = 'MapInfo', 
                                                            index = False)# ok
		print('[Info] Map Information compiled successfully.') 
		logger.info('MapInfo compiled successfully.')

		data_manipulation_as_of.term_deposits(order = order, 
										terms = terms, 
										cnxn = cnxn, 
										aal01 = True ).to_excel(excel_writer = writer, 
                                                            sheet_name = 'Term01Deposits', 
                                                            index = False)# ok
		print('[Info] Term 01 deposits compiled successfully.') 
		logger.info('Term01Deposits compiled successfully.')

		data_manipulation_as_of.term_deposits(order = order, 
										terms = terms, 
										cnxn = cnxn, 
										aal01 = False ).to_excel(excel_writer = writer, 
                                                            sheet_name = 'UpperYearDeposits', 
                                                            index = False)# ok
		print('[Info] Upper year deposits compiled successfully.') 
		logger.info('UpperYearDeposits compiled successfully.')

		data_manipulation_as_of.registrations(order = order, 
										terms = terms, 
										cnxn = cnxn, 
										aal01 = True ).to_excel(excel_writer = writer, 
                                                            sheet_name = 'Term01Registrations', 
                                                            index = False)# ok
		print('[Info] Term 01 deposits compiled successfully.') 
		logger.info('Term01Registrations compiled successfully.')

		data_manipulation_as_of.registrations(order = order, 
										terms = terms, 
										cnxn = cnxn, 
										aal01 = False ).to_excel(excel_writer = writer, 
                                                            sheet_name = 'UpperYearRegistrations', 
                                                            index = False)# ok
		print('[Info] Upper year registrations compiled successfully.') 
		logger.info('UpperYearRegistrations compiled successfully.')
		

		data_manipulation_as_of.total_registrations(order = order, 
											  terms = terms, 
											  cnxn = cnxn).to_excel(excel_writer = writer, 
                                                            sheet_name = 'TotalRegistrations', 
                                                            index = False)# ok
		print('[Info] Total registrations compiled successfully.') 
		logger.info('TotalRegistrations compiled successfully.')

		side_files_handling.setting_order_budget_ottawa(terms = terms, 
												  cnxn = cnxn).to_excel(excel_writer = writer, 
                                                            sheet_name = 'Ottawa', 
                                                            index = False)# ok
		print('[Info] Ottawa Information compiled successfully.') 
		logger.info('Ottawa Information compiled successfully.')
		# All registrations all programs all AALs
		total_registrations = utils_geral.xstl_query_term_level_campus(term = terms[-1], 
																 campus = 'MAIN',
																 cnxn  = cnxn)
		total_registrations = total_registrations[(total_registrations['acad_level']=='PS')&(total_registrations['current_load'].isin(['F','O']))]
		total_registrations[['student_id','program']].groupby('program').count().reset_index().to_excel(excel_writer = writer, 
						   																				sheet_name = 'regs_all_progs_all_aals', 
                                                            											index = False)
		
		total_registrations_ott = utils_geral.xstl_query_term_level_campus(term = terms[-1], 
																	 campus = 'OTT',
																	 cnxn  = cnxn)
		total_registrations_ott = total_registrations_ott[(total_registrations_ott['acad_level']=='PS')&(total_registrations_ott['current_load'].isin(['F','O']))]
		total_registrations_ott[['student_id','program']].groupby('program').count().reset_index().to_excel(excel_writer = writer, 
						   																				sheet_name = 'regs_all_progs_all_aals_ott', 
                                                            											index = False)

		order.to_excel(writer, 
                    sheet_name = 'order', 
                    index = False)# ok
		
		side_files_handling.program_information(order = order, 
										  		cnxn = cnxn).to_excel(	writer, 
																		sheet_name = 'order_names', 
																  		index = False)

        ## OCAS province comparison 
		dataset = OCAS_data.sharepoint_download_excel_OCAS( sharepoint_base_url = sharepoint_base_url,
                                                            report_name = report_name_OCAS )

		dataset = OCAS_data.OCAS_enrolment_report_province(dataset = dataset)
		
		OCAS_data.OCAS_enrolment_report_province_format(dataset = dataset, terms = terms).to_excel(	excel_writer = writer, 
                                                                                   					sheet_name = 'OCAS_province_comparison', 
                                                                                   					index = False)# o
		logger.info('OCAS province comparison added successfully.')
		print('[Info] OCAS Province Comparison data added successfully')
		# OCAS catchement movement
		OCAS_data.OCAS_net_movement(dataframe = dataset, terms = terms, cnxn = cnxn).to_excel(	excel_writer = writer, 
                                                                                    			sheet_name = 'OCAS_net_movement', 
                                                                                    			index = False)# o
		print('[Info] OCAS net movement comparison data added successfully')
		# logger.info('OCAS net movement added successfully.')

		print('[Info] Updating program history and creating probabilities of hitting bugdet values')
		utils_prob.target_probability_report(term = terms[-1], 
											file_name_budget = file_name_budget,
											start_year = int(terms[0][:4]),
											end_year = int(terms[-1][:4]),
											file_name = file_name_order,
											test_mode = True, ###### testing mode
											cnxn = cnxn).to_excel(excel_writer = writer, 
						   												sheet_name = 'probability_targets', 
                                                            			index = False)
		apps_confs_progression.compiling_historical_college(terms = terms,
													  		folder = 'applications').to_excel(excel_writer = writer, 
						   												sheet_name = 'apps_cumulative', 
                                                            			index = False)
		apps_confs_progression.compiling_historical_college(terms = terms,
													  		folder = 'confirmations').to_excel(excel_writer = writer, 
						   												sheet_name = 'confs_cumulative', 
                                                            			index = False)
		
		side_files_handling.registration_counts(terms = terms, 
										  cnxn = cnxn).to_excel(excel_writer = writer, 
															sheet_name = 'registration_counts', 
															index = False)

		print('[Info] probabilities of hitting budgets added successfully')
		logger.info('probabilities of hitting budgets added successfully.')
	
	# Upload file to sharepoint
	custom_sharepoint.sharepoint_upload(sharepoint_base_url = sharepoint_base_url,
                                        root = root,
                                        report_name = report_name, 
										file_name = f'DashboardInput_{intake}.xlsx')

	logger.info('Sharepoint updated successfully.')

	
	
	print('[Info]  All registrations all programs all AALs included successfully')
	logger.info('All registrations all programs all AALs included successfully.')
	
	dst_path = Path(root/f'reports/DashboardInput.xlsx')
	shutil.copy(output_root, dst_path)
	print('[Info] Daily report and Dashboard input successfully created.')

	end_time = time.perf_counter()
	total_time = end_time - start_time
    
	
	print(f'[Info] Ran on {dt.datetime.now().strftime(format = "%Y-%m-%d %H:%M:%S")}. Total execution time: {total_time//60:.0f}:{total_time%60:02.0f}')

	

