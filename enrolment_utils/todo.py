		# data_manipulation_as_of.DomesticRegs(order = order, 
        #                                     terms = terms, 
        #                                     cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                         sheet_name='DomesticRegistrations',
        #                                                         index=False)# ok
		# print('[Info] Domestic Registrations compiled successfully.') 
		# logger.info('DomesticRegs compiled successfully.')
		# data_manipulation_as_of.InternationalRegs(order = order, 
        #                                         terms = terms, 
        #                                         cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                 sheet_name='InternationalRegistrations',
        #                                                                 index=False)# ok
		# print('[Info] International Registrations compiled successfully.') 
		# logger.info('InternationalRegs compiled successfully.')
		# data_manipulation_as_of.term01deposits(order = order, 
        #                                         terms = terms, 
        #                                         cnxn = cnxn, 
		# 										sv_flag = True).to_excel(excel_writer = writer, 
        #                                                                 sheet_name='ReturningStudents_international',
        #                                                                 index=False)# ok
		# data_manipulation_as_of.term01deposits(order = order, 
        #                                         terms = terms, 
        #                                         cnxn = cnxn, 
		# 										sv_flag = False).to_excel(excel_writer = writer, 
        #                                                                 sheet_name='ReturningStudents_domestic',
        #                                                                 index=False)# ok
		
		# print('[Info] Returning Students compiled successfully.') 
		# logger.info('ReturningStudents compiled successfully.')
		# data_manipulation_as_of.NewStudents(order = order, 
        #                                     terms = terms, 
        #                                     cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                         sheet_name='NewStudents',
        #                                                         index=False)# ok
		# print('[Info] New Students compiled successfully.') 
		# logger.info('NewStudents compiled successfully.')
		# data_manipulation_as_of.RegistrationsRates(order = order, 
        #                                         terms = terms, 
        #                                         cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                 sheet_name='RegistrationsRates',
        #                                                                 index=False)# ok
		# print('[Info] Registrations for Rates compiled successfully.') 
		# logger.info('RegistrationsRates compiled successfully.')
		# data_manipulation_as_of.RegistrationsBudget(order = order, 
        #                                         terms = terms, 
        #                                         cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                 sheet_name='RegistrationsBudget',
        #                                                                 index=False)# ok
		# print('[Info] Registrations for Budget compiled successfully.') 
		# logger.info('RegistrationsBudget compiled successfully.')



		# data_manipulation_historical.AppsEndofCycle(order = order, #order_EndOfCycle, 
        #                                             terms = terms, 
        #                                             cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                 sheet_name = 'AppsEndofCycle', 
        #                                                                 index = False) # ok
		# print('[Info] Applications by End of Cycle compiled successfully.') 
		# logger.info('AppsEndofCycle compiled successfully.')
		# data_manipulation_historical.OffersEndofCycle(order = order, #order_EndOfCycle, , 
        #                                             terms = terms, 
        #                                             cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                     sheet_name = 'OffersEndofCycle', 
        #                                                                     index = False) #ok 
		# print('[Info] Offers by End of Cycle compiled successfully.') 
		# logger.info('OffersEndofCycle compiled successfully.')
		# data_manipulation_historical.ConfirmationsEndofCycle(order = order, #order_EndOfCycle, , 
        #                                                     terms = terms, 
        #                                                     cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                         sheet_name = 'ConfirmationsEndofCycle', 
        #                                                                         index = False) # ok
		# print('[Info] Confirmations by End of Cycle compiled successfully.') 
		# logger.info('ConfirmationsEndofCycle compiled successfully.')
		# data_manipulation_historical.RegistrationsEndofCycle(order =order, #order_EndOfCycle, , 
        #                                                     terms = terms, 
        #                                                     cnxn = cnxn).to_excel(excel_writer = writer, 
        #                                                                     sheet_name = 'RegistrationsEndofCycle', 
        #                                                                     index = False)# ok
		# print('[Info] Registrations End of Cycle compiled successfully.')
		# logger.info('RegistrationsEndofCycle compiled successfully.')
		# data_manipulation_historical.RegistrationHistorical(order = order_EndOfCycle,
        #                                                     sharepoint_user = sharepoint_user,
        #                                                     sharepoint_password = sharepoint_password,
        #                                                     sharepoint_base_url = sharepoint_base_url).to_excel(excel_writer = writer, 
        #                                                                                                         sheet_name = 'RegistrationHistorical', 
        #                                                                                                         index = False)# ok
		# print('[Info] Registration Historical compiled successfully.')                                                                                                                
		# logger.info('RegistrationHistorical compiled successfully.')
# order_EndOfCycle.to_excel(writer, 
        #                         sheet_name = 'ProgramsHistoric', 
        #                         index = False)# ok
		# print('[Info] Order file added successfully')
		# logger.info('Order added successfully.')

		

        #     ## OCAS data as of today
		# 	dataset = OCAS_data.sharepoint_download_excel_OCAS(sharepoint_user = sharepoint_user,
        #                                                         sharepoint_password = sharepoint_password,
        #                                                         sharepoint_base_url = sharepoint_base_url,
        #                                                         report_name = 'enrolment_report.xlsx')

		# 	dataset = OCAS_data.OCAS_enrolment_report(dataset = dataset)
		# 	df_apps, df_offs, df_cons = OCAS_data.OCAS_cleaning_process(order= order, 
		# 	                                                                dataset = dataset)
		# 	# applications
		# 	df_apps.to_excel(excel_writer = writer, 
		# 	                    sheet_name = 'OCAS_Applications', 
		# 	                    index = False)# ok
                
        #     # offers
		# 	df_offs.to_excel(excel_writer = writer, 
        #     			sheet_name = 'OCAS_Offers', 
        #     			index = False)# ok
                
        #     # confirmations
		# 	df_cons.to_excel(excel_writer = writer, 
        #     			sheet_name = 'OCAS_Confirmations', 
        #     			index = False)# ok
                
		# 	print('[Info] OCAS as of date data added successfully')
		# 	logger.info('OCAS as of date added successfully.')
                
        #     ## OCAS data historical 
		# 	dataset = OCAS_data.sharepoint_download_excel_OCAS(sharepoint_user = sharepoint_user,
        #                                                         sharepoint_password = sharepoint_password,
        #                                                         sharepoint_base_url = sharepoint_base_url,
        #                                                         report_name = 'OCAS_historical.xlsx')

		# 	dataset = OCAS_data.OCAS_enrolment_report(dataset = dataset)
		# 	df_apps, df_offs, df_cons = OCAS_data.OCAS_cleaning_process(order= order, 
        #     			dataset = dataset)
		# 	# applications
		# 	df_apps.to_excel(excel_writer = writer, 
        #     			sheet_name = 'OCAS_Applications_hist', 
        #     			index = False)# ok
                
        #     # offers
		# 	df_offs.to_excel(excel_writer = writer, 
        #     			sheet_name = 'OCAS_Offers_hist', 
        #     			index = False)# ok
                
        #     # confirmations
		# 	df_cons.to_excel(excel_writer = writer, 
        #     			sheet_name = 'OCAS_Confirmations_hist', 
        #     			index = False)# ok
                
		# 	print('[Info] OCAS historical data added successfully')
		# 	logger.info('OCAS historical added successfully.')