from enrolment_utils import custom_sharepoint

def naming_files():
	intake_mapping = {
		'fall': {
			'terms': ['2020F', '2021F', '2022F', '2023F', '2024F'],
			'file_name_order': 'order_fall.txt',
			'file_name_budget': 'budget_fall.txt',
			'report_name_OCAS': 'province_report_fall.xlsx',
			'message': 'STARTING COMPILATION FOR FALL TERMS'
		},
		'winter': {
			'terms': ['2020W', '2021W', '2022W', '2023W', '2024W'],
			'file_name_order': 'order_winter.txt',
			'file_name_budget': 'budget_winter.txt',
			'report_name_OCAS': 'province_report_winter.xlsx',
			'message': 'STARTING COMPILATION FOR WINTER TERMS'
		},
		'summer': {
			'terms': ['2020S', '2021S', '2022S', '2023S', '2024S'],
			'file_name_order': 'order_summer.txt',
			'file_name_budget': 'budget_summer.txt',
			'report_name_OCAS': 'province_report_summer.xlsx',
			'message': 'STARTING COMPILATION FOR SUMMER TERMS'
		}
	}
	return intake_mapping;

def catchment_convention():
	catchment_dict = {'algo_catchment': 'Algonquin', 
						'gbtc_catchment': 'George Brown', 
						'sene_catchment': 'Seneca', 
						'sher_catchment': 'Sheridan',  
						'slaw_catchment': 'St. Lawrence', 
						'ssfl_catchment': 'Fleming', 
						'unknown catchment name eng': 'Unknown', 
						'cons_catchment': 'Conestoga', 
						'durh_catchment': 'Durham', 
						'fans_catchment': 'Fanshawe',
						'geor_catchment': 'Georgian', 
						'moha_catchment': 'Mohawk', 
						'nort_catchment': 'Northern', 
						'cent_catchment': 'Centennial', 
						'lamb_catchment': 'Lambton', 
						'stcl_catchment': 'St. Clair', 
						'camb_catchment': 'Cambrian', 
						'cana_catchment': 'Canadore', 
						'conf_catchment': 'Confederation', 
						'humb_catchment': 'Humber', 
						'loyt_catchment': 'Loyalist', 
						'niag_catchment': 'Niagara', 
						'saul_catchment': 'Sault'}
	return catchment_dict

def colleges_sizes():
	college_size_dict = {'Algonquin':'Large',
    'Cambrian':'Small',
    'Canadore':'Small',
    'Centennial':'Large',
    'Collège Boréal':'Small',
    'Conestoga':'Medium',
    'Confederation':'Small',
    'Durham':'Medium',
    'Fanshawe':'Large',
    'Fleming':'Medium',
    'George Brown':'Large',
    'Georgian':'Medium',
    'Humber':'Large',
    'La Cité Collégiale':'Medium',
    'Lambton':'Small',
    'Loyalist':'Small',
    'Michener Institute':'Not Classified',
    'Mohawk':'Large',
    'Niagara':'Medium',
    'Niagara Parks School Of Hortic':'Not Classified',
    'Northern':'Small',
    'Ridgetown Campus':'Not Classified',
    'Sault':'Small',
    'Seneca':'Large',
    'Sheridan':'Large',
    'St. Clair':'Medium',
    'St. Lawrence':'Medium'}

	return college_size_dict

def intake_dict(): 
	intake_dict= {'F':'fall',
                   'W':'winter',
                   'S':'spring'}
	return intake_dict

def start_end_term_dates():
	end_date_map = {'F': '-09-20',
        'W': '-01-06',
        'S': '-05-06'}
	start_date_map = {'F': '-09-21',
        'W': '-01-05',
        'S': '-05-05'}
	return end_date_map, start_date_map

def program_school_dict(file_name):

    # Importing auxiliary files (order and order end of cycle)
    sharepoint_base_url = 'https://mylambton.sharepoint.com/sites/EnrolmentDashboard'

    order = custom_sharepoint.sharepoint_download(sharepoint_base_url = sharepoint_base_url,
                                                      local_folder = 'orders',
                                                      file_name = file_name)


    return order.set_index(order.columns[0])[order.columns[1]].to_dict()