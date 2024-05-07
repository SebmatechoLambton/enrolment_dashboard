import warnings
import schedule
import time
from enrolment_utils import main_pipeline
import argparse

warnings.filterwarnings("ignore")



if __name__ == '__main__':

	# Setting console arguments
	
	# parser = argparse.ArgumentParser()
	# Add an argument via flag in CLI 
	# parser.add_argument('--intake', type=str, default = 'fall')


	# args = parser.parse_args()
	# intake = args.intake

	# main_pipeline.enroment_dashboard_update(intake = 'intake')
	def job():
		main_pipeline.enroment_dashboard_update(intake = 'winter')
		main_pipeline.enroment_dashboard_update(intake = 'fall')
		main_pipeline.enroment_dashboard_update(intake = 'summer') 


	job() 

	
	schedule.every(6).hours.do(job)
	# schedule.every().monday.at("09:00").do(job)
	# schedule.every().monday.at("15:00").do(job)
	# schedule.every().tuesday.at("09:00").do(job)
	# schedule.every().tuesday.at("15:00").do(job)
	# schedule.every().wednesday.at("09:00").do(job)
	# schedule.every().wednesday.at("15:00").do(job)
	# schedule.every().thursday.at("09:00").do(job)
	# schedule.every().thursday.at("15:00").do(job)
	# schedule.every().friday.at("09:00").do(job)
	# schedule.every().friday.at("15:00").do(job)

	while True:
		schedule.run_pending()
		time.sleep(1)