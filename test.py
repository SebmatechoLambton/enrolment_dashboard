# import time
 
# # a module which has functions related to time.
# # It can be installed using cmd command:
# # pip install time, in the same way as pyautogui.
# import pyautogui

# while True:

# 	time.sleep(10)
	
# 	pyautogui.moveTo(1000, 1000, duration = 1)
# 	pyautogui.dragRel(100, 0, duration = 1)

###############################################
import warnings
import schedule
import datetime as dt
import time
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
		today_date = dt.datetime.today().strftime(format = '%m_%d_%Y_%H_%M_%S')
		print(today_date)


	job() 

	
	schedule.every(2).minutes.do(job)
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