# Enrolment Dashboard
###### Developed by Sébastien Lozano-Forero. 

## Description
This projects aims to automate the enrolment reporting for various simultaneous intakes. It uses OCAS data, production (database), snapshots and static files hosted at Sharepoint to provide a comprehensive report of current enrolment cycle compared with past enrolment cycles. 

This project also aims to provide an estimation of registrations by program.

## How it works?
The whole project is initiated using the *run_report.py* file. 

It uses the **schedule** module to run the project every six hours, run the process and creates an output file embedded into sharepoing. The project is accessible through a PowerBI dashboard which runs into the PowerBI service and takes information directly from the updated sharepoint file. 