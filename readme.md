# Enrolment Dashboard
###### Developed by Sébastien Lozano-Forero. 

## Description
This projects aims to automate the enrolment reporting for various simultaneous intakes. It uses OCAS data, production (database), snapshots and static files hosted at Sharepoint to provide a comprehensive report of current enrolment cycle compared with past enrolment cycles. 

## How it works?s
The whole project is initiated using the *run_report.py* file. 

It uses the **schedule** module to run the project every six hours, run the process and creates an output file embedded into sharepoint. The project is accessible through a PowerBI dashboard which runs into the PowerBI service and takes information directly from the updated sharepoint file.

The whole process is developed with the projection files at its core. Such file contains the active process for current enrolment cycle, even providing the projected amount of registrations. Such file is to be stored within Sharepoint. Once the process has gone through the projections, sets a list of programs of interest. Such list of programs is then used to gather information about the applicaciones, first choice applications, offers, confirmations, waitlisted, deleted applications, withdrawals, payments and confirmations for both domestic and international students. 

This project also aims to provide an estimation of registrations by program and the likelihood of the program reaching the expected number of registrations. This piece uses a historical base built for all programs in the list of reporting programs. For each of them, the record of applications, confirmations and registrations was built and hosted in sharepoint. Such information is used by Facebook prophet model in order to provide an estimation of total number of registrations. 

this project also uses OCAS data. OCAS provides the ability to schedule sending reports to emails. A Power Automate script was developed in order to embed such reports into sharepoint and from there, to be accessible to the Python process. 

## What data is coming from where? 

- production data refers to college database. Such database is used to reflect main numbers from the cycle based on college numbers. Historically, OCAS provided this, but it was noted that discrepancies were in place. Because of this, is recommended to prefer production numbers at the beggining of the enrolment cycle and OCAS numbers when reaching the end of it.

- OCAS data includes the baseline comparison to see what is going on province-wide, by catchment and by program. This dataset is set to be sent daily.

- Sharepoint is used as a static way to host files, here OCAS files are dropped, auxiliary files (like order, projections, schools and program status)

Finally, Power BI is set to look up for updated input files in Sharepoint. 