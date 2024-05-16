import pandas as pd
import numpy as np

def AppsEndofCycleQuery(term, cnxn):
    
    query = """
SELECT APPL_APPLICANT AS Applicant_ID
	,APPL_ACAD_PROGRAM AS Program
    ,APPL_PRIORITY AS Level
    ,APPL_STATUS as status
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
ORDER BY APPL_APPLICANT
	,APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)


def OffersEndofCycleQuery(term, cnxn):

    
    query = """
SELECT APPL_APPLICANT AS Applicant_ID
	,APPL_ACAD_PROGRAM AS Program
    ,APPL_STATUS as Curr_Status
    ,APPL_PRIORITY AS Level
    ,APPL_CHOICE as Choice
    ,APPL_START_TERM
    , REPLACE(
        REPLACE(REPLACE((CAST((
            SELECT stat.APPL_STATUS AS X FROM APPL_STATUSES AS stat
            WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
            AND stat.APPL_STATUS IS NOT NULL
            FOR XML PATH('')) AS VARCHAR(2048))),
            '</X><X>', ' '),'<X>', ''),'</X>',''
    ) AS 'PreviousStatuses'
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
ORDER BY APPL_APPLICANT
	,APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)




def ConfirmationsEndofCycleQuery(term, cnxn):

    
    query = """
SELECT APPL_APPLICANT AS Applicant_ID
	,APPL_ACAD_PROGRAM AS Program
    ,APPL_STATUS as Curr_Status
    ,APPL_CHOICE as Choice
    , APPL_PRIORITY as Level
    , REPLACE(
        REPLACE(REPLACE((CAST((
            SELECT stat.APPL_STATUS AS X FROM APPL_STATUSES AS stat
            WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
            AND stat.APPL_STATUS IS NOT NULL
            FOR XML PATH('')) AS VARCHAR(2048))),
            '</X><X>', ' '),'<X>', ''),'</X>',''
    ) AS 'PreviousStatuses'
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
ORDER BY APPL_APPLICANT
	,APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)


def RegistrationsEndofCycleQuery(term, cnxn):

    
    query = """
SELECT STC_PERSON_ID AS Applicant_ID
	,IMMIGRATION_STATUS AS 'Imm. Status'
	,SUBSTRING(STC_COURSE_NAME, 6, 4) AS Program
	,STC_SECTION_NO AS AAL
	,STTR_STUDENT_LOAD AS 'Current Load'
	,STTR_USER1 AS '10th Load'
	,STC_STATUS AS Curr_Status
FROM STUDENT_ACAD_CRED AA
JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM STC_STATUSES
		WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
		)
JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
JOIN PERSON P ON STC_PERSON_ID = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE STC_TERM = '"""+term+"""'
	AND STC_SUBJECT = 'CTRL'
	AND SCS_LOCATION = 'MAIN'
ORDER BY STC_PERSON_ID
	,STC_COURSE_NAME
    """
    query = pd.read_sql(query, cnxn)
    return(query)


def TableauQuery(term, cnxn):
    """Query to extract relevant data about the "Tableau" data.

    Args:
        term (string):term to be used when retreiving information
        cnxn (pyodbc.connect): Conection string to access the database
    """
    query = """
SELECT APPL_ACAD_PROGRAM AS program
	,APPL_STATUS AS curr_status
    ,APPL_PRIORITY AS level
	,APPL_CHOICE AS choice
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
		)
JOIN PERSON ON APPL_APPLICANT = ID
LEFT JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '"""+term+"""'
    """
    query = pd.read_sql(query, cnxn)

    return query;