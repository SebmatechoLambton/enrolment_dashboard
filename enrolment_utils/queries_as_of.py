import pandas as pd
import numpy as np
import pyodbc

def ApplicationsQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
	"""Query to extract relevant data about applications.

	Args:
		term (str): Term to be used when retrieving information.
		number (str): Number of years to go back so numbers can be as of this day in previous years.
		cnxn (pyodbc.connect): Connection string to access the database.
	
	Returns:
		pd.DataFrame: Dataframe with data of interest.
		
	Example Usage:
	
	"""
	query = f"""
	DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, {number}, GETDATE())

	SELECT 
		APPL_APPLICANT AS Applicant_ID
		,APPL_ACAD_PROGRAM AS Program
		,APPL_PRIORITY AS Level
		,APPL_STATUS as status
	FROM APPLICATIONS AA
	JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
		AND POS = (
			SELECT TOP 1 POS
			FROM APPL_STATUSES
			WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
				AND APPL_STATUS_DATE <= @STAT_DATE
		)
	JOIN PERSON P ON APPL_APPLICANT = P.ID
	JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
	WHERE APPL_START_TERM = '{term}'
	ORDER BY APPL_APPLICANT
		,APPL_ACAD_PROGRAM

	"""
	query = pd.read_sql(query, cnxn)
	return query



# DLT to be included in the system on November 2022. 

def OffersQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    """Query to extract relevant data about offers.

    Args:
        term (string):term to be used when retreiving information
        number (string): Number of years to go back so numbers can be as of this day in previous years
        cnxn (pyodbc.connect): Conection string to access the database
    """
    query = f"""
    DECLARE @STAT_DATE AS DATETIME =  DATEADD(YEAR, -{number}, GETDATE());
SELECT APPL_APPLICANT AS Applicant_ID
,APPL_START_TERM
,APPL_ACAD_PROGRAM AS Program
,APPL_PRIORITY AS Level
,REPLACE(REPLACE(REPLACE((
CAST((
SELECT stat.APPL_STATUS AS X
FROM APPL_STATUSES AS stat
WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
AND stat.APPL_STATUS IS NOT NULL
FOR XML PATH('')
) AS VARCHAR(2048))
), '</X><X>', ' '), '<X>', ''), '</X>', '') AS 'PreviousStatuses'
,APPL_STATUS AS Curr_Status
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
AND POS = (
SELECT TOP 1 POS
FROM APPL_STATUSES
WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
AND APPL_STATUS_DATE <= @STAT_DATE
)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '{term}'
ORDER BY APPL_APPLICANT
,APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)



def TableauQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    """Query to extract relevant data about the "Tableau" data.

    Args:
        term (string):term to be used when retreiving information
        number (string): Number of years to go back so numbers can be as of this day in previous years
        cnxn (pyodbc.connect): Conection string to access the database
    """
    query = f"""
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

SELECT APPL_ACAD_PROGRAM AS Program
	,APPL_STATUS AS Curr_Status
    ,APPL_PRIORITY AS Level
	,APPL_CHOICE AS Choice
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
			AND APPL_STATUS_DATE <= @STAT_DATE
		)
JOIN PERSON ON APPL_APPLICANT = ID
LEFT JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '{term}'
    """
    query = pd.read_sql(query, cnxn)
    return(query)


def ConfirmationsQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    
    query = """
    DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE())
    SELECT APPL_ACAD_PROGRAM AS Program, 
    REPLACE(
        REPLACE(REPLACE((CAST((
            SELECT stat.APPL_STATUS AS X FROM APPL_STATUSES AS stat
            WHERE stat.APPLICATIONS_ID = AA.APPLICATIONS_ID
            AND stat.APPL_STATUS IS NOT NULL
            FOR XML PATH('')) AS VARCHAR(2048))),
            '</X><X>', ' '),'<X>', ''),'</X>',''
    ) AS 'Previous Statuses'
    ,APPL_STATUS AS Curr_Status
    ,APPL_PRIORITY AS Level
    FROM APPLICATIONS AA 
    join APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID and POS = (SELECT TOP 1 POS FROM  APPL_STATUSES WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID AND APPL_STATUS_DATE <= @STAT_DATE  )
    JOIN PERSON P ON APPL_APPLICANT = P.ID
    JOIN ADDRESS on ADDRESS_ID = PREFERRED_ADDRESS 
    WHERE APPL_START_TERM = '{term}'
    ORDER BY APPL_APPLICANT, APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)



def FirstApplicationsQuery(term,number, cnxn):
    
    query = """
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

SELECT APPL_APPLICANT AS Applicant_ID
	,APPL_ACAD_PROGRAM AS Program
	,APPL_CHOICE AS Choice
    ,APPL_PRIORITY AS Level
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
			AND APPL_STATUS_DATE <= @STAT_DATE
		)
JOIN PERSON ON APPL_APPLICANT = ID
LEFT JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM = '{term}'
ORDER BY APPL_ACAD_PROGRAM
	,APPL_STATUS
	,APPL_APPLICANT
    """
    query = pd.read_sql(query, cnxn)
    return(query)


def MapInfoQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:

    
    query = f"""
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

SELECT 
	APPL_APPLICANT AS Applicant_ID
	,BIRTH_DATE
	,GENDER
	,CASE 
		WHEN IMMIGRATION_STATUS = 'NA'
			THEN 'NatAme'
		ELSE 'NotNatAme'
		END AS 'Indigenous Status'
	,CITY
	,ADDRESS.ZIP AS 'Postal Code'
	,APPL_ACAD_PROGRAM AS Program
	,APPL_STATUS AS Curr_Status
	,APPL_CHOICE AS Choice
FROM APPLICATIONS AA
JOIN APPL_STATUSES BB ON AA.APPLICATIONS_ID = BB.APPLICATIONS_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM APPL_STATUSES
		WHERE APPLICATIONS_ID = AA.APPLICATIONS_ID
			AND APPL_STATUS_DATE <= @STAT_DATE
		)
JOIN PERSON P ON APPL_APPLICANT = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE APPL_START_TERM =  '{term}'
ORDER BY APPL_APPLICANT
	,APPL_ACAD_PROGRAM
    """
    query = pd.read_sql(query, cnxn)
    return(query)



def DomesticRegistrationsQuery(term,number, cnxn):

    
    query = """
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

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
			AND STC_STATUS_DATE <= @STAT_DATE
		)
JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
JOIN PERSON P ON STC_PERSON_ID = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE STC_TERM = '{term}'
	AND STC_SUBJECT = 'CTRL'
ORDER BY STC_PERSON_ID
	,STC_COURSE_NAME
    """
    query = pd.read_sql(query, cnxn)
    return(query)




def InternationalRegistrationsQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    
    query = f"""
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

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
			AND STC_STATUS_DATE <= @STAT_DATE
		)
JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
JOIN PERSON P ON STC_PERSON_ID = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE STC_TERM = '{term}'
	AND STC_SUBJECT = 'CTRL'
ORDER BY STC_PERSON_ID
	,STC_COURSE_NAME
    """
    query = pd.read_sql(query, cnxn)
    return(query)

def RegistrationsRatesQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:

    
    query = """
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

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
			AND STC_STATUS_DATE <= @STAT_DATE
		)
JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
JOIN PERSON P ON STC_PERSON_ID = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE STC_TERM = '{term}'
	AND STC_SUBJECT = 'CTRL'
ORDER BY STC_PERSON_ID
	,STC_COURSE_NAME
    """
    query = pd.read_sql(query, cnxn)
    return(query)



def RegistrationsBudgetQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    
    query = f"""
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR, -{number}, GETDATE());

SELECT STC_PERSON_ID AS Applicant_ID
	,SUBSTRING(STC_COURSE_NAME, 6, 4) AS Program
	,STC_SECTION_NO AS AAL
	,STTR_STUDENT_LOAD AS 'Current Load'
	,STTR_USER1 AS '10th Load'

FROM STUDENT_ACAD_CRED AA
JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
	AND POS = (
		SELECT TOP 1 POS
		FROM STC_STATUSES
		WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
			AND STC_STATUS_DATE <= @STAT_DATE
		)
JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
JOIN PERSON P ON STC_PERSON_ID = P.ID
JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
WHERE STC_TERM = '{term}'
	AND STC_SUBJECT = 'CTRL'
ORDER BY STC_PERSON_ID
	,STC_COURSE_NAME
    """
    query = pd.read_sql(query, cnxn)
    return(query)



def ReturningStudentsQuery(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:

    
    query = f"""
DECLARE @TERM AS VARCHAR(10) = '{term}';
DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR,  -{number}, GETDATE());

WITH T1 (
	STC_PERSON_ID
	,IMMIGRATION_STATUS
	,Program
	,AAL
	,Course_Name
	,SCS_LOCATION
	,STNT
	,STNT_Date
	)
AS (
	SELECT STC_PERSON_ID
		,IMMIGRATION_STATUS
		,substring(STC_COURSE_NAME, 6, 4) AS Program
		,STC_SECTION_NO AS AAL
		,STC_COURSE_NAME + '-' + STC_SECTION_NO AS 'CTRL Course'
		,SCS_LOCATION
		,cast((
				SELECT STTN_NOTES + ' '
				FROM STTN_TERM_NOTES
				WHERE STUDENT_TERM_NOTES_ID = STC_PERSON_ID + '*' + STC_TERM
				FOR XML path('')
				) AS VARCHAR(100)) AS STNT
		,cast((
				SELECT STTN_DATES + ' '
				FROM STTN_TERM_NOTES
				WHERE STUDENT_TERM_NOTES_ID = STC_PERSON_ID + '*' + STC_TERM
				FOR XML path('')
				) AS VARCHAR(100)) AS STNT_Date
	FROM STUDENT_ACAD_CRED
	INNER JOIN STC_STATUSES ON STUDENT_ACAD_CRED.STUDENT_ACAD_CRED_ID = STC_STATUSES.STUDENT_ACAD_CRED_ID
	INNER JOIN STUDENT_COURSE_SEC ON STUDENT_COURSE_SEC_ID = STC_STUDENT_COURSE_SEC
	LEFT JOIN STTN_TERM_NOTES ON STUDENT_TERM_NOTES_ID = STC_PERSON_ID + '*' + STC_TERM
	LEFT JOIN PERSON ON ID = STC_PERSON_ID
	WHERE STC_TERM = @TERM
		AND STC_SUBJECT = 'CTRL'
		AND STC_STATUSES.POS = 1
		AND STC_STATUS IN (
			'N'
			,'A'
			,'D'
			)
		AND SCS_LOCATION = 'MAIN'
		AND STC_ACAD_LEVEL = 'PS'
	)
	,T3 (
	ARP_PERSON_ID
	,ARP_AMT
	,ARP_TERM
	,Pay_Methods
	,ARP_DATE
	,Pay_Methods_Deposit
	)
AS (
	SELECT ARP_PERSON_ID
		,ARP_AMT
		,ARP_TERM
		,cast((
				SELECT RCPT_PAY_METHODS + ' '
				FROM RCPT_NON_CASH
				WHERE CASH_RCPTS_ID = ARP_CASH_RCPT
				FOR XML path('')
				) AS VARCHAR(100)) AS Pay_Methods
		,ARP_DATE
		,cast((
				SELECT RCPT_PAY_METHODS + ' '
				FROM RCPT_NON_CASH
				WHERE CASH_RCPTS_ID = ARD_CASH_RCPT
				FOR XML path('')
				) AS VARCHAR(100)) AS Pay_Methods_Deposit
	FROM AR_PAYMENTS
	LEFT JOIN AR_DEPOSIT_ITEMS ON AR_DEPOSIT_ITEMS_ID = ARP_DEPOSIT_ITEM
	LEFT JOIN AR_DEPOSITS ON AR_DEPOSITS_ID = ARDI_DEPOSIT
	WHERE ARP_TERM = @TERM
		AND ARP_LOCATION = 'MAIN'
		AND (ARP_DATE <= @STAT_DATE)
	)
	,T4 (
	STUDENT_ID
	,SPONSORSHIP
	,SPONSOR_APPLIED
	)
AS (
	SELECT SPNP_PERSON_ID
		,SPNP_SPONSORSHIP
		,SPONSORED_PERSON_ADDDATE
	FROM SPONSORED_PERSON_LS AA
	LEFT JOIN SPONSORED_PERSON BB ON AA.SPONSORED_PERSON_ID = BB.SPONSORED_PERSON_ID
	WHERE SPNP_TERMS = @TERM
	)
SELECT STC_PERSON_ID AS 'Student ID'
	,Program
	,AAL
	,SPONSORSHIP
	,SPONSOR_APPLIED
    ,IMMIGRATION_STATUS
	,STNT
	,STNT_Date
	,ARP_AMT AS 'Pay Amt'
	,ARP_DATE AS 'Pay Date'
	,ARP_TERM
	,Pay_Methods
	,Pay_Methods_Deposit
FROM T1
LEFT JOIN T3 ON T1.STC_PERSON_ID = T3.ARP_PERSON_ID
LEFT JOIN T4 ON T1.STC_PERSON_ID = T4.STUDENT_ID
ORDER BY SPONSORSHIP DESC
    """
    query = pd.read_sql(query, cnxn)
    return(query)

def xstl_query_term_level_campus(term: str, 
                      number: str, 
                      cnxn: pyodbc.connect) -> pd.DataFrame:
    """
    This query pulls the exact same information as a XSTL report

    Args: 
        term (str): Term of interest
        level (str): Academic level of interest (PS set as default)
        campus (str): Campus of interest (MAIN set as defaul)

    Returns: 
        query (str): Output raw query to be passed through connection

    Example usage: 
        utils.xstl_query_term_level_campus(term = '2022F',
                                            level = 'PS', 
                                            campus = 'MAIN')
    """

    query = f"""
    DECLARE @TERM AS VARCHAR(10) = '{term}';
    DECLARE @STAT_DATE AS DATETIME = DATEADD(YEAR,  -{number}, GETDATE());

    SELECT STC_PERSON_ID AS student_id
        ,FIRST_NAME as first_name
        ,LAST_NAME as last_name
        ,BIRTH_DATE as birth_date
        ,GENDER as gender
        ,STC_ACAD_LEVEL as acad_level
        ,IMMIGRATION_STATUS AS imm_status
        ,CITY AS city
        ,ADDRESS.ZIP AS postal_code
        ,SCS_LOCATION AS location
        ,SUBSTRING(STC_COURSE_NAME, 6, 4) AS program
        ,STC_SECTION_NO AS AAL
        ,STTR_STUDENT_LOAD AS current_load
        ,STTR_USER1 AS tenth_day_load
        ,STC_STATUS AS curr_status
        ,BB.STC_STATUS_DATE AS status_date
    FROM STUDENT_ACAD_CRED AA
    JOIN STC_STATUSES BB ON AA.STUDENT_ACAD_CRED_ID = BB.STUDENT_ACAD_CRED_ID
        AND POS = (
            SELECT TOP 1 POS
            FROM STC_STATUSES
            WHERE STUDENT_ACAD_CRED_ID = AA.STUDENT_ACAD_CRED_ID
                AND STC_STATUS_DATE <= @STAT_DATE
            )
    JOIN STUDENT_COURSE_SEC SCS ON SCS.STUDENT_COURSE_SEC_ID = AA.STC_STUDENT_COURSE_SEC
    JOIN STUDENT_TERMS ON STUDENT_TERMS_ID = STC_PERSON_ID + '*' + STC_TERM + '*' + STC_ACAD_LEVEL
    JOIN PERSON P ON STC_PERSON_ID = P.ID
    JOIN ADDRESS ON ADDRESS_ID = PREFERRED_ADDRESS
    WHERE STC_TERM = '{term}'
        AND STC_SUBJECT = 'CTRL'
        AND SCS_LOCATION = 'MAIN'
        AND STC_ACAD_LEVEL = 'PS'
        AND STC_STATUS IN ('A','D','N')
    ORDER BY STC_PERSON_ID
        ,STC_COURSE_NAME
    """

    return pd.read_sql(query, cnxn)