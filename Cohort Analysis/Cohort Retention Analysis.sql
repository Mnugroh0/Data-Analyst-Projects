
-- IMPORT DATA (LOAD DATA LOCAL INFILE)

-- ----------------------------------------------------------------------
-- CREATING TABLE 
CREATE TABLE Online_Retail (
	InvoiceNo varchar(255),
    StockCode varchar(255),
    `Description` varchar(255),
    Quantity int, 
    InvoiceDate date,
    UnitPrice double,
    CustomerID varchar(255),
    Country varchar(255)
);

-- ----------------------------------------------------------------------
-- to allow this methods input works, need to set LOCAL_INFILE = YES
SET GLOBAL local_infile = 1;

-- ----------------------------------------------------------------------
-- IMPORT FROM CSV
LOAD DATA LOCAL INFILE 'D:/DATA SCIENCE/PROJECT/Cohort Retention/Online Retail.csv' 
INTO Table Online_Retail -- from previous table that already created
FIELDS TERMINATED BY ';' -- the separator used in csv
IGNORE 1 rows; -- ignore header


-- ----------------------------------------------------------------------
-- INFO DATA 
/* 
8 columns
541909 rows
*/



-- ----------------------------------------------------------------------
-- 								CLEANING
-- ----------------------------------------------------------------------
-- Delete CustomerID with 0 value
CREATE TEMPORARY TABLE online_retail AS 
SELECT *
FROM online_retail
WHERE CustomerID != 'NULL'; -- Record without 0 value: 406829


-- Delete Quantity / UnitPrice less than 0
CREATE TEMPORARY TABLE qp_greater_than_zero AS
SELECT *
FROM online_retail
WHERE Quantity > 0 AND UnitPrice > 0; -- Record values with greater than 0: 397884


-- Check if there is any duplicate? 
CREATE TEMPORARY TABLE check_duplicate AS
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY InvoiceNo, StockCode, Quantity ORDER BY InvoiceDate) AS col_duplicate
FROM qp_greater_than_zero; 

CREATE TEMPORARY TABLE clean_data AS
SELECT *
FROM check_duplicate
WHERE col_duplicate = 1; -- Value without duplicate: 392669



-- ----------------------------------------------------------------------
-- 							COHORT ANALYSIS
-- ----------------------------------------------------------------------
-- Add Revenue
CREATE TEMPORARY TABLE online_retail_main
SELECT 
	Quantity*UnitPrice AS Revenue
FROM clean_data;










-- ----------------------------------------------------------------------
-- 							COHORT ANALYSIS
-- ----------------------------------------------------------------------


-- Check Total Customer
SELECT 
	COUNT(DISTINCT(CustomerID)) AS Total_Customer
FROM online_retail_main; -- Total Customers: 4338



-- ----------------------------------------------------------------------
/* NOTE

The main reason doing cohort analysis is to know 
the customer behavior, patterns, and trends.

Cohort is group of people with something in common
*/
-- ----------------------------------------------------------------------



-- ----------------------------------------------------------------------
CREATE TEMPORARY TABLE Cohort AS
SELECT
    CustomerID,
    MIN(InvoiceDate) AS First_Purchase,
    IFNULL(DATE_FORMAT(MIN(InvoiceDate), '%Y-%m-01'), 'N/A') AS Cohort_Date
FROM online_retail_main
GROUP BY CustomerID;
 
 
-- ----------------------------------------------------------------------
-- Create Cohort Index
CREATE TEMPORARY TABLE Cohort_Retention AS
SELECT 
	tbl2.*,
    (Year_Diff_Column*12 + Month_Diff_Column + 1) AS Cohort_Index_Column
FROM
	(
    SELECT 
		tbl1.*,
		(Invoice_Year - Cohort_Year) AS Year_Diff_Column,
		(Invoice_Month - Cohort_Month) AS Month_Diff_Column
	FROM
		(
		
		SELECT 
			retail.*,
            Cohort.Cohort_Date,
			YEAR(retail.InvoiceDate) AS Invoice_Year,
			MONTH(retail.InvoiceDate) AS Invoice_Month,
			YEAR(Cohort.Cohort_Date) AS Cohort_Year,
			MONTH(Cohort.Cohort_Date) AS Cohort_Month
		FROM online_retail_main AS retail
		LEFT JOIN Cohort ON retail.CustomerID = Cohort.CustomerID			
		
		) AS tbl1

) AS tbl2;



-- Pivot Data to see the cohort table
CREATE TEMPORARY TABLE Pivot_Cohort
SELECT
    Cohort_Date,
    SUM(CASE WHEN Cohort_Index_Column = 1 THEN 1 ELSE 0 END) AS `1`,
    SUM(CASE WHEN Cohort_Index_Column = 2 THEN 1 ELSE 0 END) AS `2`,
    SUM(CASE WHEN Cohort_Index_Column = 3 THEN 1 ELSE 0 END) AS `3`,
    SUM(CASE WHEN Cohort_Index_Column = 4 THEN 1 ELSE 0 END) AS `4`,
    SUM(CASE WHEN Cohort_Index_Column = 5 THEN 1 ELSE 0 END) AS `5`,
    SUM(CASE WHEN Cohort_Index_Column = 6 THEN 1 ELSE 0 END) AS `6`,
    SUM(CASE WHEN Cohort_Index_Column = 7 THEN 1 ELSE 0 END) AS `7`,
    SUM(CASE WHEN Cohort_Index_Column = 8 THEN 1 ELSE 0 END) AS `8`,
    SUM(CASE WHEN Cohort_Index_Column = 9 THEN 1 ELSE 0 END) AS `9`,
    SUM(CASE WHEN Cohort_Index_Column = 10 THEN 1 ELSE 0 END) AS `10`,
    SUM(CASE WHEN Cohort_Index_Column = 11 THEN 1 ELSE 0 END) AS `11`,
    SUM(CASE WHEN Cohort_Index_Column = 12 THEN 1 ELSE 0 END) AS `12`,
    SUM(CASE WHEN Cohort_Index_Column = 13 THEN 1 ELSE 0 END) AS `13`
FROM (
	SELECT DISTINCT
		CustomerID,
        Cohort_Date,
        Cohort_Index_Column
	FROM Cohort_Retention
) AS tbl
GROUP BY Cohort_Date;


-- In percentage form
SELECT
    Cohort_Date,
    (`1`/`1`) * 100 AS '1',
    (`2`/`1`) * 100 AS '2',
    (`3`/`1`) * 100 AS '3',
    (`4`/`1`) * 100 AS '4',
    (`5`/`1`) * 100 AS '5',
    (`6`/`1`) * 100 AS '6',
    (`7`/`1`) * 100 AS '7',
    (`8`/`1`) * 100 AS '8',
    (`9`/`1`) * 100 AS '9',
    (`10`/`1`) * 100 AS '10',
    (`11`/`1`) * 100 AS '11',
    (`12`/`1`) * 100 AS '12',
    (`13`/`1`) * 100 AS '13'
FROM Pivot_Cohort;














