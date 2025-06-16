-- data resource is PT Bursa Efek Indonesia

SELECT *
FROM db_projects.f_d_and_r3;

CREATE table db_projects.f_d_staging1
LIKE db_projects.f_d_and_r3;

create table back_up_stage5
like db_projects.back_up_stage3;

INSERT f_d_staging1
select * from db_projects.f_d_and_r3;

insert back_up_stage5
select * from db_projects.back_up_stage3;

select * from db_projects.f_d_staging1;
select FS_Date from db_projects.f_d_staging1 limit 5;
update db_projects.f_d_staging1
set FS_Date = str_to_date(FS_Date, '%m/%d/%y');
select FS_Date from db_projects.f_d_staging1;

alter table db_projects.f_d_staging1
modify column FS_Date date;

select * from db_projects.f_d_staging1;

select Stock_Name, 'Code', FS_Date, Assets, Liabilities, Equity, Sales, Profit_Period, Profit_owner, EBT, EPS, Book_Value, P_E_Ratio, Price_BV, D_E_Ratio, ROA, ROE, NPM,
	row_number() OVER (
    PARTITION BY Stock_Name, 'Code', FS_Date, Assets, Liabilities, Equity, Sales, Profit_Period, Profit_owner, EBT, EPS, Book_Value, P_E_Ratio, Price_BV, D_E_Ratio, ROA, ROE, NPM) as row_num
from db_projects.f_d_staging1;

select *
from (
	select Stock_Name, 'Code', FS_Date, Assets, Liabilities, Equity, Sales, Profit_Period, Profit_owner, EBT, EPS, Book_Value, P_E_Ratio, Price_BV, D_E_Ratio, ROA, ROE, NPM,
	row_number() OVER (
    PARTITION BY Stock_Name, 'Code', FS_Date, Assets, Liabilities, Equity, Sales, Profit_Period, Profit_owner, EBT, EPS, Book_Value, P_E_Ratio, Price_BV, D_E_Ratio, ROA, ROE, NPM) as row_num
from db_projects.f_d_staging1
) duplicates
where row_num > 1;

select * from db_projects.f_d_staging1;

CREATE TABLE db_projects.f_d_staging3
like db_projects.f_d_staging1;

alter table db_projects.f_d_staging3
add row_num INT;

select * from db_projects.f_d_staging3;

INSERT INTO `db_projects`.`f_d_staging3`
(`NO`,
`Sector`,
`Sub_Industry`,
`Code`,
`Stock_Name`,
`FS_Date`,
`Fiscal_Year_End`,
`Assets`,
`Liabilities`,
`Equity`,
`Sales`,
`EBT`,
`Profit_Period`,
`Profit_owner`,
`EPS`,
`Book_Value`,
`P_E_Ratio`,
`Price_BV`,
`D_E_Ratio`,
`ROA`,
`ROE`,
`NPM`,
`row_num`)
SELECT `NO`,
`Sector`,
`Sub_Industry`,
`Code`,
`Stock_Name`,
`FS_Date`,
`Fiscal_Year_End`,
`Assets`,
`Liabilities`,
`Equity`,
`Sales`,
`EBT`,
`Profit_Period`,
`Profit_owner`,
`EPS`,
`Book_Value`,
`P_E_Ratio`,
`Price_BV`,
`D_E_Ratio`,
`ROA`,
`ROE`,
`NPM`,
		ROW_NUMBER() OVER (
			PARTITION BY Sector, Sub_industry, Stock_Name, 'Code', FS_Date, Assets, Liabilities, Equity, Sales, Profit_Period, Profit_owner, EBT, EPS, Book_Value, P_E_Ratio, Price_BV, D_E_Ratio, ROA, ROE, NPM
			) AS row_num
	FROM 
		db_projects.f_d_staging1;
select * from db_projects.f_d_staging3;    

DELETE FROM db_projects.f_d_staging3
WHERE row_num >= 2;

SET SQL_SAFE_UPDATES = 0;

select * from db_projects.f_d_staging3;

SELECT * FROM db_projects.f_d_staging3
where row_num >=2;

select distinct Stock_Name
from db_projects.f_d_staging3
order by Stock_Name;

select *
from db_projects.f_d_staging3
where Stock_name = '-'
or Stock_Name = ''
order by Stock_Name;

update db_projects.f_d_staging3
set Stock_Name = NULL
where Stock_Name = '';

select * from db_projects.f_d_staging3
where Stock_Name = '_';

select distinct Sector
from db_projects.f_d_staging3
order by Sector;

update db_projects.f_d_staging3
set Sub_Industry = NULL
where Sub_Industry = '';

update db_projects.f_d_staging3
set `Code` = NULL
where `Code` = '-';

update db_projects.f_d_staging3
set FS_Date = NULL
where FS_Date = '-';

select * from db_projects.f_d_staging3
where FS_Date = '-';

select FS_Date from db_projects.f_d_staging3;

update db_projects.f_d_staging3
set Assets = NULL
where Assets = '-';

update db_projects.f_d_staging3
set Liabilities = NULL
where Liabilities = '-';

update db_projects.f_d_staging3
set Equity = NULL
where Equity = '-';

update db_projects.f_d_staging3
set Sales = NULL
where Sales = '-';

update db_projects.f_d_staging3
set EBT = NULL
where EBT = '-';

update db_projects.f_d_staging3
set Profit_Period = NULL
where Profit_Period = '-';

update db_projects.f_d_staging3
set Profit_owner = NULL
where Profit_owner = '-';

update db_projects.f_d_staging3
set EPS = NULL
where EPS = '-';

update db_projects.f_d_staging3
set Book_Value = NULL
where Book_Value = '-';

update db_projects.f_d_staging3
set P_E_Ratio = NULL
where P_E_Ratio = '-';

update db_projects.f_d_staging3
set Price_BV = NULL
where Price_BV = '-';

update db_projects.f_d_staging3
set D_E_Ratio = NULL
where D_E_Ratio = '-';

update db_projects.f_d_staging3
set ROA = NULL
where ROA = '-';

update db_projects.f_d_staging3
set ROE = NULL
where ROE = '-';

update db_projects.f_d_staging3
set NPM = NULL
where NPM = '-';

UPDATE db_projects.f_d_staging3 t1
JOIN db_projects.f_d_staging2 t2
ON t1.Stock_Name = t2.Stock_Name
SET t1.Equity = t2.Equity
WHERE t1.Equity IS NULL
AND t2.Equity IS NOT NULL;

select * from db_projects.f_d_staging3;

select distinct Stock_Name
from db_projects.f_d_staging3
order by Stock_Name;
-- not work evectively, before execute cek dulu karakter nya apa jadi bisa tau apakah pake trim atau left atau replace
update db_projects.f_d_staging3
set Assets = trim('' from Assets);

update db_projects.f_d_staging3
set Liabilities = trim(' ' from Liabilities);

update db_projects.f_d_staging3
set Equity = trim(' ' from Equity);

update db_projects.f_d_staging3
set Sales = trim(' ' from Sales);

update db_projects.f_d_staging3
set EBT = trim(' ' from EBT);

update db_projects.f_d_staging3
set Profit_Period = trim(' ' from Profit_Period);

update db_projects.f_d_staging3
set Profit_owner = trim(' ' from Profit_owner);

update db_projects.f_d_staging3
set EPS = trim(' ' from EPS);

update db_projects.f_d_staging3
set Book_Value = trim(' ' from Book_Value);

update db_projects.f_d_staging3
set P_E_Ratio = trim(' ' from P_E_Ratio);

update db_projects.f_d_staging3
set Price_BV = trim(' ' from Price_BV);

update db_projects.f_d_staging3
set D_E_Ratio = trim(' ' from D_E_Ratio);

update db_projects.f_d_staging3
set ROA = trim(' ' from ROA);

update db_projects.f_d_staging3
set ROE = TRIM(' ' from ROE);

update db_projects.f_d_staging3
set NPM = trim(' ' from NPM);

create table back_up_stage3
like db_projects.f_d_staging3;

insert db_projects.back_up_stage3
select * from db_projects.f_d_staging3;

alter table db_projects.f_d_staging3
modify column Assets INT;
-- sama seperti yang dibawah
select * from db_projects.back_up_stage3
where Assets = 'NA';

SELECT COUNT(*) FROM db_projects.back_up_stage3 WHERE Assets = 0;
SELECT COUNT(*) FROM db_projects.back_up_stage3 WHERE Assets IS NULL;
-- untuk melihat karakter apa saja pada nilai, agar mempermudah replace
SELECT *
FROM db_projects.back_up_stage3
WHERE Assets REGEXP '[^0-9]';
-- solve not work
UPDATE db_projects.back_up_stage3
SET Assets = REPLACE(REPLACE(Assets, '.', ''), ',', '.')
WHERE Assets IS NOT NULL;
update db_projects.back_up_stage3
set Assets = trim('.' from Assets);
-- solve not work
alter table db_projects.back_up_stage3
add Assets1 varchar(255);
-- solve not work
INSERT INTO back_up_stage3(Assets1)
SELECT f_d_staging1.Assets
FROM f_d_staging1
JOIN back_up_stage3 on f_d_staging1.Assets = back_up_stage3.Assets1;

select Assets from db_projects.back_up_stage3;
-- solve not work
INSERT INTO `db_projects`.`back_up_stage3`
(`Assets1`)
SELECT Assets
	FROM 
		db_projects.f_d_staging1;
select * from back_up_stage3;
SELECT *
FROM db_projects.back_up_stage3
WHERE Liabilities='[^0-9]';
ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Liabilities_normal DECIMAL(15,2);
SELECT 
  Liabilities AS asli,
  REPLACE(REPLACE(Liabilities, '.', ''), ',', '.') AS liabilities_bersih,
  CAST(REPLACE(REPLACE(Liabilities, '.', ''), ',', '.') AS DECIMAL(15,2)) AS liabilities_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Liabilities_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Liabilities), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Liabilities IS NOT NULL AND Liabilities != '';
SET SQL_SAFE_UPDATES = 0;
select Liabilities_normal from db_projects.back_up_stage3;
select *
FROM db_projects.back_up_stage3
WHERE Liabilities = '[^0-9]';
ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Liabilities_normal DECIMAL(15,2);
SELECT 
  Liabilities AS asli,
  REPLACE(REPLACE(Liabilities, '.', ''), ',', '.') AS liabilities_bersih,
  CAST(REPLACE(REPLACE(Liabilities, '.', ''), ',', '.') AS DECIMAL(15,2)) AS liabilities_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Liabilities_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Liabilities), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Liabilities IS NOT NULL AND Liabilities != '';
SET SQL_SAFE_UPDATES = 0;
select Liabilities_normal from db_projects.back_up_stage3;

select *
FROM db_projects.back_up_stage3
Where Equity = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Equity_normal DECIMAL(15,2);
SELECT 
  Equity AS asli,
  REPLACE(REPLACE(Equity, '.', ''), ',', '.') AS Equity_bersih,
  CAST(REPLACE(REPLACE(Equity, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Equity_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Equity_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Equity), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Equity IS NOT NULL AND Equity != '';
SET SQL_SAFE_UPDATES = 0;

select Equity_normal from db_projects.back_up_stage3;

select *
FROM db_projects.back_up_stage3
Where Sales = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Sales_normal DECIMAL(15,2);
SELECT 
  Sales AS asli,
  REPLACE(REPLACE(Sales, '.', ''), ',', '.') AS Sales_bersih,
  CAST(REPLACE(REPLACE(Sales, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Sales_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Sales_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Sales), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Sales IS NOT NULL AND Sales != '';
select Sales_normal from db_projects.back_up_stage3;



/*
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

/*
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);
bisa pakai leading untuk di awal atau langsung aja replace
left untuk mengambil potongan karakter pada teks
*/

SELECT *
FROM db_projects.back_up_stage3
WHERE EBT = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN EBT_normal DECIMAL(15,2);
SELECT 
  EBT AS EBT,
  REPLACE(REPLACE(EBT, '.', ''), ',', '.') AS EBT_bersih,
  CAST(REPLACE(REPLACE(EBT, '.', ''), ',', '.') AS DECIMAL(15,2)) AS EBT_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET EBT_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM EBT), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE EBT IS NOT NULL AND EBT != '';
select EBT_normal from db_projects.back_up_stage3;

-- PROFIT_PERIOD

SELECT *
FROM db_projects.back_up_stage3
WHERE Profit_Period = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Profit_Period_normal DECIMAL(15,2);
SELECT 
  Profit_Period AS Profit_Period,
  REPLACE(REPLACE(Profit_Period, '.', ''), ',', '.') AS Profit_Period_bersih,
  CAST(REPLACE(REPLACE(Profit_Period, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Profit_Period_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Profit_Period_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Profit_Period), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Profit_Period IS NOT NULL AND Profit_Period != '';
select Profit_Period_normal from db_projects.back_up_stage3;

-- Profit_owner

SELECT *
FROM db_projects.back_up_stage3
WHERE Profit_owner = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Profit_owner_normal DECIMAL(15,2);
SELECT 
  Profit_owner AS Profit_owner,
  REPLACE(REPLACE(Profit_owner, '.', ''), ',', '.') AS Profit_owner_bersih,
  CAST(REPLACE(REPLACE(Profit_owner, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Profit_owner_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Profit_owner_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Profit_owner), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Profit_owner IS NOT NULL AND Profit_owner != '';
select Profit_owner_normal from db_projects.back_up_stage3;

-- EPS
SELECT *
FROM db_projects.back_up_stage3
WHERE EPS = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN EPS_normal DECIMAL(15,2);
SELECT 
  EPS AS EPS,
  REPLACE(REPLACE(EPS, '.', ''), ',', '.') AS EPS_bersih,
  CAST(REPLACE(REPLACE(EPS, '.', ''), ',', '.') AS DECIMAL(15,2)) AS EPS_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET EPS_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM EPS), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE EPS IS NOT NULL AND EPS != '';
select EPS_normal from db_projects.back_up_stage3;

-- Book_Value

SELECT *
FROM db_projects.back_up_stage3
WHERE Book_Value = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Book_Value_normal DECIMAL(15,2);
SELECT 
  Book_Value AS Book_Value,
  REPLACE(REPLACE(Book_Value, '.', ''), ',', '.') AS Book_Value_bersih,
  CAST(REPLACE(REPLACE(Book_Value, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Book_Value_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Book_Value_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Book_Value), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Book_Value IS NOT NULL AND Book_Value != '';
select Book_Value_normal from db_projects.back_up_stage3;

-- P_E_Ratio
SELECT *
FROM db_projects.back_up_stage3
WHERE P_E_Ratio = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN P_E_Ratio_normal DECIMAL(15,2);
SELECT 
  P_E_Ratio AS P_E_Ratio,
  REPLACE(REPLACE(P_E_Ratio, '.', ''), ',', '.') AS P_E_Ratio_bersih,
  CAST(REPLACE(REPLACE(P_E_Ratio, '.', ''), ',', '.') AS DECIMAL(15,2)) AS P_E_Ratio_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET P_E_Ratio_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM P_E_Ratio), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE P_E_Ratio IS NOT NULL AND P_E_Ratio != '';
select P_E_Ratio_normal from db_projects.back_up_stage3;

-- Price_BV
SELECT *
FROM db_projects.back_up_stage3
WHERE Price_BV = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN Price_BV_normal DECIMAL(15,2);
SELECT 
  Price_BV AS Price_BV,
  REPLACE(REPLACE(Price_BV, '.', ''), ',', '.') AS Price_BV_bersih,
  CAST(REPLACE(REPLACE(Price_BV, '.', ''), ',', '.') AS DECIMAL(15,2)) AS Price_BV_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET Price_BV_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM Price_BV), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE Price_BV IS NOT NULL AND Price_BV != '';
select Price_BV_normal from db_projects.back_up_stage3;

-- D_E_Ratio

SELECT *
FROM db_projects.back_up_stage3
WHERE D_E_Ratio = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN D_E_Ratio_normal DECIMAL(15,2);
SELECT 
  D_E_Ratio AS D_E_Ratio,
  REPLACE(REPLACE(D_E_Ratio, '.', ''), ',', '.') AS D_E_Ratio_bersih,
  CAST(REPLACE(REPLACE(D_E_Ratio, '.', ''), ',', '.') AS DECIMAL(15,2)) AS D_E_Ratio_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET D_E_Ratio_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM D_E_Ratio), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE D_E_Ratio IS NOT NULL AND D_E_Ratio != '';
select D_E_Ratio_normal from db_projects.back_up_stage3;

select ROA from db_projects.back_up_stage3;

-- ROA
SELECT *
FROM db_projects.back_up_stage3
WHERE ROA = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN ROA_normal DECIMAL(15,2);
SELECT 
  ROA AS ROA,
  REPLACE(REPLACE(ROA, '.', ''), ',', '.') AS ROA_bersih,
  CAST(REPLACE(REPLACE(ROA, '.', ''), ',', '.') AS DECIMAL(15,2)) AS ROA_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET ROA_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM ROA), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE ROA IS NOT NULL AND ROA != '';
select ROA_normal from db_projects.back_up_stage3;

-- ROE
SELECT *
FROM db_projects.back_up_stage3
WHERE ROE = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN ROE_normal DECIMAL(15,2);
SELECT 
  ROE AS ROE,
  REPLACE(REPLACE(ROE, '.', ''), ',', '.') AS ROE_bersih,
  CAST(REPLACE(REPLACE(ROE, '.', ''), ',', '.') AS DECIMAL(15,2)) AS ROE_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET ROE_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM ROE), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE ROE IS NOT NULL AND ROE != '';
select ROE_normal from db_projects.back_up_stage3;

-- NPM

SELECT *
FROM db_projects.back_up_stage3
WHERE NPM = '[^0-9]';

ALTER TABLE db_projects.back_up_stage3
ADD COLUMN NPM_normal DECIMAL(15,2);
SELECT 
  NPM AS NPM,
  REPLACE(REPLACE(NPM, '.', ''), ',', '.') AS NPM_bersih,
  CAST(REPLACE(REPLACE(NPM, '.', ''), ',', '.') AS DECIMAL(15,2)) AS NPM_normal
FROM back_up_stage3;
UPDATE db_projects.back_up_stage3
SET NPM_normal = CAST(
    REPLACE(REPLACE(TRIM(BOTH '"' FROM NPM), '.', ''), ',', '.') AS DECIMAL(15,2)
)
WHERE NPM IS NOT NULL AND NPM != '';
select NPM_normal from db_projects.back_up_stage3;

CREATE table db_projects.back_up_stage4
LIKE db_projects.back_up_stage3;

INSERT back_up_stage4
select * from db_projects.back_up_stage3;

select * from db_projects.back_up_stage4;

DELETE FROM db_projects.back_up_stage3
WHERE 
    Liabilities_normal IS NULL OR
    Equity_normal IS NULL OR
    Sales_normal IS NULL OR
    EBT_normal IS NULL OR
    Profit_Period_normal IS NULL OR
    Profit_owner_normal IS NULL OR
    EPS_normal IS NULL OR
    Book_Value_normal IS NULL OR
    P_E_Ratio_normal IS NULL OR
    Price_BV_normal IS NULL OR
    D_E_Ratio_normal IS NULL OR
    ROA_normal IS NULL OR
    ROE_normal IS NULL OR
    NPM_normal IS NULL;

select * from db_projects.back_up_stage3;
SET Nomor := 0;

UPDATE db_projects.back_up_stage3
SET row_num = (Nomor = Nomor + 1)
ORDER BY row_num;

alter table back_up_stage3
add Nomor INt;

select * from db_projects.back_up_stage3;SELECT *
FROM db_projects.back_up_stage3
ORDER BY Nomor ASC;

alter table back_up_stage3
drop Column `No`;

select * from db_projects.back_up_stage3;
-- back_up stage 4
alter table back_up_stage3
drop Column Fiscal_Year_End,
drop Column Liabilities,
drop Column Equity,
drop Column Sales,
drop Column EBT,
drop Column Profit_Period,
drop Column Profit_owner,
drop Column EPS,
drop Column Book_Value,
drop Column P_E_Ratio,
drop Column ROA,
drop Column ROE,
drop Column NPM,
drop Column row_num,
drop Column Assets1,
drop Column Nomor;

select * from db_projects.back_up_stage3;

-- outlier values back_up stage 5

select distinct sector
from db_projects.back_up_stage3
order by sector;

alter table back_up_stage3
add column is_outlier_PE_Ratio decimal;

UPDATE back_up_stage3
SET is_outlier_PE_Ratio = CASE 
    WHEN sector = 'Basic Materials'            AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 30) THEN 1
    WHEN sector = 'Consumer Cyclicals'         AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 35) THEN 1
    WHEN sector = 'Consumer Non-Cyclicals'     AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 30) THEN 1
    WHEN sector = 'Energy'                     AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 25) THEN 1
    WHEN sector = 'Financials'                 AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 20) THEN 1
    WHEN sector = 'Healthcare'                 AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 40) THEN 1
    WHEN sector = 'Industrials'                AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 30) THEN 1
    WHEN sector = 'Infrastructures'            AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 25) THEN 1
    WHEN sector = 'Properties & Real Estate'   AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 20) THEN 1
    WHEN sector = 'Technology'                 AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 60) THEN 1
    WHEN sector = 'Transportation & Logistic'  AND (P_E_Ratio_normal < 0 OR P_E_Ratio_normal > 30) THEN 1
    ELSE 0
END;
SET SQL_SAFE_UPDATES = 0;
UPDATE back_up_stage3
SET is_outlier_DE = CASE 
    WHEN sector = 'Financials'                AND (D_E_Ratio_normal < 0.5 OR D_E_Ratio_normal > 10.0) THEN 1
    WHEN sector = 'Technology'                AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.5) THEN 1
    WHEN sector = 'Consumer Cyclicals'        AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.5) THEN 1
    WHEN sector = 'Consumer Non-Cyclicals'    AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.0) THEN 1
    WHEN sector = 'Energy'                    AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.5) THEN 1
    WHEN sector = 'Healthcare'                AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.0) THEN 1
    WHEN sector = 'Industrials'               AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.5) THEN 1
    WHEN sector = 'Infrastructures'           AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 3.0) THEN 1
    WHEN sector = 'Properties & Real Estate'  AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 3.5) THEN 1
    WHEN sector = 'Basic Materials'           AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 2.5) THEN 1
    WHEN sector = 'Transportation & Logistic' AND (D_E_Ratio_normal < 0.0 OR D_E_Ratio_normal > 3.0) THEN 1
    ELSE 0
END;
ALTER TABLE back_up_stage3
ADD COLUMN is_outlier_ROE INT;

UPDATE back_up_stage3
SET is_outlier_ROE = CASE 
    WHEN sector = 'Financials'                AND (ROE_normal < 5 OR ROE_normal > 25) THEN 1
    WHEN sector = 'Technology'                AND (ROE_normal < 0 OR ROE_normal > 30) THEN 1
    WHEN sector = 'Consumer Cyclicals'        AND (ROE_normal < 0 OR ROE_normal > 25) THEN 1
    WHEN sector = 'Consumer Non-Cyclicals'    AND (ROE_normal < 0 OR ROE_normal > 20) THEN 1
    WHEN sector = 'Energy'                    AND (ROE_normal < 0 OR ROE_normal > 20) THEN 1
    WHEN sector = 'Healthcare'                AND (ROE_normal < 0 OR ROE_normal > 25) THEN 1
    WHEN sector = 'Industrials'               AND (ROE_normal < 0 OR ROE_normal > 25) THEN 1
    WHEN sector = 'Infrastructures'           AND (ROE_normal < 0 OR ROE_normal > 20) THEN 1
    WHEN sector = 'Properties & Real Estate'  AND (ROE_normal < 0 OR ROE_normal > 15) THEN 1
    WHEN sector = 'Basic Materials'           AND (ROE_normal < 0 OR ROE_normal > 20) THEN 1
    WHEN sector = 'Transportation & Logistic' AND (ROE_normal < 0 OR ROE_normal > 20) THEN 1
    ELSE 0
END;
SET SQL_SAFE_UPDATES = 0;
ALTER TABLE back_up_stage3
ADD COLUMN is_outlier_ROA INT;

UPDATE back_up_stage3
SET is_outlier_ROA = CASE 
    WHEN sector = 'Financials'                AND (ROA_normal < 0.5 OR ROA_normal > 5.0) THEN 1
    WHEN sector = 'Technology'                AND (ROA_normal < 0.0 OR ROA_normal > 15.0) THEN 1
    WHEN sector = 'Consumer Cyclicals'        AND (ROA_normal < 0.0 OR ROA_normal > 12.0) THEN 1
    WHEN sector = 'Consumer Non-Cyclicals'    AND (ROA_normal < 0.0 OR ROA_normal > 10.0) THEN 1
    WHEN sector = 'Energy'                    AND (ROA_normal < 0.0 OR ROA_normal > 10.0) THEN 1
    WHEN sector = 'Healthcare'                AND (ROA_normal < 0.0 OR ROA_normal > 12.0) THEN 1
    WHEN sector = 'Industrials'               AND (ROA_normal < 0.0 OR ROA_normal > 10.0) THEN 1
    WHEN sector = 'Infrastructures'           AND (ROA_normal < 0.0 OR ROA_normal > 8.0) THEN 1
    WHEN sector = 'Properties & Real Estate'  AND (ROA_normal < 0.0 OR ROA_normal > 7.0) THEN 1
    WHEN sector = 'Basic Materials'           AND (ROA_normal < 0.0 OR ROA_normal > 10.0) THEN 1
    WHEN sector = 'Transportation & Logistic' AND (ROA_normal < 0.0 OR ROA_normal > 8.0) THEN 1
    ELSE 0
END;

ALTER TABLE back_up_stage3
ADD COLUMN is_outlier_NPM INT;

UPDATE back_up_stage3
SET is_outlier_NPM = CASE 
    WHEN sector = 'Financials'                AND (NPM_normal < 10 OR NPM_normal > 40) THEN 1
    WHEN sector = 'Technology'                AND (NPM_normal < 0 OR NPM_normal > 30) THEN 1
    WHEN sector = 'Consumer Cyclicals'        AND (NPM_normal < 0 OR NPM_normal > 20) THEN 1
    WHEN sector = 'Consumer Non-Cyclicals'    AND (NPM_normal < 0 OR NPM_normal > 15) THEN 1
    WHEN sector = 'Energy'                    AND (NPM_normal < 0 OR NPM_normal > 20) THEN 1
    WHEN sector = 'Healthcare'                AND (NPM_normal < 0 OR NPM_normal > 25) THEN 1
    WHEN sector = 'Industrials'               AND (NPM_normal < 0 OR NPM_normal > 15) THEN 1
    WHEN sector = 'Infrastructures'           AND (NPM_normal < 0 OR NPM_normal > 15) THEN 1
    WHEN sector = 'Properties & Real Estate'  AND (NPM_normal < 0 OR NPM_normal > 25) THEN 1
    WHEN sector = 'Basic Materials'           AND (NPM_normal < 0 OR NPM_normal > 20) THEN 1
    WHEN sector = 'Transportation & Logistic' AND (NPM_normal < 0 OR NPM_normal > 15) THEN 1
    ELSE 0
END;


