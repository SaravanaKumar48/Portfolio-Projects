-- Data Cleaning

SELECT *
FROM  layoffs; 

-- 1. Remove Duplicates
-- 2. Standarized the Data
-- 3. Null Values or blank Values
-- 4. Remove Any Columns or Rows


CREATE TABLE layoffs_staging   -- this step is used to create a new table for perfroming cleaning operations while preserving the raw data
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT*
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(       -- we are using row_numer() for removing duplicates
PARTITION BY company,industry,total_laid_off,percentage_laid_off,`date`) AS row_num  -- since date is a method so we have to use backtick``
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(      
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num  
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


SELECT *
FROM layoffs_staging
WHERE company='Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(      
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num  
FROM layoffs_staging
)
DELETE   -- THIS line does not delete and update in table
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(      
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num  
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
;

-- Standarizing Data

SELECT company,(TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company=TRIM(company);

SELECT DISTINCT(industry)
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';

SELECT *
FROM layoffs_staging2
WHERE country = 'United States.';

SELECT DISTINCT(country),TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country) -- this is alternative method for updating ,generally TRIM and TRAILING IS USED to remove elements,spaces that we want to
WHERE country LIKE 'United States';

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '' ;

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb';

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company    
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL
;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company=t2.company 
SET t1.industry=t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL ;


DELETE   -- We are Deleting this beacuse both column have null value so we cannot able to perfrom operations its like useless data
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2 -- Now we remove the row_num column because its no longer needed after removing duplicate data 
DROP COLUMN row_num;