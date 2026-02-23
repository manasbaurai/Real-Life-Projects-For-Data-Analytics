
# 🧹 Layoffs Data Cleaning & Analysis (MySQL)

This project demonstrates a complete **SQL-based Data Cleaning and Exploratory Analysis workflow** using MySQL.  
It includes removing duplicates, standardizing data, handling null values, and performing business analysis queries.

Dataset Used in this:- [Datasets](Datasets_Used)

---

## 🧱 1. Initial Setup — Staging Table

Before making any changes, we create a staging table to preserve the original data.
```sql
SELECT * FROM layoffs;

CREATE TABLE layoffs_staging LIKE layoffs;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT * FROM layoffs_staging;
```

---

## 🔁 2. Removing Duplicates

### 2.1 Identify Duplicates Using ROW_NUMBER()
```sql
WITH duplicate_cte AS (
    SELECT *, 
        ROW_NUMBER() OVER(
            PARTITION BY company, location, industry, total_laid_off, 
            percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
    FROM layoffs_staging
)
SELECT * FROM duplicate_cte
WHERE row_num > 1;
```

### 2.2 Verify a Specific Company
```sql
SELECT * FROM layoffs_staging WHERE company = "Ola";
```

### 2.3 Create Second Staging Table with Row Number Column
```sql
CREATE TABLE layoffs_staging2 LIKE layoffs_staging;

ALTER TABLE layoffs_staging2
ADD COLUMN row_num INT;

INSERT INTO layoffs_staging2
SELECT *, 
    ROW_NUMBER() OVER(
        PARTITION BY company, location, industry, total_laid_off, 
        percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
FROM layoffs_staging;

SELECT * FROM layoffs_staging2;
```

### 2.4 Delete Duplicate Rows
```sql
SELECT * FROM layoffs_staging2 WHERE row_num > 1;

DELETE FROM layoffs_staging2 WHERE row_num > 1;

SELECT * FROM layoffs_staging2 WHERE row_num > 1;
```

---

## 🧽 3. Standardizing Data

### 3.1 Trim Whitespace from Company Names
```sql
SELECT company, TRIM(company) FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT * FROM layoffs_staging2;
```

### 3.2 Standardize Industry Names
```sql
SELECT DISTINCT industry FROM layoffs_staging;

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";

SELECT DISTINCT industry FROM layoffs_staging2;
```

### 3.3 Check Location & Country Values
```sql
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;
```

### 3.4 Fix Trailing Period in Country Names
```sql
SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";
```

### 3.5 Fix Date Format
```sql
SELECT `date`,
    STR_TO_DATE(`date`, "%m/%d/%Y")
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%Y");

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;
```

---

## 🔍 4. Handling NULL Values

### 4.1 Check Industry Column
```sql
SELECT DISTINCT industry FROM layoffs_staging2;

SELECT * FROM layoffs_staging2 WHERE company = "Juul";
```

### 4.2 Convert Blank Industry to NULL
```sql
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = "";

SELECT * FROM layoffs_staging2 WHERE industry IS NULL;
```

### 4.3 Fill NULL Industry Using Self Join
```sql
SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
```

### 4.4 Remove Rows with No Layoff Data
```sql
DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;
```

### 4.5 Drop Helper Column
```sql
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
```

---

## 📊 5. Exploratory Data Analysis (EDA)

### 5.1 Overview of Clean Data
```sql
SELECT * FROM layoffs_staging2;
```

### 5.2 Maximum Layoffs & Percentage
```sql
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;
```

### 5.3 Companies with 100% Layoffs (Sorted by Funding)
```sql
SELECT * FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
```

### 5.4 Total Layoffs by Company
```sql
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;
```

### 5.5 Total Layoffs by Date
```sql
SELECT `date`, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY `date`
ORDER BY 2 DESC;
```

### 5.6 Total Layoffs by Year
```sql
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 2 DESC;
```

---

## 📈 6. Rolling Monthly Total of Layoffs
```sql
WITH rolling_total AS (
    SELECT 
        SUBSTRING(`date`, 1, 7) AS `Month`,
        SUM(total_laid_off) AS total_off
    FROM layoffs_staging2
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `Month`
    ORDER BY 1 ASC
)
SELECT 
    `Month`,
    total_off,
    SUM(total_off) OVER(ORDER BY `Month`) AS rolling_total
FROM rolling_total;
```

---

## 🏆 7. Top 5 Companies with Most Layoffs Per Year

### 7.1 Layoffs by Company and Year
```sql
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;
```

### 7.2 Rank Companies by Layoffs Per Year (Top 5)
```sql
WITH company_year(company, years, total_laid_off) AS (
    SELECT company, YEAR(`date`), SUM(total_laid_off)
    FROM layoffs_staging2
    GROUP BY company, YEAR(`date`)
),
company_year_rank AS (
    SELECT *,
        DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
    WHERE years IS NOT NULL
)
SELECT * FROM company_year_rank
WHERE ranking <= 5;
```

---

## 🚀 Conclusion

This project demonstrates:

- Creating and managing staging tables to protect raw data
- Identifying and removing duplicate records using window functions
- Standardizing text, dates, and categorical values
- Handling NULL and blank values with self joins
- Performing exploratory analysis with aggregations and window functions

It can be extended further by connecting to **Power BI** or **Tableau** for visual dashboards.
