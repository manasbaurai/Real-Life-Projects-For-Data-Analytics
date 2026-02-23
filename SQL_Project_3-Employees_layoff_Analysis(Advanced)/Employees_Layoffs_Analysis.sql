-- Removing Duplicates

select * from layoffs ;

create table layoffs_staging like layoffs ;

insert into layoffs_staging 
select * from layoffs ;

select * from layoffs_staging ;

with duplicate_cte as (
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging )

select * from duplicate_cte 
where row_num > 1 ;

select * from layoffs_staging where company = "Ola" ;

create table layoffs_staging2 
like layoffs_staging ;

alter table layoffs_staging2 
add column row_num int ;

insert into layoffs_staging2 
select *, 
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging ;

select * from layoffs_staging2 ;

select * from layoffs_staging2 where row_num > 1 ;

delete from layoffs_staging2 where row_num > 1 ;

select * from layoffs_staging2 where row_num > 1 ;

-- Standardizing Data 

select company, trim(company) from layoffs_staging2 ;

update layoffs_staging2 
set company = trim(company) ;

select * from layoffs_staging2 ;

select distinct industry from layoffs_staging ;

update layoffs_staging2 
set industry = "Crypto" 
where industry like "Crypto%" ;

select distinct industry from layoffs_staging2 ;

select distinct location 
from layoffs_staging2 
order by 1 ;

select distinct country 
from layoffs_staging2 order by 1 ;

select distinct country, trim(trailing "." from country) 
from layoffs_staging2 order by 1 ;

update layoffs_staging2 
set country = trim(trailing "." from country) 
where country like "United States%" ;

select date, 
str_to_date(`date`, "%m/%d/%Y")
from layoffs_staging2 ;

update layoffs_staging2
set `date` = str_to_date(`date`, "%m/%d/%Y") ;

alter table layoffs_staging2
modify column `date` date ;


select distinct industry from layoffs_staging2 ;

select * from layoffs_staging2 
where company = "Juul" ;

update layoffs_staging2
set industry = null 
where industry = "" ;

select * from layoffs_staging2 where industry is null ;

select t1.industry, t2.industry 
from layoffs_staging2 as t1 
join layoffs_staging2 as t2
on t1.company = t2.company
where t1.industry is null and t2.industry is not null ;

update layoffs_staging2 as t1 
join layoffs_staging2 as t2
on t1.company = t2.company
set t1.industry =t2.industry
where t1.industry is null and t2.industry is not null ;

delete from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null ;

select * from layoffs_staging2 
where total_laid_off is null
and percentage_laid_off is null ;

alter table layoffs_staging2 
drop column row_num ;

-- Part 2 

select * from layoffs_staging2 ;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2 ;

select * from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc ;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc ;

select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 2 desc ;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 2 desc ;

with rolling_total as (
select substring(`date`,1,7) as `Month`, sum(total_laid_off) as total_off
from layoffs_staging2
where substring(`date`,1,7) is not null 
group by `Month` 
order by 1 asc )

select `Month`, total_off, sum(total_off) over(order by `Month`) as rolling_total
from rolling_total ;

select company, year(`date`), sum(total_laid_off)
from layoffs_staging2 
group by company, year(`date`) 
order by 3 desc ;

with company_year(company, years, total_laid_off) as (
select company, year(`date`), sum(total_laid_off)
from layoffs_staging2 
group by company, year(`date`) ),

company_year_rank as ( 
select *, dense_rank() over(partition by years order by total_laid_off desc) as ranking 
from company_year
where years is not null)

select * from company_year_rank
where ranking <= 5 ;