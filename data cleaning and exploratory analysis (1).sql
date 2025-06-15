-- DATA CLEANING and exploratory data analysis in sql


# removing duplicates
CREATE TABLE `layoffs_1` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL
  

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into `layoffs_1`
select * from layoffs;



select * ,
ROW_NUMBER() OVER(PARTITION BY company,location,industry,date,stage,country,funds_raised_millions,total_laid_off,percentage_laid_off ) as row_num
from layoffs_1;


WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, date, stage, country, funds_raised_millions, total_laid_off, percentage_laid_off
           ) AS row_num
    FROM layoffs_1
)
SELECT *
FROM duplicate_cte
where row_num>1;


select * from layoffs_1
where company='Casper';

CREATE TABLE `layoffs_2`(
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from `layoffs_2`;

insert into  `layoffs_2`
SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry,`date`,stage, country, funds_raised_millions, total_laid_off, percentage_laid_off
           ) AS row_num
    FROM layoffs_1;


select * from `layoffs_2`
where row_num>1;

delete from `layoffs_2`
where row_num>1;

SET SQL_SAFE_UPDATES = 0;






DELETE FROM `layoffs_2`
WHERE row_num > 1;

select * from `layoffs_2`;

# standerdising the data

update layoffs_2 
set company=trim(company);
select distinct industry from`layoffs_2`
order by 1;

update `layoffs_2`
set industry='crypto'
where industry like 'crypto%';
select distinct country from `layoffs_2`
order by 1;

update `layoffs_2`
set country='United States'
where country like 'United States%';

update `layoffs_2`
set `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

ALTER TABLE `layoffs_2`
MODIFY column `date` DATE;

# REOMVING NULL VALUES
select * from `layoffs_2`
where industry is NULL OR industry='';

select * from `layoffs_2`
where company like 'carvana';

select t1.industry,t2.industry  from `layoffs_2` as t1
join `layoffs_2` as t2
on t1.company=t2.company 
where (t1.industry is null or t1.industry='') and (t2.industry is not null)  ;

update `layoffs_2`
set industry=null
where industry='';

update `layoffs_2` as t1
join `layoffs_2` as t2
on t1.company=t2.company 
set t1.industry=t2.industry
where (t1.industry is null ) and (t2.industry is not null);

delete from `layoffs_2`
where total_laid_off is NULL and percentage_laid_off is null;

select * from `layoffs_2`
where total_laid_off is NULL and percentage_laid_off is null;

# REMOVING COLUMNS
alter table `layoffs_2`
drop column row_num;

select * from `layoffs_2`;

select max(total_laid_off),max(percentage_laid_off) from `layoffs_2`;

select `date`,sum(total_laid_off) from `layoffs_2`
group by `date`
order by 1 desc;

select company,sum(total_laid_off) from `layoffs_2`
group by company
order by 2 desc;

select industry,sum(total_laid_off) from `layoffs_2`
group by industry
order by 2 desc;

select `date`,country,sum(total_laid_off) from `layoffs_2`
group by country,`date`
order by 3 desc;

select year(`date`),sum(total_laid_off) from `layoffs_2`
group by year(`date`)
order by 1 desc;

select substring(`date`,1,7) as 'month',sum(total_laid_off) as'lay_off' from `layoffs_2`
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)

order by 1 asc;
# aggregate window function(sum,count,avg,etc)
WITH rolling_total AS(
select substring(`date`,1,7) as `month`,sum(total_laid_off) as lay_off from `layoffs_2`
where substring(`date`,1,7) is not null
group by substring(`date`,1,7)

order by 1 asc
)
SELECT `month` ,lay_off,sum(lay_off) over(order by `month` ) as Rolling_total
FROM rolling_total;

select year(`date`) ,company,sum(total_laid_off) from `layoffs_2`
group by year(`date`),company;

with company_layoff(company,years,total_layoff)as(
select company,year(`date`),sum(total_laid_off) from `layoffs_2`
group by year(`date`),company)
,company_year_rank as (
select *, dense_rank() over(partition by years order by total_layoff desc) as ranking
from company_layoff
where years is not null
order by ranking asc)

select * from company_year_rank
where ranking>=5;















