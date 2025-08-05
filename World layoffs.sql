select *
from layoffs;

-- 1. remove duplicates
-- 2. standardized the data
-- 3. null values or blank values
-- 4. remove any columns or rows especially if blank

create table layoffs_staging
like layoffs;

select *
from layoffs_staging;

insert into  layoffs_staging
select *
from layoffs;

-- why did we do this? that's because we are going to be changing the data alot, incase we make some kind of mistake we will know we still have our
-- raw data which is the layoffs

select *,
row_number() over(partition by company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging;


with CTE_duplicate as
(
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country,
funds_raised_millions) as row_num
from layoffs_staging
)
select *
from CTE_duplicate
where row_num > 1;

select *
from layoffs_staging
where company like 'casper';

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2;

insert into  layoffs_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, 
country,
funds_raised_millions) as row_num
from layoffs_staging;

select *
from layoffs_staging2
where `row_num`> 1;

delete
from layoffs_staging2
where `row_num`> 1;

select *
from layoffs_staging2;

-- standardizing data

select distinct trim(company)
from layoffs_staging2;

select company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'crypto%';

select distinct location
from layoffs_staging2
order by 1;
-- looks good.. lets move to country

select distinct country
from layoffs_staging2
order by 1;

select *
from layoffs_staging2
where country like 'united states.';

update layoffs_staging2
set country = 'United States'
where country like 'united states.';

select *
from layoffs_staging2;

select `date`
from layoffs_staging2;

-- the date is in a text format.. we have to change it to a time series

select date_format(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = date_format(`date`,'%m/%d/%Y');

-- the date was in a text format, i converted it to a time series but i made a mistake, i tried converting it back to the text series
-- and i successfully did which is the to commands above. now i want to convert it to a time series

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select *
from layoffs_staging2;

alter table layoffs_staging2
modify column `date` date;

select distinct stage
from layoffs_staging2;

-- the stage column is okay

-- null and blank data

select *
from layoffs_staging2
where total_laid_off is null;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where industry is null
or industry = '';

select *
from layoffs_staging2
where company = 'airbnb';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where industry is null;

select *
from layoffs_staging2
where industry = '';

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2
where company like 'bally%';

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num