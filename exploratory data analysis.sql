select *
from layoffs_staging2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging2;

select *
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs_staging2;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 1 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select substring(`date`, 1, 7) as 'month', sum(total_laid_off)  
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by substring(`date`, 1, 7)
order by 1 asc;

with CTE as
(select substring(`date`, 1, 7) as 'month', sum(total_laid_off) as sum_total  
from layoffs_staging2
where substring(`date`, 1, 7) is not null
group by substring(`date`, 1, 7)
order by 1 asc)
select `month`, sum_total, sum(sum_total) over (order by `month`)
from CTE;

select company, year(`date`), sum(total_laid_off) as sum
from layoffs_staging2
group by company,year(`date`)
order by sum(total_laid_off) desc;

with CTE1 as
(select company, year(`date`) as `year`, sum(total_laid_off) as sum
from layoffs_staging2
group by company,year(`date`)),
CTE2 as 
(select *, dense_rank() over (partition by `year` order by sum desc) as `rank`
from CTE1
where `year` is not null)
select *
from CTE2
where `rank` <= 5