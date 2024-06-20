-- Запрос 1 
SELECT name, city, salary, 
	round(salary*100.0/last_value(salary) OVER (PARTITION BY city
ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) AS percent
FROM employees
ORDER BY city, salary

-- Запрос 2
SELECT name, city, salary,
	sum(salary) OVER (PARTITION BY city) AS fund,
	round(salary*100.0/sum(salary) OVER (PARTITION BY city
),0) AS perc
FROM employees
ORDER BY city, salary

-- Запрос 3
SELECT name, department, salary,
	count(id) OVER (PARTITION BY department) AS emp_cnt,
	round(avg(salary) OVER (PARTITION BY department),0) AS sal_avg,
	(round(salary*100.0/avg(salary) OVER (PARTITION BY department
),0)-100) AS diff
FROM employees
ORDER BY department, salary, id

-- Запрос 4
select
  city,
  department,
  sum(salary) as dep_salary,
  sum(sum(salary)) over (partition by city) as x,
  sum(sum(salary)) over () as y
from employees
group by city, department
order by city, department;

-- Запрос 5
select
  DISTINCT city, department,
  string_agg(name, ', ') over (partition by department) as x
from employees

-- Запрос 6

SELECT "year", "month", income, round(avg(income) OVER(
ORDER BY "year", "month" ROWS BETWEEN 1 PRECEDING AND 0 FOLLOWING), 0) AS roll_avg
FROM expenses
ORDER BY "year", "month"

-- Запрос 7
select
  year, month, income, expense,
  sum(income) over w as t_income,
  sum(expense) over w as t_expense,
  (sum(income) over w) - (sum(expense) over w) as t_profit
from expenses
window w as (
  order by year, month
  rows between unbounded preceding and current row
)
order by year, month

-- Запрос 8
SELECT id, name, department, salary, sum(salary) OVER (
PARTITION BY department ORDER BY department, salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total
FROM employees e
ORDER BY department, salary, id

-- Запрос 9
select 
  name, salary,
  cume_dist() over w as cd,
  round(percent_rank() over w::numeric, 2) as pr
from employees
window w as (order by salary)
order by salary, id

-- Запрос 10
SELECT *,
	round(cume_dist() over w::numeric, 2) as cd,
    round(percent_rank() over w::numeric, 2) as pr
FROM weather
WHERE wdate BETWEEN '2020-03-01' AND '2020-03-31'
window w as (order by wtemp)
order by wtemp DESC 
LIMIT 5

-- Запрос 11
SELECT wdate, COALESCE(wtemp, 0) AS wtemp,
	ROUND(COALESCE(CUME_DIST() OVER w::numeric, 0), 2) AS cd,
    ROUND(COALESCE(PERCENT_RANK() OVER w::numeric, 0), 2) AS pr
FROM weather
WHERE wdate BETWEEN '2020-03-01' AND '2020-03-31'
WINDOW w AS (ORDER BY wtemp)
ORDER BY wtemp DESC 
LIMIT 5;


-- Запрос 12
SELECT wdate, COALESCE(wtemp, 0) AS wtemp,
	ROUND(CUME_DIST() OVER w::numeric, 2) AS perc
FROM weather
WHERE wdate BETWEEN '2020-03-01' AND '2020-03-31'
WINDOW w AS (ORDER BY wtemp)
ORDER BY wdate 
LIMIT 5;

-- Запрос 13
SELECT wdate, COALESCE(wtemp, 0) AS wtemp,
	ROUND(CUME_DIST() OVER w::numeric, 2) AS perc
FROM weather
WHERE wdate BETWEEN '2020-03-01' AND '2020-03-31'
WINDOW w AS (ORDER BY wtemp)
ORDER BY wdate 
LIMIT 5;

-- Запрос 14
WITH Q1 AS (
SELECT wdate, COALESCE(wtemp, 0) AS wtemp,
	ROUND(CUME_DIST() OVER w::numeric, 2) AS perc
FROM weather
WINDOW w AS (PARTITION BY EXTRACT(MONTH FROM wdate)  ORDER BY wtemp)
ORDER BY wdate)

SELECT *
FROM Q1
WHERE EXTRACT(DAY FROM wdate) = 7

-- Запрос 15
select department, city,
  percentile_disc(0.95) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees
GROUP BY department, city

-- Запрос 16
select EXTRACT(MONTH FROM wdate) AS wmonth,
	round(avg(wtemp)::decimal, 2)  AS t_avg,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY wtemp) AS t_med,
  percentile_disc(0.9) WITHIN GROUP (ORDER BY wtemp) AS  t_p90
FROM weather
GROUP BY wmonth

-- Запрос 17
select
  name, department, salary,
  percentile_disc(0.5) within group (order by salary)
    over (partition by department) as dep_p50
from employees
order by department, id;

-- Запрос 18
SELECT
  employees.name,
  employees.department,
  employees.salary,
  median_salaries.dep_p50
FROM
  employees
JOIN (
  SELECT
    department,
    percentile_disc(0.5) within group (order by salary) AS dep_p50
  FROM
    employees
  GROUP BY
    department
) AS median_salaries ON employees.department = median_salaries.department
ORDER BY
  employees.department, employees.id;

-- Запрос 19
select
  name, department, salary,
  (
    select percentile_disc(0.5) within group (order by salary)
    from employees as e2
    where e2.department = e1.department
  ) as dep_p50
from employees as e1
order by department, id;

-- Запрос 20
SELECT id, name, department, salary, 
FIRST_VALUE(salary) OVER w AS prev_salary,
last_VALUE(salary) OVER w1 AS max_salary
FROM employees
WINDOW w AS (PARTITION BY department
ORDER BY salary ROWS BETWEEN 1 PRECEDING AND CURRENT ROW),
w1 AS (PARTITION BY department
ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY department, salary;

-- Запрос 21
SELECT id, name, salary, 
count(salary) OVER w AS ge_cnt
FROM employees
WINDOW w AS (ORDER BY salary groups BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
ORDER BY salary, id;

-- Запрос 22
SELECT id, name, salary, 
FIRST_VALUE (salary) OVER w AS next_salary
FROM employees
WINDOW w AS (ORDER BY salary GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
ORDER BY salary, id;

-- Запрос 23
SELECT id, name, salary, 
count (salary) OVER w AS p10_cnt 
FROM employees
WINDOW w AS (ORDER BY salary RANGE BETWEEN CURRENT ROW AND 10 FOLLOWING)
ORDER BY salary, id;

-- Запрос 24
SELECT id, name, salary, 
LAST_VALUE (salary) OVER w AS lower_sal 
FROM employees
WINDOW w AS (ORDER BY salary RANGE BETWEEN 30 PRECEDING AND 10 PRECEDING)
ORDER BY salary, id;

-- Запрос 25
SELECT id, name, city, salary, department, 
sum(salary) OVER w AS cnt_dep 
FROM employees
window w as (
  partition by city
  order by department
  groups between current row and unbounded following
  exclude ties
)
ORDER BY  city, department, id;

-- Запрос 26
SELECT id, name, salary,  
round(avg(salary) OVER w,0) AS p20_sal 
FROM employees
window w as (
   order by salary
  range between current row and 20 following
  exclude current row
)
ORDER BY  salary, id;

-- Запрос 27
select
  name, department, salary,
  sum(salary) over () as "база",
  sum(salary) over w as "+0%",
  sum(salary*1.1) over w as "+10%",
  sum(salary*1.5)
    filter(where department <> 'it')
    over () as "+50% без ИТ"
from employees
window w as (
  rows between unbounded preceding and unbounded following
  exclude current row
)
order by id;

-- Запрос 28
select
  id, name, salary, city,
  round(salary * 100 / avg(salary) over ()) as "perc",
  round(salary * 100 / avg(salary) filter(where city <> 'Самара')
    over ()) as "msk",
 round(salary * 100 / avg(salary) filter(where city <> 'Москва')
    over ()) as "sam"
from employees
order by id;

-- Запрос 29
select
  name, city,
  sum(salary) over w as base,
  sum(CASE WHEN department = 'it' THEN salary/2 
  		WHEN  department = 'hr'THEN salary*2
  		WHEN  department = 'sales'THEN salary
  		ELSE salary 
  		end) over w as alt
from employees
window w as (partition by city)
order by city, id;

-- Запрос 30
with data as (
  select
    month,
    (case when plan = 'silver' then revenue end) silver,
    lag((case when plan = 'gold' then revenue end)) over w as gold
  from sales
  where year = 2020 and plan in ('gold', 'silver')
  window w as (
    partition by month
    order by plan
  )
)

select month, silver, gold
from data
where silver is not null;

-- Запрос 31
with data as (
  select
    month,
    (case when plan = 'silver' then revenue end) as silver,
    (case when plan = 'gold' then revenue end) as gold
  from Sales
  where year = 2020 and plan in ('gold', 'silver')
)
select
  month,
  max(silver) as silver,
  max(gold) as gold
from data
group by month
order by month;

-- Запрос 32
SELECT *
FROM Sales
WHERE YEAR = '2020'
ORDER BY YEAR, MONTH

SELECT YEAR, MONTH, 
	revenue,
	lag(revenue) OVER (ORDER BY MONTH) AS prev,
	round(revenue::NUMERIC/lag(revenue) OVER (ORDER BY MONTH)*100, 0) AS perc
FROM Sales
WHERE YEAR = '2020' AND plan = 'gold'
ORDER BY YEAR, MONTH

-- Запрос 33
SELECT plan, year, month, 
	revenue,
	sum(revenue) OVER (PARTITION BY plan  ORDER BY MONTH) AS total
FROM sales
WHERE YEAR = '2020' AND month BETWEEN 1 AND 3
ORDER BY plan, year, month

-- Запрос 34
SELECT year, month, 
	revenue,
	avg(revenue) OVER (ORDER BY month
	ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS avg3m
FROM sales
WHERE YEAR = '2020' AND plan = 'platinum'
ORDER BY plan, year, month

-- Запрос 35
SELECT year, month, 
	revenue,
	LAST_VALUE(revenue) OVER (PARTITION BY YEAR ORDER BY MONTH
	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS december,
	round(revenue::NUMERIC/	LAST_VALUE(revenue) OVER (PARTITION BY YEAR ORDER BY MONTH
	ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)*100, 0) AS perc
FROM Sales
WHERE plan = 'silver'
ORDER BY year, month

-- Запрос 36
SELECT DISTINCT year, plan,  
		sum(revenue) OVER w AS revenue,
		sum(revenue) OVER (PARTITION BY year) AS total,
		round(100.0*sum(revenue) OVER w/sum(revenue) OVER (PARTITION BY year), 0) AS perc
FROM sales
WINDOW w AS (PARTITION BY year, plan ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) 
ORDER BY year, plan

-- Запрос 37
SELECT year, month, 
		sum(revenue) AS revenue,
		NTILE(3) OVER (ORDER BY sum(revenue) DESC) AS tile
FROM sales
WHERE year = '2020'
GROUP BY year, month
ORDER BY revenue DESC 

-- Запрос 38
WITH Q1 AS (
SELECT  DISTINCT year, quarter,
	CASE WHEN year = '2020' THEN 
		sum(revenue) OVER (PARTITION BY year, quarter)
		ELSE 0
		END AS revenue,
	CASE WHEN year = '2019' THEN 
		sum(revenue) OVER (PARTITION BY year, quarter)
		ELSE 0
		END AS prev	
FROM sales
ORDER BY quarter, year DESC),

Q2 AS (
SELECT Q1.year, Q1.quarter, Q1.revenue,
	LEAD(Q1.prev) OVER (ORDER BY Q1.quarter) AS prev
	
FROM Q1)

SELECT *,
 round(100.0*Q2.revenue/Q2.prev, 0) AS perc
FROM Q2
WHERE year = '2020'

-- Запрос 39
with data as (
  select
    year, quarter,
    sum(revenue) as revenue,
    lag(sum(revenue), 4) over w as prev,
    round(
      sum(revenue) * 100.0 / lag(sum(revenue), 4) over ()
    ) as perc
  from sales
  group by year, quarter
  window w as (
    order by year, quarter
  )
)
select 
  year, quarter, revenue,
  prev, perc
from data
where year = 2020
order by quarter;

-- Запрос 40
with tgold as (
  SELECT year, month,
  	rank() OVER (ORDER BY sum(quantity) desc) AS rank_gold
  FROM sales
  WHERE year = '2020' AND plan = 'gold'
  GROUP BY year, month
  ORDER BY month 
),

tsilver as (
    SELECT year, month,
  	rank() OVER (ORDER BY sum(quantity) desc) AS rank_silver
  FROM sales
  WHERE year = '2020' AND plan = 'silver'
  GROUP BY year, month
  ORDER BY month
),

tplatinum as (
     SELECT year, month,
  	rank() OVER (ORDER BY sum(quantity) desc) AS rank_platinum
  FROM sales
  WHERE year = '2020' AND plan = 'platinum'
  GROUP BY year, month
  ORDER BY month
)

select
  tgold.year, tgold.month,
  tsilver.rank_silver as silver,
  tgold.rank_gold as gold,
  tplatinum.rank_platinum as platinum
from tgold
  join tsilver on tgold.month = tsilver.month
  join tplatinum on tgold.month = tplatinum.month

-- Запрос 41
SELECT *
FROM activity

with agroups as (
  SELECT user_id,
    adate,
    extract(epoch from adate)/86400 - dense_rank() over w as group_id
  from activity
  window w as (partition by user_id order by adate)
  )
SELECT user_id, group_id,
  min(adate) as day_start,
  max(adate) as day_end,
  count(*) as day_count
from agroups
group by user_id, group_id
order BY user_id, day_start

-- Запрос 42
WITH ranked_activity AS (
  SELECT *,
         LAG(points, 1, 0) OVER (PARTITION BY user_id ORDER BY adate) AS prev_points
  FROM activity
), series AS (
  SELECT *,
         SUM(CASE WHEN points >= prev_points THEN 0 ELSE 1 END) OVER (PARTITION BY user_id ORDER BY adate) AS series_group
  FROM ranked_activity
), series_summary AS (
  SELECT user_id,
         MIN(adate) AS day_start,
         MAX(adate) AS day_end,
         COUNT(*) AS day_count,
         SUM(points) AS p_total
  FROM series
  GROUP BY user_id, series_group
),
Q1 AS (
SELECT user_id, day_start, day_end, day_count, p_total
FROM series_summary
ORDER BY user_id, day_start)

SELECT *
FROM Q1
WHERE day_count !=1

-- Запрос 43
WITH ranked_activity AS (
  SELECT *,
         LAG(points, 1, 0) OVER (PARTITION BY user_id ORDER BY adate) AS prev_points
  FROM activity
), filtered_activity AS (
  SELECT *,
         CASE WHEN adate = lag(adate) OVER (PARTITION BY user_id ORDER BY adate) THEN 0 ELSE 1 END AS is_first_day
  FROM ranked_activity
), series AS (
  SELECT *,
         SUM(CASE WHEN points >= prev_points THEN 0 ELSE 1 END) OVER (PARTITION BY user_id ORDER BY adate) AS series_group
  FROM filtered_activity
), series_summary AS (
  SELECT user_id,
         MIN(adate) AS day_start,
         MAX(adate) AS day_end,
         COUNT(*) AS day_count,
         SUM(points) AS p_total
  FROM series 
  GROUP BY user_id, series_group
)
SELECT user_id, day_start, day_end, day_count, p_total
FROM series_summary
 WHERE day_count != 1
ORDER BY user_id, day_start;











