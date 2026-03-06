select dp.category, sum(amount) as summ
from fact_sales
join dim_product dp using(product_id)
join dim_date dd using(date_id)
where dd.year = 2024
group by dp.category
order by summ desc

select dc.region, count(*) as count_orders
from fact_sales
join dim_customer dc using(customer_id)
group by dc.region
order by count_orders desc
limit 3

select round(avg(amount),2) as avg,
case
    when extract(year from age(dc.birth_date)) < 25 then '<25'
    when extract(year from age(dc.birth_date)) between 25 and 34 then '25-34'
    when extract(year from age(dc.birth_date)) between 35 and 44 then '35-44'
    when extract(year from age(dc.birth_date)) > 45 then '45+'
end as age_category
from fact_sales
join dim_customer dc using(customer_id)
group by age_category