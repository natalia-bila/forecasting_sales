with table1 as
(select customer_id, aq_year, (case when aq_freq>=4 then 4 else aq_freq end) as aq_freq, aq_sale_amount from
(
select 
        customer_id, 
        EXTRACT(YEAR FROM aq_time AT TIME ZONE "UTC") AS aq_year, 
        count(*) as aq_freq,
        sum(sale_amount) as aq_sale_amount
    from (
          select 
              customer_id ,
              sale_timestamp, 
              min(sale_timestamp) over (PARTITION BY customer_id) as aq_time,
              sum(sale_amount) as sale_amount
          from da_test_task_20180318.sales
          group by 1,2
         )
         where EXTRACT(YEAR FROM sale_timestamp AT TIME ZONE "UTC") = EXTRACT(YEAR FROM aq_time AT TIME ZONE "UTC")
         group by 1,2
)
),
table2 as (
 select customer_id, sale_year, (case when freq>=4 then 4 else freq end) as freq,  sale_amount from
 (select 
        customer_id, 
        EXTRACT(YEAR FROM sale_timestamp AT TIME ZONE "UTC") AS sale_year, 
        count(*) as freq,
        sum(sale_amount) as sale_amount
        from (
          select 
              customer_id ,
              sale_timestamp, 
              sum(sale_amount) as sale_amount
          from da_test_task_20180318.sales
          group by 1,2
          )
          group by 1,2
)
),
table3 as
(select t1.*, t2.sale_year, t2.freq, t2.sale_amount, 
 from table1 as t1
inner join table2 as t2 on t1.customer_id = t2.customer_id and (t1.aq_year = t2.sale_year and aq_freq=freq or t1.aq_year+1 = t2.sale_year)
)
select t3.aq_year, t3.aq_freq, sale_year, freq, aq_n_customers, aq_rev,
count(customer_id) as n_customers,
round(sum(sale_amount),3) as rev
  from table3 as t3
inner join 
(select  aq_year, aq_freq, count(customer_id) as aq_n_customers, sum(aq_sale_amount) as aq_rev from table1
group by 1,2) as t4
on t3.aq_year = t4.aq_year and t3.aq_freq = t4.aq_freq
group by 1,2,3,4,5,6;