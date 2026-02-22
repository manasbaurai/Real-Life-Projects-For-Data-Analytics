CREATE SCHEMA gold;


-- Create dim_customers table
CREATE TABLE gold.dim_customers(
    customer_key INT,
    customer_id INT,
    customer_number VARCHAR(50),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    country VARCHAR(50),
    marital_status VARCHAR(50),
    gender VARCHAR(50),
    birthdate DATE,
    create_date DATE
);


-- Create dim_products table
CREATE TABLE gold.dim_products(
    product_key INT,
    product_id INT,
    product_number VARCHAR(50),
    product_name VARCHAR(50),
    category_id VARCHAR(50),
    category VARCHAR(50),
    subcategory VARCHAR(50),
    maintenance VARCHAR(50),
    cost INT,
    product_line VARCHAR(50),
    start_date DATE
);


-- Create fact_sales table
CREATE TABLE gold.fact_sales(
    order_number VARCHAR(50),
    product_key INT,
    customer_key INT,
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    sales_amount INT,
    quantity SMALLINT,
    price INT
);


-- TRUNCATE tables
TRUNCATE TABLE gold.dim_customers;
TRUNCATE TABLE gold.dim_products;
TRUNCATE TABLE gold.fact_sales;


-- Import CSV data (PostgreSQL uses COPY instead of BULK INSERT)

COPY gold.dim_customers
FROM 'C:\Program Files\PostgreSQL\18\data\dim_customers.csv'
DELIMITER ','
CSV HEADER;


COPY gold.dim_products
FROM 'C:\Program Files\PostgreSQL\18\data\dim_products.csv'
DELIMITER ','
CSV HEADER;


COPY gold.fact_sales
FROM 'C:\Program Files\PostgreSQL\18\data\fact_sales.csv'
DELIMITER ','
CSV HEADER;


select order_date,
sales_amount
from gold.fact_sales ;

select 
extract(month from order_date) as order_year,
sum(sales_amount) as total_,
count(distinct customer_key) as total_customers,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by order_year
order by order_year ;


--Calculating the total sales per month and the running total of sales over time.

select * from gold.fact_sales ;

select 
order_year,
order_month,
total_sales,
sum(total_sales) over(order by total_sales) as running_total_sales,
round(avg(average_price) over(order by order_year, order_month),2) as moving_average_price
from(
	select 
	extract(year from order_date) as order_year,
	extract(month from order_date) as order_month,
	avg(price) as average_price,
	sum(sales_amount) as total_sales
	from gold.fact_sales 
	where order_date is not null
	group by 1,2
	order by 1,2 ) ;


--Analyze the yearly performance of products by comparing their sales to both the average sales performance of the product 
--and the previous year's sale

with yearly_product_sales as 
(
select 
extract (year from f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from gold.fact_sales as f
join gold.dim_products as p 
on f.product_key = p.product_key 
where f.order_date is not null
group by 1,2 
) 

select order_year, product_name, current_sales,
round(avg(current_sales) over(partition by product_name),2) as average_sales,
current_sales - round(avg(current_sales) over(partition by product_name),2) as diff_avg,
case
	when current_sales - round(avg(current_sales) over(partition by product_name),2) > 0 then 'Above Avg'
	when current_sales - round(avg(current_sales) over(partition by product_name),2) < 0 then 'Below Avg'
	else 'Avg'
end as avg_change,
lag(current_sales) over(partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_py,
case 
	when current_sales - lag(current_sales) over(partition by product_name order by order_year) > 0 then 'Increased'
	when current_sales - lag(current_sales) over(partition by product_name order by order_year) < 0 then 'Decreased'
	else 'No Change'
end as Change_In_Sales
from yearly_product_sales 
order by 2,1 ;


--Which categories contribute the most to the overall sales.

with category_sales as (
select category,
sum(sales_amount) as total_sales
from gold.fact_sales as f
join gold.dim_products as p
on f.product_key = p.product_key 
group by 1
)
select category,
total_sales,
sum(total_sales) over() as overall_sales,
concat(round((total_sales / sum(total_sales) over()) * 100,2), ' %') as percentage_of_total
from category_sales ;


--Categorize each product based on their cost and count how many of them fall into each category.

with product_segments as (
select 
product_key,
cost,
case
	when cost < 100 then 'Below 100'
	when cost between 100 and 500 then '100-500'
	when cost between 500 and 1000 then '500-1000'
	else 'Above 1000'
end as cost_range
from gold.dim_products
)

select cost_range,
count(product_key) as total_products
from product_segments
group by 1 
order by 2 desc ;



/* Group customers into three segments based on their spending behavior:
- VIP: Customers with at least 12 months of history and spending more than €5,000.
- Regular: Customers with at least 12 months of history but spending €5,000 or less.
- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group */

with customer_spending as (
select 
c.customer_key,
sum(sales_amount) as total_spending,
min(order_date),
max(order_date),
( EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
+ EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))
) AS lifespan
from gold.fact_sales as f
join gold. dim_customers as c
on f.customer_key = c.customer_key 
group by 1
)

select 
customer_segment,
count(*) as total_customers 
from 
	(select customer_key,
	lifespan,
	total_spending,
	case
		when lifespan >= 12 and total_spending > 5000 then 'VIP'
		when lifespan >= 12 and total_spending < 5000 then 'Regular'
		else 'New'  
	end as customer_segment
	from customer_spending )
	group by 1
	order by 2 desc ;


/* Customer Report

Purpose:
This report consolidates key customer metrics and behaviors
Highlights:
1. Gathers essential fields such as names, ages, and transaction details. 
2. Segments customers into categories (VIP, Regular, New) and age groups. 
3. Aggregates customer-level metrics:
- total orders
- total sales
- lifespan (in months)
- total quantity purchased 
- total products

4. Calculates valuable KPIs:
- recency (months since last order)
- average order value
- average monthly spend */

create view gold.customers_report as
/* ----------------------------------------------------------------------------------------------------------------------------------------------
1. Base Query: Retrieves core columns from tables. 
 ---------------------------------------------------------------------------------------------------------------------------------------------- */

with base_query as 
	( select 
	order_number,
	product_key,
	order_date,
	sales_amount,
	quantity,
	c.customer_key,
	customer_number,
	concat(first_name, ' ', last_name) as customer_name,
	extract(year from age(birthdate)) as age
	from gold.fact_sales as f
	join gold.dim_customers as c
	on f.customer_key = c.customer_key 
	where order_date is not null )

/* 2. Aggregating columns on the customer level metrics */

 , customer_aggregation as 
(select 
customer_key,
customer_number,
customer_name,
age,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_purchased,
count(product_key) as total_products,
max(order_date)as last_order_date,
( EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
+ EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))
) AS lifespan
from base_query
group by customer_key,
customer_number,
customer_name,
age ) 

/* 3. Segments customers into categories (VIP, Regular, New) and age groups. */

select 
customer_key,
customer_number,
customer_name,
age,
case 
	when age < 20 then 'Below 20'
	when age between 20 and 29 then '20-29'
	when age between 30 and 29 then '30-39'
	when age between 40 and 49 then '40-49'
	else '50 and Above'
end as age_group,
case
	when lifespan >= 12 and total_sales > 5000 then 'VIP'
	when lifespan >= 12 and total_sales < 5000 then 'Regular'
	else 'New'  
end as customer_segment,
total_orders,
total_sales,
total_quantity_purchased,
total_products,
last_order_date,

/* 4. Calculating valuable KPIs. */

-- Calculating Recency 
( EXTRACT(YEAR FROM AGE(last_order_date)) * 12 
+ EXTRACT(MONTH FROM AGE(last_order_date))) as recency,
lifespan,
-- Calculating Avg Order Value
case
	when total_orders = 0 then total_sales 
	else round(total_sales / total_orders, 2)
end as avg_order_value,
-- Calculating Avg. Monthly Spend
case
	when lifespan = 0 then total_sales
	else round(total_sales / lifespan, 2) 
end as avg_monthly_spend
from customer_aggregation ; 


--Product Report 
/*
Purpose:
This report consolidates key product metrics and behaviors.
Highlights:
1. Gathers essential fields such as product name, category, subcategory, and cost.
2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
3. Aggregates product-level metrics:
- total orders
- total sales
- total quantity sold
- total customers (unique)
- lifespan (in months)
4. Calculates valuable KPIs:
- recency (months since last sale)
- average order revenue (AOR)
- average monthly revenue */


create view gold.products_report as 
-- 1. Gathers essential fields such as product name, category, subcategory, and cost.

with base_query as 
 (  select 
	order_number,
	order_date,
	sales_amount,
	quantity,
	customer_key,
	p.product_key,
	product_number,
	product_name,
	category,
	subcategory,
	cost
from gold.fact_sales as f
join gold.dim_products as p
on f.product_key = p.product_key
where order_date is not null) 


-- 2. Aggregating product-level metrics.

, product_aggregation as (
select product_key,
product_name, 
category, 
subcategory,
cost,
count(order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity_sold,
count(distinct customer_key) as total_customers,
max(order_date) as last_order_date,
extract(year from age(max(order_date), min(order_date))) * 12 
+ extract(month from age(max(order_date), min(order_date))) as lifespan
from base_query
group by 
product_key,
product_name, 
category, 
subcategory,
cost )

-- 3. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.

select product_key,
product_name, 
category, 
subcategory,
cost,
total_orders,
total_sales,
case
	when total_sales > 50000 then 'High Performer'
	when total_sales >=10000 then 'Mid Range'
	else 'Low Performer'
end as product_segment,
total_quantity_sold,
total_customers,
lifespan,

--4. Calculating valuable KPIs.

--Recency
last_order_date,
( EXTRACT(YEAR FROM AGE(last_order_date)) * 12 
+ EXTRACT(MONTH FROM AGE(last_order_date))) as recency,

--Calculating Average Order Revenue
case
	when total_orders = 0 then total_sales
	else round(total_sales / total_orders, 2) 
end as avg_order_revenue,

-- Calculating Average Monthly Revenue
case 
	when lifespan = 0 then total_sales
	else round(total_sales / lifespan, 2) 
end as avg_monthly_revenue
from product_aggregation ;








