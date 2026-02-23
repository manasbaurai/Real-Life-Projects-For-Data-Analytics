create table zepto 
like zepto_v2 ;

insert into zepto 
select * from zepto_v2 ;

alter table zepto 
add column sku_id int auto_increment primary key first;

alter table zepto 
modify column category VARCHAR(120),
modify column name VARCHAR(150) NOT NULL,
modify column mrp DECIMAL(8,2),
modify column discountPercent DECIMAL(5,2),
modify column availableQuantity INT,
modify column discountedSellingPrice DECIMAL(8,2),
modify column weightInGms INT,
modify column quantity INT ;

select * from zepto ;

alter table zepto 
modify column category VARCHAR(120),
modify column name VARCHAR(150) NOT NULL,
modify column mrp DECIMAL(8,2),
modify column discountPercent DECIMAL(5,2),
modify column availableQuantity INT,
modify column discountedSellingPrice DECIMAL(8,2),
modify column weightInGms INT,
modify column quantity INT ;

-- Data Cleaning 

select * from zepto 
where quantity is null ;

select distinct category 
from zepto 
order by 1 ;

select outOfStock, count(sku_id)
from zepto 
group by outOfStock ;

select name, count(sku_id) as number_of_repetitions 
from zepto 
group by name 
having number_of_repetitions > 1
order by number_of_repetitions desc ;

select * from zepto
where mrp = 0 or discountedSellingPrice = 0 ;

delete from zepto 
where sku_id = 3116 ;

update zepto 
set mrp = mrp/100.0,
discountedSellingPrice = discountedSellingPrice/100.0 ;

select  mrp , discountedSellingPrice from zepto ;

-- Q1. Find the top 10 best value products based on discount percentage.

select * from zepto ;

select distinct name, mrp, discountPercent
from zepto 
order by discountPercent desc
limit 10 ;

-- Q2. What are the products with High MRP but out of stock. 

select distinct name, mrp, outOfStock
from zepto 
where outOfStock = "TRUE" 
order by mrp desc ;

-- Q3. Calculate estimated revenue for each category

select category, sum(discountedSellingPrice) as Estimated_Revenue 
from zepto  
group by category 
order by sum(discountedSellingPrice) desc ;

-- Q4. Find all products where MRP is greater than 500 Rupees and discount percentage is less than 10 %.

select distinct name, mrp, discountPercent
from zepto 
where mrp > 500.00 and discountPercent < 10.00 
order by mrp desc, discountPercent desc ;

-- Q5. Identify the top 5 categories offering the highest average discount percentage. 

select category, round(avg(discountPercent),2) as Average_Discount_Percentage
from zepto 
group by category
order by Average_Discount_Percentage desc
limit 5 ;

-- Q6. Find the price per gram for products above 100g and sort by best value. 

select distinct name, discountedSellingPrice, 
weightInGms, 
round(discountedSellingPrice/weightInGms,2) as Price_per_gram
from zepto
where weightInGms >= 100
order by Price_per_gram asc ;

-- Q7. Group the products into categories like Low, Medium, Bulk 

select * from zepto ;

select name, weightInGms,
case when weightInGms <= 1000 then "Low"
	 when weightInGms between 1000 and 5000 then "Medium"
     else "Bulk"
end as Weight_Category
from zepto ;

-- Q8. What is the total inventory weight per category. 

select category, 
sum(weightInGms * availableQuantity) as Weight_per_category
from zepto
group by category 
order by Weight_per_category desc ;