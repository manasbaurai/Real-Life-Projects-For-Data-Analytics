# 📊 Sales Data Analytics Project (PostgreSQL)

This project demonstrates a complete **SQL-based Advanced Data Analytics workflow** using PostgreSQL.  
It includes schema creation, table design, CSV data import, business analysis queries, and final reporting views for customers and products.

The main goal of this project is to analyze:

- Sales performance over time  
- Customer purchasing behavior  
- Product performance and contribution to revenue  

---

# 🧱 1. Database & Schema Setup

We created a dedicated schema named `gold` to store all analytics tables.

```sql
CREATE SCHEMA gold;
```

This helps separate analytics-ready data from raw data sources.

---

## 🗂️ 2. Table Design (Star Schema)

The project uses a star schema design with:

- 2 Dimension Tables → Customers and Products
- 1 Fact Table → Sales

### 👤 Customers Dimension

Stores customer demographic details.

```sql
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
```

### 📦 Products Dimension

Stores product information, categories, and cost.

```sql
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
```

### 🧾 Sales Fact Table

Stores transactional sales data.

```sql
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
```

---

## 🧹 3. Data Cleaning & Loading

Before loading new data, we clear old data:

```sql
TRUNCATE TABLE gold.dim_customers;
TRUNCATE TABLE gold.dim_products;
TRUNCATE TABLE gold.fact_sales;
```

We then load CSV files using PostgreSQL `COPY`:

```sql
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
```

---

## 📈 4. Monthly Sales Analysis

This query calculates monthly performance including sales, customers, and quantity.

```sql
SELECT 
    EXTRACT(MONTH FROM order_date) AS order_month,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_month
ORDER BY order_month;
```

---

## 📊 5. Running Total & Moving Average Analysis

This query calculates monthly sales, running total of sales, and moving average of product price.

```sql
SELECT 
    order_year,
    order_month,
    total_sales,
    SUM(total_sales) OVER(ORDER BY total_sales) AS running_total_sales,
    ROUND(AVG(average_price) OVER(ORDER BY order_year, order_month),2) AS moving_average_price
FROM(
    SELECT 
        EXTRACT(YEAR FROM order_date) AS order_year,
        EXTRACT(MONTH FROM order_date) AS order_month,
        AVG(price) AS average_price,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales 
    WHERE order_date IS NOT NULL
    GROUP BY 1,2
    ORDER BY 1,2
) t;
```

---

## 📉 6. Yearly Product Performance Analysis

This compares each product's yearly sales with its own average sales and previous year sales.

```sql
WITH yearly_product_sales AS (
    SELECT 
        EXTRACT(YEAR FROM f.order_date) AS order_year,
        p.product_name,
        SUM(f.sales_amount) AS current_sales
    FROM gold.fact_sales f
    JOIN gold.dim_products p
    ON f.product_key = p.product_key 
    WHERE f.order_date IS NOT NULL
    GROUP BY 1,2
)
SELECT 
    order_year,
    product_name,
    current_sales,
    ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) AS avg_sales,
    LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS previous_year_sales
FROM yearly_product_sales
ORDER BY product_name, order_year;
```

---

## 🏆 7. Category Contribution to Sales

This query shows which product categories contribute most to revenue.

```sql
WITH category_sales AS (
    SELECT 
        category,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales f
    JOIN gold.dim_products p
    ON f.product_key = p.product_key 
    GROUP BY category
)
SELECT 
    category,
    total_sales,
    SUM(total_sales) OVER() AS overall_sales,
    CONCAT(ROUND((total_sales / SUM(total_sales) OVER()) * 100,2),' %') AS percentage
FROM category_sales;
```

---

## 💰 8. Product Cost Segmentation

Products are grouped into cost ranges.

```sql
SELECT 
    CASE
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END AS cost_range,
    COUNT(*) AS total_products
FROM gold.dim_products
GROUP BY cost_range
ORDER BY total_products DESC;
```

---

## 👥 9. Customer Segmentation

Customers are segmented based on lifespan (months active) and total spending.

```sql
WITH customer_spending AS (
    SELECT 
        c.customer_key,
        SUM(sales_amount) AS total_spending,
        MIN(order_date),
        MAX(order_date),
        ( EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
        + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) ) AS lifespan
    FROM gold.fact_sales f
    JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key 
    GROUP BY 1
)
SELECT 
    CASE
        WHEN lifespan >= 12 AND total_spending > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_spending <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment,
    COUNT(*) AS total_customers
FROM customer_spending
GROUP BY customer_segment
ORDER BY total_customers DESC;
```

---

## 📘 10. Customer Report View

This view creates a complete customer analytics dashboard.

```sql
CREATE VIEW gold.customers_report AS
WITH base_query AS (
    SELECT 
        order_number,
        product_key,
        order_date,
        sales_amount,
        quantity,
        c.customer_key,
        customer_number,
        CONCAT(first_name,' ',last_name) AS customer_name,
        EXTRACT(YEAR FROM AGE(birthdate)) AS age
    FROM gold.fact_sales f
    JOIN gold.dim_customers c
    ON f.customer_key = c.customer_key 
    WHERE order_date IS NOT NULL
),
customer_aggregation AS (
    SELECT 
        customer_key,
        customer_number,
        customer_name,
        age,
        COUNT(order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity_purchased,
        COUNT(product_key) AS total_products,
        MAX(order_date) AS last_order_date,
        ( EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
        + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) ) AS lifespan
    FROM base_query
    GROUP BY customer_key, customer_number, customer_name, age
)
SELECT 
    *,
    ROUND(total_sales/NULLIF(total_orders,0),2) AS avg_order_value,
    ROUND(total_sales/NULLIF(lifespan,0),2) AS avg_monthly_spend
FROM customer_aggregation;
```

---

## 📦 11. Product Report View

This view provides product-level performance metrics.

```sql
CREATE VIEW gold.products_report AS
WITH base_query AS (
    SELECT 
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
    FROM gold.fact_sales f
    JOIN gold.dim_products p
    ON f.product_key = p.product_key
    WHERE order_date IS NOT NULL
),
product_aggregation AS (
    SELECT 
        product_key,
        product_name,
        category,
        subcategory,
        cost,
        COUNT(order_number) AS total_orders,
        SUM(sales_amount) AS total_sales,
        SUM(quantity) AS total_quantity_sold,
        COUNT(DISTINCT customer_key) AS total_customers,
        MAX(order_date) AS last_order_date,
        ( EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12 
        + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) ) AS lifespan
    FROM base_query
    GROUP BY product_key, product_name, category, subcategory, cost
)
SELECT 
    *,
    ROUND(total_sales/NULLIF(total_orders,0),2) AS avg_order_revenue,
    ROUND(total_sales/NULLIF(lifespan,0),2) AS avg_monthly_revenue
FROM product_aggregation;
```

---

## 🚀 Conclusion

This project demonstrates:

- SQL Data Warehouse design
- Analytical query writing
- Window functions and aggregations
- Customer and product behavior analysis

It can be extended further by integrating dashboards in **Power BI** or **Tableau**.
```
