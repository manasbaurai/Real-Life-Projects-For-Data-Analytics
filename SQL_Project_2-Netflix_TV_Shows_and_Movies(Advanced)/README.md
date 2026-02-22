# 🎬 Netflix Data Analysis Project (PostgreSQL)

This project focuses on performing **data analysis on Netflix dataset using SQL (PostgreSQL)**.

It includes:

- Database table creation
- Data exploration
- Data cleaning techniques
- Business-driven analytical queries

The aim of this project is to extract **meaningful insights about Netflix content** such as content distribution, ratings, actors, directors, and trends.

The Dataset used in this:- [Netflix_Dataset](https://www.kaggle.com/datasets/shivamb/netflix-shows)

---

## 🧱 1. Database Setup

We start by creating a table named **`netflix`** to store all the data.

```sql
CREATE TABLE netflix(
    show_id VARCHAR(10),
    type VARCHAR(10),
    title VARCHAR(150),
    director VARCHAR(250),
    casts VARCHAR(1000),
    country VARCHAR(150),
    date_added VARCHAR(50),
    release_year INT,
    rating VARCHAR(10),
    duration VARCHAR(15),
    listed_in VARCHAR(100),
    description VARCHAR(250)
);
```

This table stores both Movies and TV Shows data from Netflix.

---

## 🔍 2. Business Analysis Queries

Below are the key business questions solved using SQL.

---

### 🎥 1. Count Movies vs TV Shows

```sql
SELECT type, COUNT(*)
FROM netflix
GROUP BY type;
```

👉 This shows how much content is Movies vs TV Shows.

---

### ⭐ 2. Most Common Rating by Content Type

```sql
SELECT type, rating, total_count
FROM (
    SELECT 
        type,
        rating,
        COUNT(*) AS total_count,
        RANK() OVER(PARTITION BY type ORDER BY COUNT(*) DESC) AS ranking
    FROM netflix
    GROUP BY type, rating
) t
WHERE ranking = 1;
```

👉 Identifies the most frequent rating for Movies and TV Shows.

---

### 📅 3. Movies Released in a Specific Year (2020)

```sql
SELECT title, release_year
FROM netflix
WHERE type = 'Movie'
AND release_year = 2020;
```

👉 Lists all movies released in 2020.

---

### 🌍 4. Top 5 Countries with Most Content

```sql
SELECT 
    TRIM(UNNEST(STRING_TO_ARRAY(country, ','))) AS new_country,
    COUNT(*) AS total_content
FROM netflix
GROUP BY new_country
ORDER BY total_content DESC
LIMIT 5;
```

👉 Extracts top contributing countries.

---

### 🎬 5. Longest Movie

```sql
SELECT type, title, duration
FROM netflix
WHERE type = 'Movie'
AND duration = (SELECT MAX(duration) FROM netflix);
```

👉 Finds the movie with the longest duration.

---

### ⏳ 6. Content Added in Last 5 Years

```sql
SELECT type, title, date_added
FROM netflix
WHERE TO_DATE(date_added, 'Month DD, YYYY') >= CURRENT_DATE - INTERVAL '5 years';
```

👉 Filters recent content added in the last 5 years.

---

### 🎬 7. Content by Director Rajiv Chilaka

```sql
SELECT type, title, director
FROM netflix
WHERE director ILIKE '%Rajiv Chilaka%';
```

👉 Finds all content directed by Rajiv Chilaka.

---

### 📺 8. TV Shows with More Than 5 Seasons

```sql
SELECT type, title, duration
FROM netflix
WHERE type = 'TV Show'
AND SPLIT_PART(duration, ' ', 1)::NUMERIC > 5;
```

👉 Filters long-running TV shows.

---

### 🎭 9. Content Count by Genre

```sql
SELECT 
    TRIM(UNNEST(STRING_TO_ARRAY(listed_in, ','))) AS genre,
    COUNT(*) AS total_content
FROM netflix
GROUP BY genre
ORDER BY total_content DESC;
```

👉 Shows most popular genres on Netflix.

---

### 🇮🇳 10. Average Content Released by India per Year

```sql
SELECT 
    EXTRACT(YEAR FROM TO_DATE(date_added, 'Month DD, YYYY')) AS year,
    COUNT(*) AS yearly_content,
    ROUND(
        COUNT(*)::NUMERIC /
        (SELECT COUNT(*) FROM netflix WHERE country = 'India')::NUMERIC * 100,
        2
    ) AS avg_content_per_year
FROM netflix
WHERE country = 'India'
GROUP BY year
ORDER BY avg_content_per_year DESC
LIMIT 5;
```

👉 Shows top 5 years when India produced most Netflix content.

---

### 🎥 11. Movies that are Documentaries

```sql
SELECT type, title, listed_in
FROM netflix
WHERE type = 'Movie'
AND listed_in ILIKE '%Documentaries%';
```

👉 Filters documentary movies.

---

### ❌ 12. Content Without Director

```sql
SELECT *
FROM netflix
WHERE director IS NULL;
```

👉 Identifies missing director data.

---

### 🎬 13. Salman Khan Movies in Last 10 Years

```sql
SELECT *
FROM netflix
WHERE casts ILIKE '%Salman Khan%'
AND release_year > EXTRACT(YEAR FROM CURRENT_DATE) - 10;
```

👉 Counts recent Salman Khan appearances.

---

### 🌟 14. Top 10 Actors in Indian Content

```sql
SELECT 
    TRIM(UNNEST(STRING_TO_ARRAY(casts, ','))) AS actors,
    COUNT(*) AS appearances
FROM netflix
WHERE country ILIKE '%India%'
GROUP BY actors
ORDER BY appearances DESC
LIMIT 10;
```

👉 Finds most frequent actors in Indian content.

---

### ⚠️ 15. Content Classification (Good vs Bad)

```sql
SELECT category, COUNT(*)
FROM (
    SELECT 
        type,
        title,
        description,
        CASE 
            WHEN description ILIKE '%kill%' OR description ILIKE '%violence%' 
            THEN 'Bad_Content'
            ELSE 'Good_Content'
        END AS category
    FROM netflix
) t
GROUP BY category
ORDER BY COUNT(*) DESC;
```

👉 Categorizes content based on description keywords.

---

## 📊 Key Skills Demonstrated

This project showcases:

- SQL Data Cleaning
- String manipulation in PostgreSQL
- Window Functions (RANK)
- Date Functions
- Aggregations and Grouping
- Business Analytics Queries

---

## 🚀 Conclusion

This project demonstrates how SQL can be used to extract meaningful insights from entertainment datasets.

It can be extended further by:

- Connecting to Power BI / Tableau dashboards
- Building recommendation systems
- Performing trend analysis over time
```
