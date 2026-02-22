--Netflix Data Analysis Project 

create table netflix(
show_id varchar(10),
type varchar(10),
title varchar(150),
director varchar(250),
casts varchar(1000),
country varchar(150),
date_added varchar(50),
release_year int,
rating varchar(10),
duration varchar(15),
listed_in varchar(100),
description varchar(250)
)


--1. Count the number of movies vs TV Shows

select type, count(*)
from netflix
group by 1 ;


--2. Find the most common rating for movies and TV Shows.

select type, rating, total_count from
	(
	select 
	type,
	rating,
	count(*) as total_count,
	rank() over(partition by type order by count(*) desc) as ranking
	from netflix
	group by 1,2 )
where ranking = 1 ;


--3. List all the movies released in a specific year (e.g., 2020)

select title, release_year
from netflix
where type = 'Movie'
and
release_year = 2020 ;


-- 4. Find the top 5 countries with the most content on Netflix.

select * from netflix 

select 
trim(unnest(string_to_array(country, ','))) as new_country,
count(*)
from netflix
group by new_country
order by 2 desc 
limit 5;


--5. Identify the longest movie.

select type, title, duration 
from netflix
where type = 'Movie' and 
duration = (select max(duration) from netflix)


--6. Find content added in the last 5 years.

select to_date(date_added, 'Month DD, YYYY'),*
from netflix ;

select type, title, date_added
from netflix
where to_date(date_added, 'Month DD, YYYY')>= current_date - interval '5 Years' ;


--7. Find all the movies/TV Shows by director Rajiv Chilaka

select type, title, director
from netflix
where director ilike '%Rajiv Chilaka%' 


--8. Find all the TV Shows with more than 5 seasons

select split_part(duration, ' ', 1)
from netflix 
where type = 'TV Show';

select type, title, duration 
from netflix 
where type = 'TV Show' and
split_part(duration, ' ', 1)::numeric > 5 ;


--9. Find the number of content items in each genre.

select * from netflix ;

select 
trim(unnest(string_to_array(listed_in, ','))) as genre,
count(*) as total_content
from netflix 
group by genre 
order by total_content desc;


--10. Find each year and the average numer of content released by India on Netflix. 
-- Return top 5 year with the highest number of average content.

select 
extract(year from to_date(date_added, 'Month DD, YYYY')) ,
count(*) as yearly_content,
round(count(*)::numeric/(select count(*) from netflix where country = 'India')::numeric * 100, 2) as avg_content_per_year
from netflix
where country = 'India' 
group by 1
order by avg_content_per_year desc
limit 5 ;


--11. List all the movies that are documentaries.

select * from netflix 

select type, title, listed_in 
from netflix
where type = 'Movie'
and listed_in ilike '%Documentaries%' ;


--12. Find all the content without a director.

select * from netflix ;

select * from netflix
where director is null ;


--13. Find how many movies actor Salman Khan appeared in the last 10 years.

select *
from netflix
where casts ilike '%Salman Khan%' 
and release_year > extract(year from current_date) - 10


--14. Find the top 10 actors who have appeared in the highest number of movies produced in india.

select 
trim(unnest(string_to_array(casts, ','))) as actors,
count(*)
from netflix
where country ilike '%India%'
group by 1 
order by 2 desc 
limit 10;


--15. Categorise each content by giving them a label of "Bad" if their description contains words like "kill" or "violence" or 
--label them as "Good" if their description contains nothing like that.
--Count the different types of content based on their label. 

select category, count(*)
from
	(
	select type, title, description,
	case 
		when description ilike '%kill%' or
			 description ilike '%violence%' then 'Bad_Content'
		else 'Good_Content'
	end as category
	from netflix )
group by 1 
order by 2 desc;


