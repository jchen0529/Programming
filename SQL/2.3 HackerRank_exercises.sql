--Selected Hacker Rank SQL challenges from all difficulty levels

--Language: MySQL
--Profile: https://www.hackerrank.com/jchen0529

-- Basic Select
-- 1. Query the two cities in STATION with the shortest and longest CITY names, as well as their respective lengths.
(select city, length(city) from station order by length(city) limit 1)
UNION
(select city, length(city) from station order by length(city) DESC limit 1);

-- Amo 3
-- Marine On Saint Croix 21
