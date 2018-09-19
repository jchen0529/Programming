--Homework - query university data based on questions

-- Schema: 
-- student(sid, sname, sex, age, year, gpa)
-- dept(dname, numphds)
-- prof(pname, dname)
-- course(cno, cname, dname)
-- major(dname, sid)
-- section(dname, cno, sectno, pname)
-- enroll(sid, grade, dname, cno, sectno)

-- 1. Print the names of professors who work in departments that have more than 15 PhD students.
select pname
from prof
join dept
using (dname)
where numphds > 15
group by 1;

--      pname
-- ---------------
--   Brian, C.
--   Brown, S.
--   Clark, E.
--   Edison, L.
--   Jones, J.
--   Randolph, B.
--   Robinson, T.
--   Smith, S.
--   Walter, A.

-- 2. Print the name(s) of the youngest student(s).
select sname
from student
where age = (select min(age) from student); --youngest student

--      sname     
-- ---------------
--  News, Nightly

-- 3. For each Mathematics class, print the cno, sectno, and the number of enrolled students with GPAs below 3.
select cno, sectno, count(1) as students
from enroll
where dname = 'Mathematics'
and grade < 3 --GPA below 3
group by 1,2; 

--  cno | sectno | students 
-- -----+--------+----------
--  462 |      1 |        1
--  461 |      1 |        2

-- 4. Print the course names, course numbers and section numbers of all classes with more than 3 female students enrolled in them.
select cname, cno, sectno
from (
	select sid, dname, cno, sectno
	from enroll
	join student
	using (sid)
	where sex = 'f' --female students only
	group by 1,2,3,4
	) a
join course b
using (cno, dname)
group by 1,2,3
having count(1) > 3; --more than 3 female students

--           cname           | cno | sectno 
-- --------------------------+-----+--------
--  Advanced City Planning   | 561 |      1
--  City Planning            | 365 |      1
--  Compiler Construction    | 701 |      1
--  Highway Engineering      | 375 |      1
--  Intro to Data Structures | 467 |      1

-- 5. Print the names of professors in departments where those departments have one or more majors who are under 18 years old.
select pname
from student
join major
using (sid)
join prof 
using (dname)
where age < 18; --students under 18

--      pname     
-- ---------------
--   Robinson, T.
--   Smith, S.
--   Walter, A.

-- 6. Print the names and majors of students who are taking more than one Intro course. 
-- (Hint: You’ll need to use the “like” predicate and the string matching character in your relational algebra, as well as your query.)
select sname, dname
from (
	select sid
	from course
	join enroll
	using (cno, dname)
	where cname like 'Intro%'
	group by 1
	having count(1) > 1 --more than one Intro 
	) a
join student
using (sid)
join major
using (sid); 

--     sname     |         dname          
-- --------------+------------------------
--  Fy, Clara I. | Civil Engineering
--  Heilskov, G. | Industrial Engineering
--  Emile, R.    | Computer Science

-- 7. For those departments that have no majors taking a College Geometry course, print the department name and the number of PhD students in the department.
select dept.dname, numphds
from dept
join (
	select dname
	from major
	join enroll
	using (sid, dname)
	join course
	using (cno, dname)
	where cname not like 'College Geometry%' --no Geometry
	group by 1
	) a
using (dname); 

--          dname          | numphds 
-- ------------------------+---------
--  Civil Engineering      |      88
--  Industrial Engineering |      41
--  Computer Science       |      47

-- 8. Print the names of students who are majoring in both Computer Science and Sanitary Engineering.
select sname
from student
join major
using (sid)
where dname in ('Computer Science', 'Sanitary Engineering')
group by 1
having count(1) = 2; --2 majors 

--        sname       
-- -------------------
--  Sulfate, Barry M.

-- 9. Print the absolute difference in average age between Computer Science majors and Mathematics majors.
select max(average_age) - min(average_age) as diff_avg_age
from (
	select dname, avg(age) as average_age 
	from student
	join major
	using (sid)
	where dname in ('Computer Science', 'Mathematics')
	group by 1) a;

--     diff_avg_age    
-- --------------------
--  5.5899122807017544

-- 10. For each department that has fewer than ten male majors, print the name of the department and the average GPA of its majors.
select dname, avg(grade) as average_gpa
from (
	select major.dname
	from major 
	join student
	using (sid)
	where sex = 'm'
	group by 1
	having count(1) < 10 --fewer than ten male majors
	) a
join enroll
using (dname)
group by 1;

--         dname         | average_gpa 
-- ----------------------+-------------
--  Chemical Engineering |           3
--  Sanitary Engineering |         2.6


