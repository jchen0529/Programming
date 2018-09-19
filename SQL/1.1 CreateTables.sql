1. SQL I - Create SQL tables and insert values using PSQL in terminal


### a. Create 3 tables (customers, orders, and baristas)

CREATE TABLE customers(
c_first_name   varchar(80),
c_last_name    varchar(80),
birthday       date,
address        VARCHAR(90),
phone          bigint
);

CREATE TABLE orders(
c_first_name varchar(80), 
c_last_name varchar(80), 
d_name varchar(80), 
drink_specialty varchar(80),
order_id  int, 
order_time timestamp, 
order_price real);

CREATE TABLE baristas(b_first_name  varchar(80), 
b_last_name  varchar(80), 
drink_specialty varchar(80),
shift_start_time time, 
shift_end_time time );

-- siliangchen=# \dt
--             List of relations
--  Schema |   Name    | Type  |    Owner    
-- --------+-----------+-------+-------------
--  public | baristas  | table | siliangchen
--  public | customers | table | siliangchen
--  public | orders    | table | siliangchen
-- (3 rows)

### b. Insert values to each table
INSERT INTO customers(c_first_name, c_last_name, birthday, address, phone)
VALUES 
('Ann', 'Keys', '1991-01-08', '802 Davis Street, Evanston 60201', 3849283749),
('Jane', 'Hollander','1990-02-08', '738 Sherman Ave, Evanston 60200', 7576382905),
('Cindy', 'Crawford','1992-01-23', '758 Sherman Rd, Evanston 60201', 8397229709);

INSERT INTO orders(c_first_name, c_last_name, d_name, drink_specialty, order_id, order_time, order_price)
VALUES 
('Ann', 'Keys', 'tall vanilla latte', 'latte', 10294, '2017-10-11 10:05:00', 4.25),
('Jane', 'Hollander','grande hazelnut latte', 'latte', 20295, '2017-10-11 10:10:00', 5.25),
('Cindy', 'Crawford','venti Americano', 'brewed', 3019, '2017-10-11 11:10:00', 3.25);

INSERT INTO baristas(b_first_name, b_last_name, drink_specialty, shift_start_time, shift_end_time)
VALUES 
('Andrew','King', 'latte', '8:00:00', '12:00:00'),
('Ben','Song', 'brewed', '8:00:00', '17:00:00'),
('Andrew','King', 'latte', '13:00:00', '17:00:00');

-- siliangchen=# select * from customers;
--  c_first_name | c_last_name |  birthday  |             address              |   phone    
-- --------------+-------------+------------+----------------------------------+------------
--  Ann          | Keys        | 1991-01-08 | 802 Davis Street, Evanston 60201 | 3849283749
--  Jane         | Hollander   | 1990-02-08 | 738 Sherman Ave, Evanston 60200  | 7576382905
--  Cindy        | Crawford    | 1992-01-23 | 758 Sherman Rd, Evanston 60201   | 8397229709
-- (3 rows)

-- siliangchen=# select * from orders;
--  c_first_name | c_last_name |        d_name         | drink_specialty | order_id |     order_time      | order_price 
-- --------------+-------------+-----------------------+-----------------+----------+---------------------+-------------
--  Ann          | Keys        | tall vanilla latte    | latte           |    10294 | 2017-10-11 10:05:00 |        4.25
--  Jane         | Hollander   | grande hazelnut latte | latte           |    20295 | 2017-10-11 10:10:00 |        5.25
--  Cindy        | Crawford    | venti Americano       | brewed          |     3019 | 2017-10-11 11:10:00 |        3.25
-- (3 rows)

-- siliangchen=# select * from baristas;
--  b_first_name | b_last_name | drink_specialty | shift_start_time | shift_end_time 
-- --------------+-------------+-----------------+------------------+----------------
--  Andrew       | King        | latte           | 08:00:00         | 12:00:00
--  Ben          | Song        | brewed          | 08:00:00         | 17:00:00
--  Andrew       | King        | latte           | 13:00:00         | 17:00:00
-- (3 rows)
