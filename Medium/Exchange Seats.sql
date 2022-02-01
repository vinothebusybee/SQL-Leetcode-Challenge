-- Question 56
-- Mary is a teacher in a middle school and she has a table seat storing students' names and their corresponding seat ids.

-- The column id is continuous increment.
 

-- Mary wants to change seats for the adjacent students.
 

-- Can you write a SQL query to output the result for Mary?
 

-- +---------+---------+
-- |    id   | student |
-- +---------+---------+
-- |    1    | Abbot   |
-- |    2    | Doris   |
-- |    3    | Emerson |
-- |    4    | Green   |
-- |    5    | Jeames  |
-- +---------+---------+
-- For the sample input, the output is:
 

-- +---------+---------+
-- |    id   | student |
-- +---------+---------+
-- |    1    | Doris   |
-- |    2    | Abbot   |
-- |    3    | Green   |
-- |    4    | Emerson |
-- |    5    | Jeames  |
-- +---------+---------+

-- Solution
select row_number() over (order by (if(id%2=1,id+1,id-1))) as id, student
from seat


%python
data = [
        [1, "Abbot"], 
        [2, "Doris"], 
        [3, "Emerson"], 
        [4, "Green"], 
        [5, "Jeames"],
    #[6, "xxxxx"]
       ]
		
columns = ["id", "student"]

df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("tbl")


%sql
select id, student,
case 
when id = mxid and mod(mxid,2)=1 then id
when mod(id,2)=1 then id+1
when mod(id,2)=0 then id-1 end as new_seat
from tbl, (select max(id) mxid from tbl)


