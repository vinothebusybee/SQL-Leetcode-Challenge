
%python
data = [
        [1001,1,60],
        [1001,2,71],
        [1001,3,65],
        [1001,4,60],
        [1002,1,40],
        [1002,2,55],
        [1002,3,64],
        [1002,4,50],
        [1003,1,45],
        [1003,2,39],
        [1003,3,60],
        [1003,4,65],
        [1004,1,83],
        [1004,2,77],
        [1004,3,91],
        [1004,4,74],
        [1005,1,83],
        [1005,2,77],
        [1005,4,74],
       ]
		
columns = ["StudentId", "SubjectId", "Marks"]

df = spark.createDataFrame(data, columns)

df.createOrReplaceTempView("tbl")


%sql

select studentId, 
max(case when SubjectId = 1 then Marks else 0 end) as SubjectId1,
max(case when SubjectId = 2 then Marks else 0 end) as SubjectId2,
max(case when SubjectId = 3 then Marks else 0 end) as SubjectId3,
max(case when SubjectId = 4 then Marks else 0 end) as SubjectId4,
case when min(Marks) < 40 or count (distinct SubjectId)< 4 then 'Fail' else 'Pass' end as Result
from tbl group by studentId

+---------+--------+--------+--------+--------+----------+------+
|StudentId|Subject1|Subject2|Subject3|Subject4|TotalMarks|Result|
+---------+--------+--------+--------+--------+----------+------+
|1001     |60      |71      |65      |60      |256       |Pass  |
|1002     |40      |55      |64      |50      |209       |Pass  |
|1003     |45      |39      |60      |65      |209       |Fail  |
|1004     |83      |77      |91      |74      |325       |Pass  |
|1005     |80      |70      |0       |75      |225       |Fail  |
+---------+--------+--------+--------+--------+----------+------+

