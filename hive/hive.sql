--题目一：展示电影ID为2116这部电影各年龄段的平均影评分
select usertbl.age, avg(ratetbl.rate) as avgrate from hive_sql_test1.t_rating  ratetbl
inner join (select userid,age from hive_sql_test1.t_user  ) usertbl on  ratetbl.userid = usertbl.userid
where t_rating.movieid = 2116
group by age order by age

--题目二：找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数
select sex,name,avgrate, total from(
select sex , moviename  as name , avg(ratetbl.rate)  as avgrate  ,count(*) as total from t_movie movietbl
inner join  (select userid,movieid,rate from hive_sql_test1.t_rating) ratetbl on movietbl.movieid = ratetbl.movieid
inner join  (select userid,sex from hive_sql_test1.t_user where sex = 'M') usertbl on  ratetbl.userid = usertbl.userid
group by sex,moviename) tmp where tmp.total > 50
order by avgrate desc limit 10

--题目三： 找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分
select  movietbl.moviename, avg(ratetbl3.rate)
from hive_sql_test1.t_movie movietbl  inner join hive_sql_test1.t_rating ratetbl3 on movietbl.movieid = ratetbl3.movieid
inner join
(select ratetbl2.movieid ,ratetbl2.rate
from hive_sql_test1.t_rating ratetbl2
inner join
(select  ratetbl.userid,count(*) as times  from hive_sql_test1.t_rating ratetbl inner join  hive_sql_test1.t_user usertbl on  ratetbl.userid=usertbl.userid
where usertbl.sex='F' group by ratetbl.userid order by times desc  limit 1 ) tmp on tmp.userid = ratetbl2.userid
order by rate desc limit 10 ) tmp2  on  movietbl.movieid = tmp2.movieid
group by moviename



