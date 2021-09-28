# 思路


## 目的

1.思考题：如何避免小文件问题   
如何避免小文件问题？给出2～3种解决方案

2.实现Compact table command  
• 要求：  
添加compact table命令，用于合并小文件，例如表test1总共有50000个文件，
每个1MB，通过该命令，合成为500个文件，每个约100MB。 
• 语法：  
COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES];
• 说明：  
1.如果添加partitionSpec，则只合并指定的partition目录的文件。  
2.如果不加into fileNum files，则把表中的文件合并成128MB大小。  
3.以上两个算附加要求，基本要求只需要完成以下功能：  
COMPACT TABLE test1 INTO 500 FILES;  
代码参考  
SqlBase.g4:  
| COMPACT TABLE target=tableIdentifier partitionSpec?
(INTO fileNum=INTEGER_VALUE identifier)? #compactTable

3.Insert命令自动合并小文件
  
我们讲过AQE可以自动调整reducer的个数，但是正常跑Insert命
令不会自动合并小文件，例如insert into t1 select * from t2;

请加一条物理规则（Strategy），让Insert命令自动进行小文件合
并(repartition)。（不用考虑bucket表，不用考虑Hive表）

代码参考
```scala

object RepartitionForInsertion extends Rule[SparkPlan] {
override def apply(plan: SparkPlan): SparkPlan = {
plan transformDown {
case i @ InsertIntoDataSourceExec(child, _, _, partitionColumns, _)
...
val newChild = ...
i.withNewChildren(newChild :: Nil)
} } }

```