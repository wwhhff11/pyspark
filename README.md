# pyspark

# transmations
# list
map() 对RDD的每一个item都执行同一个操作
flatMap() 对RDD中的item执行同一个操作以后得到一个list，然后以平铺的方式把这些list里所有的结果组成新的list
filter() 筛选出来满足条件的item
distinct() 对RDD中的item去重
sample() 从RDD中的item中采样一部分出来，有放回或者无放回
sortBy() 对RDD中的item进行排序

# k-v pair 结构
reduceByKey(): 对所有有着相同key的items执行reduce操作
groupByKey(): 返回类似(key, listOfValues)元组的RDD，后面的value List 是同一个key下面的
sortByKey(): 按照key排序
countByKey(): 按照key去对item个数进行统计
collectAsMap(): 和collect有些类似，但是返回的是k-v的字典

# rdd 运算
# 1. rdd 简单运算
rdd1.union(rdd2): 所有rdd1和rdd2中的item组合（并集）
rdd1.intersection(rdd2): rdd1 和 rdd2的交集
rdd1.substract(rdd2): 所有在rdd1中但不在rdd2中的item（差集）
rdd1.cartesian(rdd2): rdd1 和 rdd2中所有的元素笛卡尔乘积（正交和）
# 2. 类似sql运算join
rdd1.join(rdd2)：key 相同进行合并
rdd1.leftOuterJoin(rdd2)
rdd1.rightOuterJoin(rdd2)
rdd1.cogroup(rdd2)：cogroup(otherDataset, [numTasks])是将输入数据集(K, V)和另外一个数据集(K, W)进行cogroup，得到一个格式为(K, (Seq[V], Seq[W]))的数据集。

# 惰性计算，actions方法
# 常见action方法
collect(): 计算所有的items并返回所有的结果到driver端，接着 collect()会以Python list的形式返回结果
first(): 和上面是类似的，不过只返回第1个item
take(n): 类似，但是返回n个item
count(): 计算RDD中item的个数
top(n): 返回头n个items，按照自然结果排序
reduce(): 对RDD中的items做聚合
