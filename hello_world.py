#!/usr/bin/python  
#-*- coding:utf-8 -*-  
############################  
#File Name: hello_world.py
#Author: wanhongfei  
#Email: wanhongfei@bytedance.com  
#Created Time: 2018-06-24 12:10:52
############################

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

#（a）利用list创建一个RDD;使用sc.parallelize可以把Python list，NumPy array或者Pandas Series,Pandas DataFrame转成Spark RDD。
#rdd = sc.parallelize([1,2,3,4,5])
#print rdd
#Output:ParallelCollectionRDD[0] at parallelize at PythonRDD.scala:480

#（b）getNumPartitions()方法查看list被分成了几部分
#print rdd.getNumPartitions() 
#Output:4

#（c）glom().collect()查看分区状况
#print rdd.glom().collect()
#Output:[[1], [2], [3], [4, 5]]
#collect()方法很危险，数据量上BT文件读入会爆掉内存

#rdd=sc.textFile("file:///home/ubuntu/pyspark/one_day.txt")
#print rdd.first()

#numbersRDD = sc.parallelize(range(1,10+1))
#print(numbersRDD.collect())

#map()对RDD的每一个item都执行同一个操作
#squaresRDD = numbersRDD.map(lambda x: x**2)  # Square every number
#print(squaresRDD.collect())

#filter()筛选出来满足条件的item
#filteredRDD = numbersRDD.filter(lambda x: x % 2 == 0)  # Only the evens
#print(filteredRDD.collect())

#flatMap() 对RDD中的item执行同一个操作以后得到一个list，然后以平铺的方式把这些list里所有的结果组成新的list
#sentencesRDD=sc.parallelize(['Hello world','My name is Patrick'])
#wordsRDD=sentencesRDD.flatMap(lambda sentence: sentence.split(" "))
#print(wordsRDD.collect())
#print(wordsRDD.count())

#def doubleIfOdd(x):
#    if x % 2 == 1:
#        return 2 * x
#    else:
#        return x
#
#numbersRDD = sc.parallelize(range(1,10+1))
#resultRDD = (numbersRDD          
#        .map(doubleIfOdd)    #map,filter,distinct()
#        .filter(lambda x: x > 6)
#        .distinct())    #distinct()对RDD中的item去重
#resultRDD.collect()

#rdd=sc.parallelize(["Hello hello", "Hello New York", "York says hello"])
#resultRDD=(rdd
#        .flatMap(lambda sentence:sentence.split(" ")) # Hello,hello,Hello,New,York,York ... 
#        .map(lambda word:word.lower()) # hello,hello,hello ...
#        .map(lambda word:(word, 1))    #将word映射成(word,1)
#        .reduceByKey(lambda x, y: x + y))  #reduceByKey对所有有着相同key的items执行reduce操作
#print resultRDD.collect()
#print resultRDD.collectAsMap()
#
#取出现频次最高的2个词
#print(resultRDD
#        .sortBy(lambda x: x[1], ascending=False)
#        .take(2))

# Home of different people
homesRDD = sc.parallelize([
        ('Brussels', 'John'),
        ('Brussels', 'Jack'),
        ('Leuven', 'Jane'),
        ('Antwerp', 'Jill'),
    ])

# Quality of life index for various cities
lifeQualityRDD = sc.parallelize([
        ('Brussels', 10),
        ('Antwerp', 7),
        ('RestOfFlanders', 5),
    ])

print homesRDD.join(lifeQualityRDD).collect()   #join

#Output:
#[('Antwerp', ('Jill', 7)),
# ('Brussels', ('John', 10)),
# ('Brussels', ('Jack', 10))]

print homesRDD.leftOuterJoin(lifeQualityRDD).collect()   #leftOuterJoin

#Output:
#[('Antwerp', ('Jill', 7)),
# ('Leuven', ('Jane', None)),
# ('Brussels', ('John', 10)),
# ('Brussels', ('Jack', 10))]

print homesRDD.rightOuterJoin(lifeQualityRDD).collect()   #rightOuterJoin

#Output:
#[('Antwerp', ('Jill', 7)),
# ('RestOfFlanders', (None, 5)),
# ('Brussels', ('John', 10)),
# ('Brussels', ('Jack', 10))]

print homesRDD.cogroup(lifeQualityRDD).collect()   #cogroup

#Output:
#[('Antwerp',
#  (<pyspark.resultiterable.ResultIterable at 0x73d2d68>,
#   <pyspark.resultiterable.ResultIterable at 0x73d2940>)),
# ('RestOfFlanders',
#  (<pyspark.resultiterable.ResultIterable at 0x73d2828>,
#   <pyspark.resultiterable.ResultIterable at 0x73d2b70>)),
# ('Leuven',
#  (<pyspark.resultiterable.ResultIterable at 0x73d26a0>,
#   <pyspark.resultiterable.ResultIterable at 0x7410a58>)),
# ('Brussels',
#  (<pyspark.resultiterable.ResultIterable at 0x73d2b38>,
#   <pyspark.resultiterable.ResultIterable at 0x74106a0>))]
# Oops!  Those <ResultIterable>s are Spark's way of returning a list
# that we can walk over, without materializing the list.
# Let's materialize the lists to make the above more readable:
print (homesRDD
 .cogroup(lifeQualityRDD)
 .map(lambda x:(x[0], (list(x[1][0]), list(x[1][1]))))
 .collect())
