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

rdd = sc.parallelize(range(1,10+1))
rdd.reduce(lambda x, y: x + y)  
#reduce的原理：先在每个分区(partition)里完成reduce操作，然后再全局地进行reduce。

squaresRDD = rdd.map(lambda x: x**2)
squaresRDD.cache()  # Preserve the actual items of this RDD in memory
avg = squaresRDD.reduce(lambda x, y: x + y) / squaresRDD.count()
print avg

#有时候需要重复用到某个transform序列得到的RDD结果。但是一遍遍重复计算显然是要开销的，所以我们可以通过一个叫做cache()的操作把它暂时地存储在内存中。缓存RDD结果对于重复迭代的操作非常有用，比如很多机器学习的算法，训练过程需要重复迭代。
