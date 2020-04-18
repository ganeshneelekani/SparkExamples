

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()

def minmax(it):
    min = max = None
    for val in it:
        if min is None or val < min:
            min = val
        if max is None or val > max:
            max = val
    return min, max


if __name__ == '__main__':

    # read_file=sc.textFile("/home/g/Software/spark/practice/LabData/big.txt")
    # for line in read_file.collect():
    #     print(line)

    my_list = ["I love mahinder singh Dhoni", "I support CSK"]
    map_rdd=sc.parallelize(my_list)

    # Map function applied to each element one by one
    map_value=map_rdd.map(lambda x:x.split(' '))
    print(map_value.collect())

    # flat map applied to all elemet at once and produce result
    map_flat_map_value = map_rdd.flatMap(lambda x: x.split(' '))
    print(map_flat_map_value.collect())

    # filter each element
    map_filter= map_rdd.filter(lambda x:len(x) > 15)
    print(map_filter.collect())

    #mapPartitions
    # mapPartitions() can be used as an alternative to map and foreach()
    # mapPartitions() is called for each partition while map and foreach is called for rdd
    # per partition and not on each element
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    numbers_rdd = sc.parallelize(numbers, 3)
    numbers_rdd_map_partition=numbers_rdd.mapPartitions(minmax)
    print(numbers_rdd_map_partition.collect())

    #MappartionwithIndex
    # It is similar to MapPartition but with one difference that it takes two parameters,
    # the first parameter is the index and second is an iterator through
    # all items within this partition (Int, Iterator < t > ).
    rdd = sc.parallelize([1, 2, 3, 4], 2)
    def f(splitIndex, iterator): yield (splitIndex,sum(iterator))
    numbers_rdd_map_partition_index = rdd.mapPartitionsWithIndex(f)
    print(numbers_rdd_map_partition_index.collect())

    #groupBy
    x = sc.parallelize(["Joseph", "Jimmy", "Tina",
                        "Thomas", "James", "Cory",
                        "Christine", "Jackeline", "Juan"], 3)

    # Applying groupBy operation on x
    y = x.groupBy(lambda word: word[0])
    for t in y.collect():
         print((t[0], [i for i in t[1]]))

    #sortBy
    tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
    tmp1 = [8, 7, 1, 5, 2]
    sort_by=sc.parallelize(tmp1).sortBy(lambda x: x)
    sort_by_tmp = sc.parallelize(tmp).sortBy(lambda x: x[0])
    print(sort_by.collect())
    print(sort_by_tmp.collect())









