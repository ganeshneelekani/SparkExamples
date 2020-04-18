from pyspark import SparkContext,SparkConf
from operator import add,mul

conf = SparkConf().setAppName("PySpark App").setMaster("local[16]")
sc = SparkContext(conf=conf).getOrCreate()


if __name__ == '__main__':

    #Reduce
    # Reduces the elements of this RDD using the specified commutative and associative binary operator.
    # Currently reduces partitions locally.
    print(sc.parallelize([1, 2, 3, 4, 5]).reduce(add))

    #Collect
    #  Return a list that contains all of the elements in this RDD.

    #aggregate
    #Aggregate the elements of each partition, and then the results for all the partitions,
    # using a given combine functions and a neutral “zero value.”
    #The functions op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation;
    # however, it should not modify t2.
    #The first function (seqOp) can return a different result type, U, than the type of this RDD.
    # Thus, we need one operation for merging a T into an U and one operation for merging two U

    seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
    combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
    print(sc.parallelize([1, 2, 3, 4]).aggregate((5, 1), seqOp, combOp))

    #fold
    print(sc.parallelize([1, 2, 3, 4, 5]).fold(10, add))

    #foreach(f)¶
    def f(x): print(x)
    sc.parallelize([1, 2, 3, 4, 5]).foreach(f)

    #take
    print(sc.parallelize([2, 3, 4, 5, 6]).cache().take(2))