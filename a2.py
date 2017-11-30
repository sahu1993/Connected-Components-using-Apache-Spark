
from pyspark import SparkConf, SparkContext
import sys
import itertools

def init(line) :
    l = line.split(" ")
    v = [ int(x) for x in l ]
    yield v[0], v[1]
    yield v[1], v[0]

def largeStarInit(record) :
    a,b = record
    yield (a,b)
    yield (b,a)

def smallStarInit(record) :
    a,b = record
    if b <= a:
        yield a,b
    else:
        yield b,a

def largeStar(record):
    a, b = record
    t_list = list(b)
    t_list.append(a)
    list_min = min(t_list)
    for x in b:
        if a <= x:
            yield x,list_min

def smallStar(record):
    a, b = record
    b = list(b)
    b.append(a)
    list_min = min(b)
    for x in b:
        yield x,list_min

if __name__ == "__main__":

    conf = SparkConf().setAppName("RDDcreate")
    sc = SparkContext(conf = conf)
    lines = sc.textFile(sys.argv[1])
    firstIteration = True;

    for i in itertools.count():
        if (firstIteration):
            largeStarinit_rdd = lines.flatMap(init)
            firstIteration = False
        else:
            largeStarinit_rdd = smallStar_rdd.flatMap(largeStarInit)

        largeStar_rdd = largeStarinit_rdd.groupByKey().flatMap(largeStar).distinct()

        smallStarinit_rdd = largeStar_rdd.flatMap(smallStarInit)
        smallStar_rdd = smallStarinit_rdd.groupByKey().flatMap(smallStar).distinct()

        changes = (largeStar_rdd.subtract(smallStar_rdd).union(smallStar_rdd.subtract(largeStar_rdd))).count()
        if changes == 0 :
            break

    largeStar_rdd.map(lambda (a,b): "{0} {1}".format(a,b)).coalesce(1).saveAsTextFile("Output")
    sc.stop()
