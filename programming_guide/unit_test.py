import unittest
from pyspark import SparkContext


class MyTest(unittest.TestCase):
    def testMapReduce(self):
        rdd = sc.parallelize(range(10))
        num = rdd.map(lambda x: x**2).reduce(lambda x, y: x + y)

        self.assertEqual(285, num)


if __name__ == "__main__":
    sc = SparkContext("local", "Unit Test")
    try:
        unittest.main()
    except Exception as e:
        raise e
    finally:
        sc.stop()
