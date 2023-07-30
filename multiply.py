from pyspark import SparkContext

sc = SparkContext("local[*]","bigDataAddCampaign")

if __name__ == "__main__":
    myList = ["WARN: Tuesday 4 September 0405",
              "ERROR: Tuesday 4 September 0408",
              "ERROR: Tuesday 4 September 0408",
              "ERROR: Tuesday 4 September 0408",
              "ERROR: Tuesday 4 September 0408",
              "ERROR: Tuesday 4 September 0408"]

    input_rdd = sc.parallelize(myList)
else:
    input_rdd = sc.textFile("/C:/Users/Manthan/Downloads/bigLog.txt")
    print("inside the else part")

map_input = input_rdd.map(lambda x:(x.split(":")[0],1))

output = map_input.reduceByKey(lambda x,y:x+y)

result = output.collect()

for a in result:
    print(a)
