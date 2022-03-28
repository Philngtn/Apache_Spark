from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    spentAmount = fields[2]
    return (int(customerID), float(spentAmount))

lines = sc.textFile("file:///sparkcourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
reducedKey = parsedLines.reduceByKey(lambda x, y: x + y)
reducedKeySorted = reducedKey.sortByKey()
results = reducedKeySorted.collect()

for result in results:
    customerID = result[0]
    totalSpent = round(result[1],2)
    print(str(customerID) + ", " + str(totalSpent))
