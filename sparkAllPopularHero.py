# First import SparkConf and SparkContext from pyspark module
from pyspark import SparkConf, SparkContext

# Then, set SparkConf by setting up master as local(means stanalone local) and app Name
sConf = SparkConf().setMaster("local").setAppName("MostPopularHero")

# Then, set SparkContext based on the SparkConf
sContext = SparkContext(conf = sConf)

# python function to return a key value pair got heroes 
def loadHeros():
	heroes={}
	with open("/home/user/bigdata/datasets/Otherdata/marvel-heroes.txt") as heroFile:
		for line in heroFile:
			fields = line.split('\"') 
			heroes[int(fields[0])] = fields[1]
	return heroes 
		
# python function to return a count of occurrences per line for a hero
def processHeroCounts(line):
        fields=line.split()
        heroID=int(fields[0])
        occuranceCount=len(fields) - 1
        return (heroID, occuranceCount)

# python function to print the RDD
def printRDD(results):
        for hero in results:
                heroName = str(hero[0])
                occurrenceCount = int(hero[1])
                print("Hero Name: %s, Occurrence Count: %d" %(heroName, occurrenceCount))
				
# broadcast the hero dictionary
heroesDict= sContext.broadcast(loadHeros())

# read the data file from the marvel networks file
networkData = sContext.textFile("/home/user/bigdata/datasets/Otherdata/marvel-network.txt")
# map the data to create a key value pair
networks = networkData.map(processHeroCounts)
# now reduce by Key to get a sum of all occurrences 
networksByKey = networks.reduceByKey(lambda x, y : (x + y))

#lets sort networksByCountAsKey and print all
networksByKeySorted = networksByKey.map(lambda (x, y) :(heroesDict.value[x], y))
printRDD(networksByKeySorted.top(25, key= lambda x : x[1]))
