from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    positive = []
    negative = []
    time = []

    for index, value in enumerate(counts[1:]):
        positive.append(value[0][1])
        negative.append(value[1][1])
        time.append(index)

    plot1 = plt.plot(time, positive, "bo-", label="Positive")
    plot2 = plt.plot(time, negative, "ro-", label="Negative")
    plt.axis([-2, len(time) + 2, 0, max(max(positive), max(negative)) + 30])
    plt.xlabel("Time Step")
    plt.ylabel("Word Count")
    plt.legend(loc=4)
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    word_list = []
    fp = open(filename, "r")
    data = fp.read().split('\n')
    for word in data:
        word_list.append(word)
    fp.close()
    return word_list

def updateFunction(newValues, runningCount):
    return (sum(newValues) + (runningCount or 0))

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    words = tweets.flatMap(lambda x: x.split(" "))
    sentiments_counts = words.map(lambda x: ('positive', 1) if x in pwords else ('positive', 0))\
                .union(words.map(lambda x: ('negative', 1) if x in nwords else ('positive', 0)))\
                .reduceByKey(lambda x, y: x + y)
    #sentiments_counts.pprint()
    sentiments_counts_all = sentiments_counts.updateStateByKey(updateFunction)
    sentiments_counts_all.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    sentiments_counts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
