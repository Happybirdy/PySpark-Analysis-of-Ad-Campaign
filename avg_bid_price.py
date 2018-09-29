import sys
from pyspark import SparkContext
import json

def get_indv_keyword_price(line):
    indv_ad = json.loads(line.strip())
    ad_price = indv_ad['bidPrice']

    for keyword in indv_ad['keyWords']:
        keyword_price_pair = []
        keyword_price_pair.append(keyword)
        keyword_price_pair.append(ad_price)

        yield keyword_price_pair  # Note that yield has to be used here instead of return.

if __name__ == "__main__":
    file = sys.argv[1] #raw Ads file
    sc = SparkContext(appName="avg_bid_price") # ads_0502.txt

    data = sc.textFile(file)\
        .flatMap(lambda line: get_indv_keyword_price(line))\
        .map(lambda w: (w[0],[1, w[1]]))\
        .reduceByKey(lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1]))\
        .mapValues(lambda k: k[1]/k[0])\
        .sortBy(lambda a: a[1], False)

    data.saveAsTextFile("avg_bid_price_output")

    sc.stop()
