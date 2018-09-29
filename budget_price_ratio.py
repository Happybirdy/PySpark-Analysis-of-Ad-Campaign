import sys
from pyspark import SparkContext
#from pyspark import *
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import desc
import json

def get_adId_campaignId_bidPrice(line):
    indv_ad = json.loads(line.strip())
    ad_Id = indv_ad['adId']
    campaign_Id = indv_ad['campaignId']
    ad_price = indv_ad['bidPrice']

    bid_price_array = []
    bid_price_array.append(ad_Id)
    bid_price_array.append(campaign_Id)
    bid_price_array.append(ad_price)

    return bid_price_array  # In this case, yield and return are equivalent.

def get_budget_campaignId(line):
    indv_campaign = json.loads(line.strip())
    budget = indv_campaign['budget']
    campaign_Id = indv_campaign['campaignId']

    budget_campaign_array = []
    budget_campaign_array.append(budget)
    budget_campaign_array.append(campaign_Id)

    return budget_campaign_array  # In this case, yield and return are equivalent.

if __name__ == "__main__":
    ad_file = sys.argv[1] #raw Ads file "ads_0502.txt"
    budget_file = sys.argv[2] # raw budget file "budget.txt"
    sc = SparkContext(appName="budget_price_ratio")
    spark = SparkSession(sc)

    ad_data = sc.textFile(ad_file).map(lambda line: get_adId_campaignId_bidPrice(line))
    ad_data_df = spark.createDataFrame(ad_data,['adId','campaignId','bidPrice'])

    budget_data = sc.textFile(budget_file).map(lambda line: get_budget_campaignId(line))
    budget_data_df = spark.createDataFrame(budget_data,['budget','campaignId'])

    # this will cause duplicated columns of campaignId
    #budget_price_ratio = ad_data_df.join(budget_data_df, ad_data_df.campaignId == budget_data_df.campaignId, "inner")
    budget_price_ratio_df = ad_data_df.join(budget_data_df, "campaignId", "inner")\
        .withColumn("budget_price_ratio", (F.col("budget") / F.col("bidPrice")))\
        .drop("campaignId").drop("budget").drop("bidPrice")\
        .orderBy(desc("budget_price_ratio"))\
        #.collect()
    #budget_price_ratio.printSchema()


    #budget_price_ratio.collect().saveAsTextFile("budget_price_ratio_output")
    budget_price_ratio_df.write.format("csv").save("budget_price_ratio_output")
    sc.stop()
