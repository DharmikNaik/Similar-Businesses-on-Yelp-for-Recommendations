from src.config_parser import config
from src.data_loader import loadData
from src.minhash import convertToSignature
from src.lsh import getSimilarBusinessPairs
from src.out import outputSimilarPairs
from pyspark import SparkContext


def setupSpark() -> SparkContext:
    sc = SparkContext('local[*]', appName="Discover Similar Businesses")
    return sc

def main():
    sc = setupSpark()
    businessRatedUsersRdd, businessRatedUsersMap = loadData(config, sc)
    nUsers = len(businessRatedUsersMap)
    signaturesRdd = convertToSignature(businessRatedUsersRdd, config, nUsers)
    similarPairs = getSimilarBusinessPairs(signaturesRdd, businessRatedUsersMap, config)
    outputSimilarPairs(similarPairs, config)

if __name__ == '__main__':
    main()