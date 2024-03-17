from pyspark import RDD
import itertools
from src.config_parser import ConfigParser
from src.similarity import calculateJaccardSimilarity


'''
Takes a bucket consisting of businesses (>1) that hashed to the same bucket for some band.
Return combinations of size 2 of arg bucket
'''


def generateCandidatePairs(bucketWithBusinesses):
    candidatePairs = sorted(list(itertools.combinations(sorted(bucketWithBusinesses), 2)))
    return candidatePairs


'''
Takes a business and the associated signature (condensed version of the mathematical 
representation of the business) and divides the signature into nBand number of bands 
with each band consisting of nRowsPerBand rows. The idea is to hash each band of each signature
into large number of buckets such that two bands from signatures of two business
hash into same bucket z if both the bands are identical in its values. Instead of using hash, 
we emit a record with the band value, band index and business ID which is then grouped using
band value and band index. This will put candidate similar businesses into the same group.
Band index is used to simulate the notion of having different buckets for different band
numbers so that we don't deem businesses x and y as a candidate for being similar if
band #i of business x and band #j of business y hash into same bucket (i != j)

Parameters:
    businessID: Identifier of business
    signature: Condensed form or fingerprint of the mathematical representation of the business
    nBand: Number of bands the signature is divided into.
    nRowsPerBand: Number of rows in each of the bands
'''


def applyLSH(businessID, signature, nBand, nRowsPerBand):
    bands = list()
    for bandIdx in range(0, nBand):
        band = signature[bandIdx * nRowsPerBand: (bandIdx + 1) * nRowsPerBand]  # hashing into different buckets
        band.insert(0, bandIdx)  # to differentiate buckets using band number
        bands.append((tuple(band), businessID))

    return bands


'''
Parameters:
    signaturesRdd: shape of elem: (businessId, signature: vector/list of ints)
    nBands: used for LSH (refer applyLSH)
    nRowsPerBand: used for LSH (refer applyLSH)
Returns:
    candidatePairs: shape of elem: (business 1, business 2), where business 1 and 2 are a candidate for being similar
'''


def getCandidatePairs(signaturesRdd: RDD, nBands, nRowsPerBand):
    candidates = signaturesRdd.flatMap(lambda row: applyLSH(row[0], list(row[1]), nBands, nRowsPerBand)) \
        .groupByKey() \
        .mapValues(list) \
        .filter(lambda row: len(row[1]) > 1)  # drop buckets where no pair of candidates can be generated
    # post transform: (bucket, list(businesses))

    candidatePairs = candidates.flatMap(lambda row: generateCandidatePairs(sorted(list(row[1])))) \
        .distinct() \
        .persist()
    # post transform, an elem of resultant RDD looks like : (business 1, business 2), where business 1 and 2 are a candidate for being similar

    return candidatePairs


'''
    Parameters:
        candidatePairs: shape of elem: (business 1, business 2), where business 1 and 2 are a candidate for being similar
        businessUserMap: dict that maps businessID to list of users that rated the business
    Returns:
        similarPairs: shape of elem: (business 1, business 2), where business 1 and 2 are similar
'''


def pruneFalsePositives(candidatePairs, businessUserMap, similarityThreshold):
    similarPairs = candidatePairs.map(lambda row: calculateJaccardSimilarity(row, businessUserMap)) \
        .filter(lambda row: row[1] >= similarityThreshold) \
        .sortByKey() \
        .map(lambda row: row[0])
    return similarPairs


'''
    Parameters:
        signaturesRdd: shape of elem: (businessId, signature: vector/list of ints)
        businessUserMap: dict that maps businessID to list of users that rated the business
        config: Represents the configuration
    Returns:
        
'''


def getSimilarBusinessPairs(signatureRdd: RDD, businessUserMap: dict, config: ConfigParser):
    nBands = config.getint('lsh_params', 'bands')
    nRowsPerBand = config.getint('lsh_params', 'rows_per_band')
    candidatePairs = getCandidatePairs(signatureRdd, nBands, nRowsPerBand)
    similarPairs = None

    if (config.getboolean('lsh_params', 'prune_false_positives')):
        similarityThreshold = config.getfloat('similarity_params', 'similarity_threshold')
        similarPairs = pruneFalsePositives(candidatePairs, businessUserMap, similarityThreshold)
    else:
        # we skip the optional second pass over the data to not prune the false positive
        similarPairs = candidatePairs
    similarPairsList = similarPairs.collect()
    return similarPairsList
