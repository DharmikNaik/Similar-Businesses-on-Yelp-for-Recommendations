from pyspark import RDD
from src.config_parser import ConfigParser
import random


# generate a list of random coefficients of size=total_num_hashes
# while ensuring that the same number doesn't appear multiple times in the list
# nRows = # of rows in characteristic matrix
# nHashes = Size of signatures
def generateRandomCoefficients(nHashes, nRows):
    randomList = []

    while nHashes > 0:
        randomIndex = random.randint(0, nRows)

        # ensure that each random number is unique
        while randomIndex in randomList:
            randomIndex = random.randint(0, nRows)

        randomList.append(randomIndex)
        nHashes -= 1

    return randomList

'''
    Applies minhash algorithm to condense the mathematical representation of a business
    Parameters:
        elems: mathematical representation(set) of business
        nRows: # of rows in the characteristic matrix i.e. # of users
        paramN: len of signature/fingerprint/condensed representation
        coefficientA/B: cofficients used for hash in minHash to simulate permutation
            of rows of characteristic matrix
    Returns:
        hashedValues: signature of the business represented by elems
'''
def minHashArray(elems, nRows, paramN, coefficientA, coefficientB):
    elems = list(elems)
    hashedValues = [float('inf') for i in range(0, paramN)]
    '''
    h(x) = (a*x + b) % c

    x -> input index
    a, b -> random coefficients
    c -> param_num_users
    '''
    for i in range(0, paramN):
        for user in elems:
            hashCode = (coefficientA[i] * user + coefficientB[i]) % nRows
            if hashCode < hashedValues[i]:
                hashedValues[i] = hashCode
    return hashedValues

'''
    Parameters:
        rdd: shape of elem: (businessId, set(userIds)), where userIds are the users that rated businessId
    Returns:
        signatures: shape of elem: (businessId, signature: vector/list of ints)
'''
def convertToSignature(rdd: RDD, config: ConfigParser, nUsers: int):
    signatureLen = config.getint('min_hash_params', 'signature_size')
    coefficientA = generateRandomCoefficients(signatureLen, nUsers)
    coefficientB = generateRandomCoefficients(signatureLen, nUsers)
    signatures = rdd.mapValues(lambda row: minHashArray(row, nUsers, signatureLen, coefficientA, coefficientB))
    return signatures