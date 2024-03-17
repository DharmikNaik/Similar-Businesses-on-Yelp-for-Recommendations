import os
from src.config_parser import ConfigParser
from pyspark import SparkContext, RDD
from src.constants import PROJECT_ROOT


def removeHeader(rdd: RDD):
    header = rdd.first()
    rdd = rdd.filter(lambda row: row != header).map(lambda row: row.split(","))
    return rdd

'''
Input RDD elem: (userID, businessID)
Returns:
    users: list of distinct userID
    numUsers: len(users)
'''
def getUsers(rdd: RDD):
    users = rdd.map(lambda row: row[0]).distinct().sortBy(lambda row: row[0]).collect()
    numUsers = len(users)
    return (users, numUsers)

'''
Parameters:
    users: list of userID
Returns:
    userIdx: dict that maps a userID to index
'''
def generateUserIdx(users):
    userIdx = {}
    index = 0
    for userId in users:
        userIdx.update({userId: index})
        index += 1
    return userIdx

'''
Parameters:
    rdd: Elem shape: (userId, businessId)
    userIdx: dict that maps userId to index
Returns:
    businessUserRdd: shape of elem: (businessId, set(userIds)), where userIds are the users that rated businessId
    businessUserMap: dict that maps businessID to list of users that rated the business
'''
def getBusinessRatedUsersMap(rdd, userIdx):
    businessUserRdd = rdd.map(lambda row: (row[1], userIdx[row[0]])) \
        .groupByKey() \
        .sortByKey() \
        .mapValues(lambda row: set(row))
    # post transform, shape of elem: (businessId, set(userIds)), where userIds are the users that rated businessId

    businessUserList = businessUserRdd.collect()

    businessUserMap = {}

    for businessUser in businessUserList:
        business = businessUser[0]
        usersRated = businessUser[1]
        businessUserMap.update({business: usersRated})

    return businessUserRdd, businessUserMap


def loadData(config: ConfigParser, sc: SparkContext):
    inputFilepath = os.path.join(PROJECT_ROOT, config['file_paths']['input_path'])
    csv = sc.textFile(inputFilepath)
    rdd = removeHeader(csv)
    # persisting rdd in memory to avoid frequent re-computations as frequent access is anticipated
    rdd.persist()
    users, numUsers = getUsers(rdd)
    userIdx = generateUserIdx(users)
    businessRatedUsersRdd, businessRatedUsersMap = getBusinessRatedUsersMap(rdd, userIdx)
    return businessRatedUsersRdd, businessRatedUsersMap