'''
    Calculates jaccard similarity of the candidate pair
    Parameters:
            candidatePair: (business 1, business 2), where business 1 and 2 are a candidate
                for being similar
            businessUserMap: dict that maps businessID to list of users that rated the business
    Returns:
            (candidatePair, jaccardSimilarity), where jaccardSimilarity is the computed jaccard
                similarity of the candidate pair
'''

def calculateJaccardSimilarity(candidatePair, businessUserMap):
    candidate1 = set(businessUserMap.get(candidatePair[0]))
    candidate2 = set(businessUserMap.get(candidatePair[1]))

    lenCandidatesIntersection = len(candidate1.intersection(candidate2))
    lenCandidatesUnion = len(candidate1.union(candidate2))

    jaccardSimilarity = float(lenCandidatesIntersection) / float(lenCandidatesUnion)

    return candidatePair, jaccardSimilarity
