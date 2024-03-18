# Finding Similar Businesses on Yelp for Recommendations
The goal is to identify similar businesses based on user ratings, transforming the recommendation system landscape. This task is pivotal for enhancing the accuracy and efficiency of collaborative filtering systems, thereby significantly impacting personalized user experiences in digital platforms.

## Motivation and Introduction to the Solution
In the realm of data mining and recommendation systems, ensuring swift and accurate suggestions is crucial for user retention and satisfaction. Traditional methods often struggle with scalability and performance issues when dealing with vast datasets like Yelp's. The naive solution that involves enumerating pairs take O(n<sup>2</sup>) time just for the enumeration. We employ LSH (Locality sensitive hashing), enabling rapid processing of large-scale data to identify similar items, in this case, businesses, based on user ratings. This method stands out for its ability to scale well and maintains high accuracy in identifying similarities. LSH is an approximate algorithm for nearest-neighbor search that result in false positives and false negatives. Our solution has the ability to take an extra pass over the data to prune the false positives.

Another problem is the size of the mathematical representation of the items. We employ a technique called minhashing that condenses the mathematical representation of an item of size m to a fingerprint representation of size n, where m >> n. This technique further improves upon the avoidance of enumeration of pairs of items using LSH. It not only improve time efficiency but also scales well and circumvents the circumstance where the items are too big to fit in main memory. This conversion to fingerprint preserves similarity of the original representation.

The motivation of this project is to employ techniques like LSH, min hashing and spark's distributed computing to find similar items and hence recommendations at lightning speed. Optimizations excite me!

## Brief Overview of Solution
The solution involves implementing the minhash and LSH algorithm using Yelp's dataset, focusing on a binary rating system where a user's interaction with a business is marked as 1 (rated) or 0 (not rated). The goal is to find business pairs with a Jaccard similarity of 0.5 or higher, indicating a strong similarity in the customer base's preferences. The approach is as follows:


![Discovery of Similar Businesses Approach](https://github.com/DharmikNaik/Similar-Businesses-on-Yelp-for-Recommendations/blob/master/images/DiscoveryOfSimilarBusinessesApproach.drawio.png)


1. Building representation of businesses (items)
Data Processing: Utilizing yelp_train.csv, we transform the user-business interactions into a binary matrix, representing whether a user has rated a business or not.

2. Condensing the above representation of items to create fingerprints or summaries using the minhash algorithm.

    Hashing Strategy: We design a collection of hash functions to create a consistent permutation of the row entries in the characteristic matrix. These functions are crucial for building an efficient signature matrix that summarizes the original data.

    Signature Matrix Construction: The algorithm constructs a signature matrix that reduces the dimensionality of the original data while preserving the similarity information, essential for the next stage of banding.

3. Employing LSH technique (an approximate near-neighbor search algorithm) to efficiently find the candidate similar businesses

    Banding Technique: The signature matrix is divided into bands, and candidate pairs are identified based on identical signatures within at least one band. This step is critical for narrowing down the potential similar pairs efficiently.

    Similarity Assessment: For each candidate pair, we compute the original Jaccard similarity using the binary rating data (not the signatures). Pairs with a similarity of 0.5 or above are considered similar and included in the final output.

4. Output the similar businesses
Result Output: The identified business pairs are saved into a CSV file, maintaining a specific format for automated grading and evaluation based on precision and recall metrics.

## Future Directions
1. Potential room for optimization of the data movement between the nodes in the cluster to improve performance.
2. Currently, the project was building like a prototype. Improve the code base and make it production level.
3. Currently, the project does batch processing of the data. Future direction would be getting a new item representation in a stream and finding similar items from the pool of already seen items. This would involve persisting the hash buckets used in LSH. An even more practical improvement would be to update the representations of the businesses as new ratings are streamed and rehashing the updated signature in the LSH buckets.  
4. Build a comprehensive test suite and a CI pipeline to streamline robust future development and updates.

## References
1. [Mining of Massive Datasets - Jure Leskovec, Anand Rajaraman, Jeff Ullman](http://www.mmds.org/#ver21)
2. "Proceedings. Compression and Complexity of SEQUENCES 1997 (Cat. No.97TB100171)," Proceedings. Compression and Complexity of SEQUENCES 1997 (Cat. No.97TB100171), Salerno, Italy, 1997, pp. iii-, doi: 10.1109/SEQUEN.1997.666897.
