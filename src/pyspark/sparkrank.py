import re
import sys
from operator import add

from pyspark.sql import SparkSession

def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    if rank is None:
        rank = 0.15
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: pagerank <file> <bucket_name> <iteration_count>", file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx", file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    # Extract input file name, bucket name and iteration count for the output.
    inputFile = sys.argv[1]
    bucketName = sys.argv[2]
    iterations = int(sys.argv[3])

    # Read input file.
    lines = spark.read.text(inputFile).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    for _ in range(iterations):
        contribs = links.leftOuterJoin(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Find the URL with the maximum rank.
    maxRankURL = ranks.max(key=lambda x: x[1])

    # Define paths based on bucket name and iteration count.
    outputPathMaxRank = f"gs://{bucketName}/out/spark/maxRank"
    outputPathAllRanks = f"gs://{bucketName}/out/spark/allRanks"

    # Save the results to GCS.
    ranks.filter(lambda x: x[1] == maxRankURL[1]).saveAsTextFile(outputPathMaxRank)
    ranks.saveAsTextFile(outputPathAllRanks)

    spark.stop()
