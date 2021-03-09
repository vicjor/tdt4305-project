
from pyspark.sql.functions import lower, col, unbase64, translate, regexp_replace, split, array_remove, expr
from pyspark.sql import SQLContext
from main import init_spark
from constants import *
import os
from itertools import permutations, combinations
from graphframes import *


"""
You should take the following steps to construct the term graph for the input document:
1. Turn all the characters to lower case
2. Remove all the punctuations (like '!' and '?') except 'DOT' characters
3. Remove all the symbols (like '$' and '>') and special characters (like 'TAB')
4. Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this
stage should have a sequence of tokens
5. Remove the tokens that are smaller than three characters long from the sequence of the tokens
6. Remove all the 'DOT' characters from the start or the end of each token
7. Remove the stopwords from the sequence of tokens (The list of stopwords is available at the end of this
document.)
"""


"""
Initialize a Window class with Window(token_list).
Use Window.run() to get a list of tuples containing all permutations. 
"""


class Window:
    def __init__(self, lst) -> None:
        self.lst = lst
        self.window = lst[0:5]
        self.lower = 0
        self.upper = 5
        self.S = []

    def slide(self):
        if self.upper < len(self.lst):
            self.lower += 1
            self.upper += 1
            self.window = self.lst[self.lower:self.upper]
            return True
        else:
            return False

    def get_combinations(self):
        _ids = [x[0] for x in self.window]
        perm = combinations(_ids, 2)
        for p in perm:
            self.S.append(p)

    def run(self):
        self.get_combinations()
        while self.slide():
            self.get_combinations()
        return self.S


# Punctuation except '.'
punc = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~\t\n'


def graph_of_terms(post, sc):
    # Lowercase all chars
    post = post.withColumn("Body", lower(unbase64(post.Body)))

    # Replace HTML LF tag
    post = post.withColumn("Body", regexp_replace(post.Body, "&#xa;", ' '))
    # Replace HTML tags
    post = post.withColumn("Body", regexp_replace(post.Body, "<[^>]*>", ''))
    # Remove all punctuations
    post = post.withColumn("Body", translate(post.Body, punc, ''))

    # Remove all the 'DOT' characters from the start or the end of each token
    post = post.withColumn("Body", translate(post.Body, '.', ''))

    # Tokenise by splitting at whitespace
    post = post.withColumn("Body", split(post.Body, " "))

    # Remove all tokens < 3 long
    post = post.withColumn("Body", expr(
        "filter(Body, x -> not(length(x) < 3))"))

    # Add the given stop words to the list of stop words

    print("Preparing stopwords...")
    with open('stopwords.txt') as f:
        stopwords = [line.rstrip().replace("'", "") for line in f]

    # Remove the stopwords from the sequence of tokens (final step)
    tokens = list(set(post.select("Body").rdd.flatMap(
        lambda token: token).collect()[0]))

    # Created a new list of tokens excluding stopwords
    vertices = []
    i = 0
    for token in tokens:
        if token not in stopwords:
            i += 1
            vertices.append((i, token))
    window = Window(vertices)

    # Get a list of tuples containing all combinations of each possible window for the set of tokens. Remove duplicates.
    graph = list(set(window.run()))

    # At this point our graph only contains edge (e1 -> e2), not (e2 -> e1)
    # Create a new list to add edges (e2 -> e1)
    edges = []
    for tup in graph:
        edges.append((tup[0], tup[1], 1))
        edges.append((tup[1], tup[0], 1))
    print("VERTCICES", vertices, "\n\n")
    print("Edges", edges, "\n\n")
    # Initiate SQLContext with SparkContext
    sqlContext = SQLContext(sc)
    # Turn list of nodes and edges into dataframes
    v = sqlContext.createDataFrame(vertices, ["id", "term"])
    e = sqlContext.createDataFrame(edges, ["e1", "e2", "weight"])

    g = GraphFrame(v, e)
    # results = g.pageRank(tol=0.0001, resetProbability=0.15)
    # results.vertices.select("id", "pagerank").show()
    # print(results)
    return


def main():
    _id = 9
    spark, sc = init_spark()
    postsdf = spark.read.option("header", "true").option(
        "delimiter", "\t").csv(os.getcwd() + "/data/posts.csv.gz")

    # Drop all columns but 'Body'
    post = postsdf.filter(postsdf.Id == "9").drop("OwnerUserId", "PostTypeId", "CreationDate", "Title", "Tags",
                                                  "CommentCount", "ClosedDate", "FavoriteCount", "LastActivityDate", "ViewCount", "AnswerCount", "Score", "Id")
    graph_of_terms(post, sc)

    return


if __name__ == "__main__":
    main()
