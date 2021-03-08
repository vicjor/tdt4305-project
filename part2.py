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

from pyspark.sql.functions import lower, col, unbase64
import base64
from main import init_spark
from constants import *
import os


def graph_of_terms(postdf):
    decoded_post = postdf.map(lambda line: str(base64.b64decode(line[5]), "utf-8"))
    decoded_post.withColumn("Body", lower(postdf.Body))
    return


def main():
    _id = 9
    spark, sc = init_spark()
    posts_df = spark.read.option("header", "true").option("delimiter", "\t").csv(os.getcwd() + "/data/posts.csv.gz")
    post = posts_df.filter(posts_df.Id == "9").drop("OwnerUserId", "PostTypeId", "CreationDate","Title", "Tags", "CommentCount", "ClosedDate","FavoriteCount","LastActivityDate","ViewCount","AnswerCount","Score","Id")
    # Decode and lower characters
    post.withColumn("Body", lower(unbase64(post.Body)))
    print(post)
    return


if __name__ == "__main__":
    main()
