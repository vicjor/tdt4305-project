
from pyspark.sql.functions import lower, col, unbase64, translate, regexp_replace, split
from main import init_spark
from constants import *
import os

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

# Punctuation except '.'
punc = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~\t\n'

def graph_of_terms(post):
    # Lowercase all chars
    post = post.withColumn("Body", lower(unbase64(post.Body)))

    # Replace HTML LF tag
    post = post.withColumn("Body", regexp_replace(post.Body, "&#xa;", ' '))
    # Replace HTML tags
    post = post.withColumn("Body", regexp_replace(post.Body, "<[^>]*>", ''))
    # Remove all punctuations
    post = post.withColumn("Body", translate(post.Body, punc, ''))

    # Tokenise by splitting at whitespace
    post = post.withColumn("Body", split(post.Body, " "))

    # Remove all tokens < 3 long
    post = post.withColumn("Body", post.Body)

    print(post.collect())
    
    return


def main():
    _id = 9
    spark, sc = init_spark()
    postsdf = spark.read.option("header", "true").option("delimiter", "\t").csv(os.getcwd() + "/data/posts.csv.gz")
    post = postsdf.filter(postsdf.Id == "9").drop("OwnerUserId", "PostTypeId", "CreationDate","Title", "Tags", "CommentCount", "ClosedDate","FavoriteCount","LastActivityDate","ViewCount","AnswerCount","Score","Id")
    graph_of_terms(post)
    
    return


if __name__ == "__main__":
    main()
