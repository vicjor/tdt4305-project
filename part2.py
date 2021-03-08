
from pyspark.sql.functions import lower, col, unbase64, translate, regexp_replace, split, array_remove, expr
from main import init_spark
from constants import *
import os
from itertools import permutations

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
class Window:
    def __init__(self, lst) -> None:
        self.lst = lst
        self.window = lst[0:5]
        self.lower = 0
        self.upper = 5

    def slide(self):
        if self.upper < len(self.lst):
            self.lower += 1
            self.upper += 1
            self.window = self.lst[self.lower:self.upper]
            return True
        else:
            return False
    
    def get_permutations(self):
        return permutations(self.window)


        

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

    # Remove all the 'DOT' characters from the start or the end of each token
    post = post.withColumn("Body", translate(post.Body, '.', ''))

    # Tokenise by splitting at whitespace
    post = post.withColumn("Body", split(post.Body, " "))

    # Remove all tokens < 3 long
    post = post.withColumn("Body", expr("filter(Body, x -> not(length(x) < 3))"))

    # Add the given stop words to the list of stop words

    print("Preparing stopwords...")
    with open('stopwords.txt') as f:
        stopwords = [line.rstrip().replace("'","") for line in f]

    # Remove the stopwords from the sequence of tokens (final step)
    tokens = post.select("Body").rdd.flatMap(lambda token: token).collect()[0]
    for token in tokens:
        if token in stopwords:
            tokens.remove(token)
    # List -> set -> list to have unique tokens in list
    unique_tokens =  list(set(tokens))
    window = Window(unique_tokens)
    print(window.window)
    window.slide()
    p = window.get_permutations()
    


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
