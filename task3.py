from constants import *
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType

# Create a graph of posts and comments.
# Nodes are users, and there is an edge from node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post.
# Each edge has a weight 𝑤𝑖𝑗 that is the number of times 𝑖 has commented a post by 𝑗


class Node:
    def __init__(self, user) -> None:
        self.user1 = int(user)

    def increment_weight(self):
        self.weight += 1

    def set_user2(self, user2):
        self.user2 = int(user2)
        self.weight = 1

    def get_weight(self):
        return self.weight

    def get_user1(self):
        return self.user

    def get_user2(self):
        return self.user2


def task3(spark, sc):
    posts_file = sc.textFile(FOLDER_NAME + POSTS_FILE_NAME)
    posts_rdd = posts_file.map(lambda line: line.split(
        "\t")).filter(lambda post: post[0] != '"Id"')

    comments_file = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME)
    # comments = (PostId, UserId)
    comments = comments_file.map(lambda line: line.split(
        "\t")).filter(lambda comment: comment[0] != '"PostId"').map(lambda comment: (comment[0], comment[4]))

    # CommentCount is col no. 11 in csv
    # Filter all posts without comments, map to (PostId, OwnerUserId)
    posts_with_comments = posts_rdd.filter(lambda post: post[11] != "0").map(
        lambda post: (post[0], post[6]))

    # (PostId, (OwnerUserId, CommentUserId))
    posts_and_comment = comments.join(posts_with_comments)

    # (CommentUserId, OwnerUserId)
    commentid_ownerid = posts_and_comment.map(lambda post: (post[1][0], post[1][1]))

    # Add weight (count comments from i to j) :     (CommentUserId, (OwnerUserId, Weight))
    graph = commentid_ownerid.map(lambda row: (
        row, 1)).reduceByKey(lambda a, b: a+b)

    print("\nFirst 10 edges in graph: {}\n".format(graph.take(10)))

    # Convert the result of the previous step into a Spark DataFrame (DF) and answer the following subtasks using DataFrame API, namely using Spark SQL

    temp_graph = graph.map(lambda row: (row[0][0], row[0][1], row[1]))
    print(temp_graph.take(1))
    schema = StructType([
        StructField('CommentOwnerId', StringType(), False),
        StructField('PostOwnerId', StringType(), False),
        StructField('Weight', IntegerType(), False),
    ])
    graphDF = spark.createDataFrame(temp_graph, schema)
    graphDF.createOrReplaceTempView("users")
    print(spark.sql("SELECT * from users LIMIT 10").show())

    # Find the user ids of top 10 users who wrote the most comments

    return

# (user_I, , user_J, #comments)
