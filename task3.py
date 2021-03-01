from constants import *
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, DecimalType
from pyspark.sql import Row
import os


def task3(spark, dataset):
    # Task 3.1:
    # Create a graph of posts and comments.
    # Nodes are users, and there is an edge from node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post.
    # Each edge has a weight 𝑤𝑖𝑗 that is the number of times 𝑖 has commented a post by 𝑗

    posts_rdd = dataset[POSTS_FILE_NAME]
    users_rdd = dataset[USERS_FILE_NAME].filter(lambda line: not line[0] == '"Id"')
    # Comments: (PostId, UserId)
    comments_rdd = dataset[COMMENTS_FILE_NAME].filter(
        lambda comment: comment[0] != '"PostId"').map(lambda comment: (comment[0], comment[4]))

    # CommentCount is col no. 11 in csv
    # Filter all posts without comments, map to (PostId, OwnerUserId)
    posts_with_comments = posts_rdd.filter(lambda post: post[11] != "0").map(
        lambda post: (post[0], post[6]))

    # (PostId, (OwnerUserId, CommentUserId))
    posts_and_comment = comments_rdd.join(posts_with_comments)

    # (CommentUserId, OwnerUserId)
    commentid_ownerid = posts_and_comment.map(lambda post: (post[1][0], post[1][1]))

    # Add weight (count comments from i to j) :     (CommentUserId, (OwnerUserId, Weight))
    temp_graph = commentid_ownerid.map(lambda row: (
        row, 1)).reduceByKey(lambda a, b: a+b)

    # Task: 3.2 Convert the result of the previous step into a Spark DataFrame (DF) and answer the following subtasks using DataFrame API, namely using Spark SQL

    graph = temp_graph.map(lambda row: (row[0][0], row[0][1], row[1]))
    print("\nFirst 10 edges in graph: {}\n".format(graph.take(10)))

    # DF schema to include name of columns
    schema = StructType([
        StructField('CommentOwnerId', StringType(), False),
        StructField('PostOwnerId', StringType(), False),
        StructField('Weight', IntegerType(), False),
    ])
    graphDF = spark.createDataFrame(graph, schema)
    graphDF.createOrReplaceTempView("users")
    spark.sql("SELECT * from users LIMIT 10").show()

    # Task 3.3 Find the user ids of top 10 users who wrote the most comments

    spark.sql("SELECT CommentOwnerId, SUM(Weight) as Amount_of_comments FROM users GROUP BY CommentOwnerId ORDER BY Amount_of_comments DESC LIMIT 10").show()

    # Task 3.4 Find the display names of top 10 users who their posts received the greatest number
    # of comments.

    # First dataframe
    userIds_with_most_comments = spark.sql(
        "SELECT PostOwnerId, SUM(Weight) as Amount_of_comments_received FROM users GROUP BY PostOwnerId ORDER BY Amount_of_comments_received DESC LIMIT 10")

    users = users_rdd.map(lambda x: (x[0], x[3]))
    schemaUsers = StructType([
        StructField('UserId', StringType(), False),
        StructField('Username', StringType(), False),
    ])

    # Second dataframe
    usersDF = spark.createDataFrame(users, schemaUsers)

    # First dataframe loaded into second dataframe
    resultDF = userIds_with_most_comments.join(
        usersDF, userIds_with_most_comments.PostOwnerId == usersDF.UserId).drop(userIds_with_most_comments.PostOwnerId)
    resultDF.show()

    # Save the DF containing the information for the graph of posts and comments (from subtask 2)
    # into a persistence format (like CSV) on your filesystem so that later could be loaded back into a Spark application’s workspace

    try:
        graphDF.repartition(1).write.option("header", "true").format(
            "com.databricks.spark.csv").save("graph.csv")
        print("The graph has been saved as 'graph.csv' in {}".format(os.getcwd()))
    except:
        print("Tried to save 'graph.csv' to {} but file already exists.".format(os.getcwd()))
    return
