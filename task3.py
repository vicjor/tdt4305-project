from constants import *

# Create a graph of posts and comments.
# Nodes are users, and there is an edge from node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post.
# Each edge has a weight 𝑤𝑖𝑗 that is the number of times 𝑖 has commented a post by 𝑗


def task3(spark, sc):
    posts_file = sc.textFile(FOLDER_NAME + POSTS_FILE_NAME)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    comment_header = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME).first()
    comments_file = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME)
    comments_no_header = comments_file.filter(lambda line: not str(line).startswith(comment_header))
    # comments = (PostId, UserId)
    comments = comments_no_header.map(lambda line: line.split(
        "\t")).map(lambda comment: (comment[0], comment[4]))

    # CommentCount is col no. 11 in csv
    # Filter all posts without comments, map to (PostId, OwnerUserId, CommentCount)
    posts_with_comments = posts_rdd.filter(lambda post: post[11] != "0").map(
        lambda post: (post[0], post[6], post[11]))

    # graph = [ [OwnerUserId, CommentUserId, weight] ]
    graph = []
    print(posts_with_comments.count())

    comment_list = comments.collect()
    # for comment in comment_list:
    #     if comment[0] in post_ids:
    #         if comment[1] in graph:
    #             graph
    # usersDF = spark.read.csv(FOLDER_NAME + "users.csv", header=True, sep="\t")
    # usersDF.createOrReplaceTempView("users")
    # print(spark.sql("SELECT * FROM users LIMIT 5").show())
    return

# (user_I, , user_J, #comments)


class Node:
    def __init__(self, user1, user2, weight) -> None:
        self.user1 = user1
        self.user2 = user2
        self.weight = weight
