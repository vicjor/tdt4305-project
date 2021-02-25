from constants import *

# Create a graph of posts and comments.
# Nodes are users, and there is an edge from node 𝑖 to node 𝑗 if 𝑖 wrote a comment for 𝑗’s post.
# Each edge has a weight 𝑤𝑖𝑗 that is the number of times 𝑖 has commented a post by 𝑗


def task3(spark, sc):

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
