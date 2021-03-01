from constants import *


def task1(dataset):
    print("Loading data into RDDs...\n")
    # Create RDDs for posts, comments, users and badges

    posts_rdd = dataset[POSTS_FILE_NAME]
    users_rdd = dataset[USERS_FILE_NAME]
    comments_rdd = dataset[COMMENTS_FILE_NAME]
    badges_rdd = dataset[BADGES_FILE_NAME]

    print("\nTask 1 results: \n")
    print("Posts: {}".format(posts_rdd.count()))
    print("Users: {}".format(users_rdd.count()))
    print("Comments: {}".format(comments_rdd.count()))
    print("Badges: {}\n".format(badges_rdd.count()))
