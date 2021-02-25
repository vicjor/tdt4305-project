from constants import *


def task1(sc):
    # Create RDDs for posts, comments, users and badges
    posts_file = sc.textFile(FOLDER_NAME + POSTS_FILE_NAME)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    users_file = sc.textFile(FOLDER_NAME + USERS_FILE_NAME)
    users_rdd = users_file.map(lambda line: line.split("\t"))

    comments_file = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME)
    comments_rdd = comments_file.map(lambda line: line.split("\t"))

    badges_file = sc.textFile(FOLDER_NAME + BADGES_FILE_NAME)
    badges_rdd = badges_file.map(lambda line: line.split("\t"))

    print("\nTask 1 results: \n")
    print("Posts: {}".format(posts_rdd.count()))
    print("Users: {}".format(users_rdd.count()))
    print("Comments: {}".format(comments_rdd.count()))
    print("Badges: {}\n".format(badges_rdd.count()))
