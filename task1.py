def task1(sc):
    # Initialize folder and filename variables
    folder_name = "./data/"
    posts_file_name = "posts.csv.gz"
    users_file_name = "users.csv.gz"
    comments_file_name = "comments.csv.gz"
    badges_file_name = "badges.csv.gz"

    # Create RDDs for posts, comments, users and badges
    posts_file = sc.textFile(folder_name + posts_file_name)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    users_file = sc.textFile(folder_name + users_file_name)
    users_rdd = users_file.map(lambda line: line.split("\t"))

    comments_file = sc.textFile(folder_name + comments_file_name)
    comments_rdd = comments_file.map(lambda line: line.split("\t"))

    badges_file = sc.textFile(folder_name + badges_file_name)
    badges_rdd = badges_file.map(lambda line: line.split("\t"))

    print("\nTask 1 results: \n")
    print("Posts: {}".format(posts_rdd.count()))
    print("Users: {}".format(users_rdd.count()))
    print("Comments: {}".format(comments_rdd.count()))
    print("Badges: {}\n".format(badges_rdd.count()))
