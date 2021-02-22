import base64


def task2(sc):
    folder_name = "./data/"
    posts_file_name = "posts.csv.gz"
    users_file_name = "users.csv.gz"
    comments_file_name = "comments.csv.gz"
    badges_file_name = "badges.csv.gz"

    # Create RDDs for posts, comments, users and badges
    posts_file = sc.textFile(folder_name + posts_file_name)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    comment_header = sc.textFile(folder_name + comments_file_name).first()
    comments_file = sc.textFile(folder_name + comments_file_name)
    comments_no_header = comments_file.filter(lambda line: not str(line).startswith(comment_header))
    comments = comments_no_header.map(lambda line: line.split("\t"))

    users_file = sc.textFile(folder_name + users_file_name)
    users_rdd = users_file.map(lambda line: line.split("\t"))

    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers = posts_rdd.filter(lambda line: line[1] == "2")

    print("Questions: {}".format(questions.count()))
    print("Answers: {}".format(answers.count()))
    print("Comments: {}".format(comments.count()))

    # Decode Q and A's with base64 decoding and strip strings of HTML tags.
    decoded_answers = answers.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_comments = comments.map(lambda line: str(base64.b64decode(line[2]), "utf-8"))

    answer_length = decoded_answers.map(lambda line: len(line))
    question_length = decoded_questions.map(lambda line: len(line))
    comment_length = decoded_comments.map(lambda line: len(line))

    avg_answer_length = answer_length.reduce(lambda a, b: a+b)/answer_length.count()
    avg_queston_length = question_length.reduce(lambda a, b: a+b)/question_length.count()
    avg_comment_length = comment_length.reduce(lambda a, b: a+b)/comment_length.count()

    print("Average answer length: {} characters".format(int(avg_answer_length)))
    print("Average question length: {} characters".format(int(avg_queston_length)))
    print("Average comment length: {} characters".format(int(avg_comment_length)))
