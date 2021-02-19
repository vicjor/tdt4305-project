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

    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers = posts_rdd.filter(lambda line: line[1] == "2")

    print("Questions: {}".format(questions.count()))
    print("Answers: {}".format(answers.count()))

    # Decode Q and A's with base64 decoding and strip strings of HTML tags.
    decoded_answers = answers.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))

    # avg_answer_length = answers.map(lambda line: )

    print(decoded_answers.take(1))
