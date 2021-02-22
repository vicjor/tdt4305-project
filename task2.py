import base64
from datetime import datetime as dt


def str_to_time(datestring):
    return dt.strptime(datestring, "%Y-%m-%d %H:%M:%S")


def get_user_post_count(posts, post, user):
    return posts.filter(lambda a: a[6] == user[0]).count()


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
    print("Comments: {}\n".format(comments.count()))

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
    print("Average comment length: {} characters\n".format(int(avg_comment_length)))

    # Task 2.2 Find the dates when the first and the last questions were asked. Also, find the display
    # name of users who posted those questions

    # Use str_to_time to convert string to date object and compare each object with each other to find min/max
    newest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) > str_to_time(b[2]) else b)
    oldest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) < str_to_time(b[2]) else b)

    print("Newest question: {}".format(newest_question[2]))
    print("Oldest question: {}\n".format(oldest_question[2]))

    # Task 2.3 Find the ids of users who wrote the greatest number of answers and questions. Ignore
    # the user with OwnerUserId equal to -1

    # Group posts by UserId, then count number of posts for each user and reduce to find the userId with the most answers
    most_answers = answers.groupBy(lambda line: line[6]).map(lambda x: (
        x[0], len(list(x[1])))).sortBy(lambda x: x[1]).reduce(lambda a, b: a if a[1] > b[1] else b)
    # UserId for user with most questions is NULL. Therefore filter UserIDs on predicate not NULL.
    most_questions = questions.groupBy(lambda line: line[6]).filter(lambda x: x[0] != "NULL").map(lambda x: (
        x[0], len(list(x[1])))).sortBy(lambda x: x[1]).reduce(lambda a, b: a if a[1] > b[1] else b)

    print("UserID for user with the most answers: {}\nNumber of answers: {}\n\n".format(
        most_answers[0], most_answers[1]))
    print("UserID for user with the most questions: {}\nNumber of questions: {}".format(
        most_questions[0], most_questions[1]))
