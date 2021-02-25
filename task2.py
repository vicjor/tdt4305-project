import base64
from datetime import datetime as dt
import math
from constants import *


def str_to_time(datestring):
    return dt.strptime(datestring, "%Y-%m-%d %H:%M:%S")


def pearson_corr(users):
    upvotes = users.map(lambda x: x[7])
    downvotes = users.map(lambda x: x[8])

    average_upvotes = upvotes.reduce(lambda x, y: int(x) + int(y))/upvotes.count()
    average_downvotes = downvotes.reduce(lambda x, y: int(x) + int(y))/downvotes.count()

    upvotes_list = upvotes.collect()
    downvotes_list = downvotes.collect()

    teller = sum([(int(upvotes_list[i])-average_upvotes) * (int(downvotes_list[i]) -
                                                            average_downvotes) for i in range(len(upvotes_list))])

    std_upvotes = math.sqrt(sum([(int(x) - average_upvotes)**2 for x in upvotes_list]))
    std_downvotes = math.sqrt(sum([(int(x) - average_downvotes)**2 for x in downvotes_list]))

    return teller / (std_upvotes*std_downvotes)


def entropy(comments):
    user_in_comments = comments.map(lambda a: (a[4], 1)).reduceByKey(lambda a, b: a + b).collect()
    length_comments = len(comments.collect())
    return -sum([(user_in_comments[i][1] / length_comments)*math.log(user_in_comments[i][1] / length_comments, 2) for i in range(len(user_in_comments))])


def task2(sc):
    # Create RDDs for posts, comments, users and badges
    posts_file = sc.textFile(FOLDER_NAME + POSTS_FILE_NAME)
    posts_rdd = posts_file.map(lambda line: line.split("\t"))

    badges_file = sc.textFile(FOLDER_NAME + BADGES_FILE_NAME)
    badges_rdd = badges_file.map(lambda line: line.split("\t"))

    comment_header = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME).first()
    comments_file = sc.textFile(FOLDER_NAME + COMMENTS_FILE_NAME)
    comments_no_header = comments_file.filter(lambda line: not str(line).startswith(comment_header))
    comments = comments_no_header.map(lambda line: line.split("\t"))

    users_header = sc.textFile(FOLDER_NAME + USERS_FILE_NAME).first()
    users_file = sc.textFile(FOLDER_NAME + USERS_FILE_NAME)
    users_no_header = users_file.filter(lambda line: not str(line).startswith(users_header))
    users_rdd = users_no_header.map(lambda line: line.split("\t"))

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
    # Different approach to perform same operations on the questions, using reduceByKey.
    most_questions = questions.map(lambda a: (a[6], 1)).filter(lambda x: x[0] != "NULL")\
        .reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).reduce(lambda a, b: a if a[1] > b[1] else b)

    print("UserID for user with the most answers: {}\nNumber of answers: {}\n\n".format(
        most_answers[0], most_answers[1]))
    print("UserID for user with the most questions: {}\nNumber of questions: {}\n".format(
        most_questions[0], most_questions[1]))

    # Map badges into (UserID, counter), reduce by key to count number of occurences for each user and then filter < 3
    less_than_three_badges = badges_rdd.map(lambda badge: (badge[0], 1)).reduceByKey(
        lambda a, b: a + b).filter(lambda x: x[1] < 3)

    print("Users with less than three badges: {}".format(less_than_three_badges.count()))

    # Task 2.5: Calculate the Pearson correlation coefficient (or Pearson’s r) between the number of upvotes and downvotes cast by a user.

    print("\nPearson correlation coefficient: {}".format(round(pearson_corr(users_rdd), 3)))

    # Task 2.6: Calculate the entropy of id of users (that is UserId column from comments data) who
    # wrote one or more comments.

    print("\nEntropy: {}".format(round(entropy(comments), 3)))
