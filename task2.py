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

    teller = users.map(lambda upvote: (
        int(upvote[7]) - average_upvotes) * (int(upvote[8]) - average_downvotes)).reduce(lambda a, b: a+b)

    std_upvotes = users.map(lambda x: (
        int(x[7]) - average_upvotes)**2).reduce(lambda a, b: a+b)**0.5
    std_downvotes = users.map(lambda x: (
        int(x[8]) - average_downvotes)**2).reduce(lambda a, b: a+b)**0.5
    nevner = std_upvotes*std_downvotes

    return teller / nevner


def entropy(comments):
    user_in_comments = comments.map(lambda a: (a[4], 1)).reduceByKey(lambda a, b: a + b)
    length_comments = comments.count()
    return -user_in_comments.map(lambda x: x[1] / length_comments *
                                 math.log(x[1] / length_comments, 2)).reduce(lambda a, b: a+b)


def task2(dataset):
    # Create RDDs for posts, comments, users and badges

    posts_rdd = dataset[POSTS_FILE_NAME]
    badges_rdd = dataset[BADGES_FILE_NAME]
    users_rdd = dataset[USERS_FILE_NAME].filter(lambda line: not line[0] == '"Id"')
    comments_rdd = dataset[COMMENTS_FILE_NAME].filter(lambda line: not line[0] == '"PostId"')

    # Line[1] is PostTypeId. 1 for questions, 2 for answers.
    questions = posts_rdd.filter(lambda line: line[1] == "1")
    answers = posts_rdd.filter(lambda line: line[1] == "2")

    print("Questions: {}".format(questions.count()))
    print("Answers: {}".format(answers.count()))
    print("Comments: {}\n".format(comments_rdd.count()))

    # Decode Q and A's with base64 decoding and strip strings of HTML tags.
    decoded_answers = answers.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
    decoded_comments = comments_rdd.map(lambda line: str(base64.b64decode(line[2]), "utf-8"))

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
    newest_questioner_name = users_rdd.filter(
        lambda user: user[0] == newest_question[6]).collect()[0]
    oldest_questioner_name = users_rdd.filter(
        lambda user: user[0] == oldest_question[6]).collect()[0]
    print("Newest question: {} by {}".format(newest_question[2], newest_questioner_name[3]))
    print("Oldest question: {} by {}\n".format(oldest_question[2], oldest_questioner_name[3]))

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

    print("\nEntropy: {}".format(round(entropy(comments_rdd), 3)))
