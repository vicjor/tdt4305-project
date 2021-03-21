# Big Data Project Part 2

### Victor Jørgensen & Hans Kristian Sande

## Prerequisites

-   Spark 3.0.1
-   graphframes-0.8.1-spark3.0-s_2.12.jar

# Installation

```bash
$ git clone git@github.com:vicjor/tdt4305-project.git
$ cd tdt4305-project
$ python3 -m venv venv
$ . venv/bin/activate # Mac / Linux
$ pip install -r requirements.txt
```

To make graphframes package work we had to download the .jar file manually from [spark-packages](https://spark-packages.org/package/graphframes/graphframes) (graphframes-0.8.1-spark3.0-s_2.12.jar). Then we had to move the .jar file into $SPARK_HOME/jars. The .jar file is included in the repo, so if you encounter the same problem you can copy the file to $SPARK_HOME/jars with:

```bash
$ cp graphframes-0.8.1-spark3.0-s_2.12.jar $SPARK_HOME/jars
```

You should now be able to run the code with

```bash
$ spark-submit main.py --input_path $PWD/data --post_id 14
```

## Stage 1: constructing the graph of terms

### Text processing

Our function `graph_of_terms` takes two arguments, namely a post from _posts.csv.gz_ and the spark-context _sc_

1. Turn all the characters to lower case

    ```python
    post = post.withColumn("Body", lower(unbase64(post.Body)))
    ```

2. Remove all the punctuations (like '!' and '?') except 'DOT' characters

    ```python
    # Punctuation except '.'
    punc = '!"#$%&\'()*+,-/:;<=>?@[\\]^_`{|}~\t\n'

    # Remove all punctuations
    post = post.withColumn("Body", translate(post.Body, punc, ''))
    ```

3. Remove all the symbols (like '\$' and '>') and special characters (like 'TAB')

    _Our previous code snippet solves both subtask 2 and 3_

    ```python
    # Remove all punctuations
    post = post.withColumn("Body", translate(post.Body, punc, ''))
    ```

4. Tokenise the output of the previous step (the separator of tokens is the 'WHITESPACE' character); at this stage should have a sequence of tokens

    ```python
    # Tokenise by splitting at whitespace
    post = post.withColumn("Body", split(post.Body, " "))
    ```

5. Remove the tokens that are smaller than three characters long from the sequence of the tokens

    ```python
    # Remove all tokens < 3 long
    post = post.withColumn("Body", expr("filter(Body, x -> not(length(x) < 3))"))
    ```

6. Remove all the 'DOT' characters from the start or the end of each token

    ```python
    # Remove all the 'DOT' characters from the start or the end of each token
    post = post.withColumn("Body", translate(post.Body, '.', ''))
    ```

7. Remove the stop words from the sequence of tokens by transforming the list to a set, and then the set back to a list to remove duplicates.

    ```python
    # Add the given stop words to the list of stop words
    with open('stopwords.txt') as f:
        stopwords = [line.rstrip().replace("'", "") for line in f]

    # Remove the stopwords from the sequence of tokens
    unique_tokens = list(set(post.select("Body").rdd.flatMap(lambda token: token).collect()[0]))
    tokens = post.select("Body").rdd.flatMap(lambda token: token).collect()[0]
    ```

### Constructing the graph

Iterate through all the unique tokens, and if the token is not a stop word, append it to the list of nodes with a unique ID. At the same time we preserve all the unique integer IDs for each term in a dictionary. This dictionary is then used to create a list with the IDs of each token in the same order as the original token list.

```python
# Created a new list of tokens excluding stopwords
nodes = []
d = {}  # Dict used to save ID for each token in nodes
i = 0
for token in unique_tokens:
    if token not in stopwords:
        i += 1
        nodes.append((i, token))
        d[token] = i
# Need to use IDs not terms as tokens. Create a list where tokens are swapped for their respective ID
id_tokens = []
for token in tokens:
    if token not in stopwords:
        id_tokens.append(d[token])  # Find ID from dict.
```

### Use a sliding window of size 5 tokens over the sequence of tokens to find the relationship between two terms

To generate a list with all relationships between terms we created a Window class. This class takes a list with IDs for each token as input, and the method run() returns a list S containing all possible combinations of terms for each window (except loops).

```python
class Window:

    """
    Initialize a Window class with Window(token_list).
    Use Window.run() to get a list of tuples containing all permutations.
    """

    def __init__(self, lst) -> None:
        self.lst = lst
        self.lower = 0
        self.upper = 5
        self.window = lst[self.lower:self.upper]
        self.S = []

    # Slide window if end of list is not reached
    def slide(self):
        if self.upper < len(self.lst):
            self.lower += 1
            self.upper += 1
            self.window = self.lst[self.lower:self.upper]
            return True
        else:
            return False

    def get_combinations(self):
        # comb contains all 2-combinations possible from current window except loops
        comb = combinations(self.window, 2)
        for c in comb:
            self.S.append(c)

    # Slide window and add combinations to list until window cannot move more
    def run(self):
        self.get_combinations()
        while self.slide():
            self.get_combinations()
        return self.S
```

We initialise the a Window instance, and create a graph containing edges (e1 → e2). We also transform the list to set and back to set again to remove any duplicates that may occur (i.e. if the same window occurs multiple times, multiple identical edges will be added).

```bash
# Initialize Window to generate edges.
window = Window(id_tokens)

# Get a list of tuples containing all combinations of each possible window for the set of tokens. Remove duplicates.
graph = list(set(window.run()))
```

Our next step is to add an edge (e2 → e1) for each edge (e1 → e2).

```bash
# At this point our graph only contains edge (e1 -> e2), not (e2 -> e1)
  # Create a new list to add edges (e2 -> e1)
  # We also remove loops
  print("Generating edges...")
  edges = []
  for tup in graph:
      if tup[0] != tup[1]:
          edges.append((tup[0], tup[1]))
          edges.append((tup[1], tup[0]))
  # Remove all duplicate edges
  edges = list(set(edges))
```

## Stage 2: calculating the PageRank scores for nodes in the graph of terms

Now we have a list of nodes and a list of edges. Before we create a GraphFrame the list of nodes and edges must be transformed into a dataframe. Then we instantiate a GraphFrame `g` and print the degrees out to console. Finally we calculate the pagerank, order the top 10 results descending and print the results to console.

```python
# Initiate SQLContext with SparkContext
sqlContext = SQLContext(sc)
# Turn list of nodes and edges into dataframes
v = sqlContext.createDataFrame(vertices, ["id", "term"])
e = sqlContext.createDataFrame(edges, ["src", "dst"])

g = GraphFrame(v, e)
g.degrees.show()

# Calculating pagerank
results = g.pageRank(tol=0.0001, resetProbability=0.15)
results.vertices.select("term", "pagerank").sort("pagerank",
	ascending=False).limit(10).show()
```

### Output for post with ID 14

```bash
+--------+------------------+
|    term|          pagerank|
+--------+------------------+
|    data|1.8654218671868266|
|  mining|1.2609991383306334|
| science|1.2550464922135116|
|  fields|1.1020312104525885|
|question|0.9850216709761106|
|   large|0.9814216116939001|
|analyzed|0.9804672747252673|
|synonyms|0.9804629823034162|
|   class|0.8765183062235449|
|   years|0.8755763969231413|
+--------+------------------+
```

```bash
+---+------+
| id|degree|
+---+------+
|  7|    14|
|  6|    16|
|  9|    18|
|  5|    28|
|  1|    12|
| 10|    12|
|  3|    14|
| 12|    12|
|  8|    10|
| 11|     6|
|  2|    12|
|  4|    14|
| 13|    14|
| 14|    18|
| 15|    12|
+---+------+
```

# Big Data Project Part 1

### Victor Jørgensen and Hans Kristian Sande

## Code description

We have created a runnable program to be executed with `spark-submit main.py --input_path /path/to/data`.

All global constants (file names) is located in `constants.py`.

Each task is organized into separate files; `task1.py`, `task2.py` and `task3.py`.

├── constants.py
├── main.py
├── task1.py
├── task2.py
├── task3.py

## main.py

The main program initiates a SparkContext and a SparkSession variable and passes these down to each task's function. The `spark` variable, which points to a SparkSession object, is passed into `task3()`.

To avoid redundance and only loading the data from file once the main function also creates a dictionary containing the RDDs for each of the four data tables. This dictionary is passed as input into each function. Spark's `map()` function is used on all four RDDs to split the text files into multiple columns by using `split("\t")` where "\t" is the delimiter.

## Task 1

To print the number of rows in each of the RDDs we simply print the output of `rdd.count()`.

```scala
Task 1 results:

Posts: 56218
Users: 91617
Comments: 58736
Badges: 105641
```

## Task 2

### Task 2.1: Find the average length of questions, answers and comments in characters

Ee started by using the post_rdd from task 1. To find all questions and answers, we filtered the posts_rdd using `.filter()` and a lambda function. PostTypeId is the second column in the RDD, therefore we filter on `line[1]`. This filtering was obviously not necessary with comments.

```python
questions = posts_rdd.filter(lambda line: line[1] == "1")
answers = posts_rdd.filter(lambda line: line[1] == "2")
```

From the description of the data, we learned that the questions, answers and comments were encoded using base64. To decode all the data we used Python's base64 library and the `b64decode()` function.

When examining the output of the decoded data, we discovered that the posts text contained HTML-tags. We chose to replace the paragraph tags and the HTML Encoded Line Feed character (&#xA;) with empty strings using `replace()`.

```python
decoded_answers = answers.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
decoded_questions = questions.map(lambda line: str(base64.b64decode(line[5]), "utf-8")).map(
        lambda line: line.replace("<p>", " ").replace("</p>", " ").replace("&#xA;", " "))
decoded_comments = comments_rdd.map(lambda line: str(base64.b64decode(line[2]), "utf-8"))
```

After making sure the data was on the correct format, we mapped the decoded answers, questions and comments to a RDD only containing the length of each string.

```python
answer_length = decoded_answers.map(lambda line: len(line))
question_length = decoded_questions.map(lambda line: len(line))
comment_length = decoded_comments.map(lambda line: len(line))
```

To find the average length of each we used `reduce()` to sum up the length of each post, and then divided by the number of rows in the RDD.

```python
avg_answer_length = answer_length.reduce(lambda a, b: a+b)/answer_length.count()
avg_queston_length = question_length.reduce(lambda a, b: a+b)/question_length.count()
avg_comment_length = comment_length.reduce(lambda a, b: a+b)/comment_length.count()
```

```python
Average answer length: 960 characters
Average question length: 1030 characters
Average comment length: 169 characters
```

```python
Average answer length: 1021 characters
Average question length: 1113 characters
Average comment length: 169 characters
```

### Task 2.2: Find the date when the first and last questions were asked, and display name of the users who posted the questions.

To be able to compare the creation dates of each question, we first created a function to convert a datestring to a datetime object.

```python
from datetime import datetime as dt
def str_to_time(datestring):
    return dt.strptime(datestring, "%Y-%m-%d %H:%M:%S")
```

To find the first and last question, we once again used `reduce()` to compare the creation date of each question with each other. To find the display name of each user, we filtered the users_rdd to find the user with Id == OwnerUserId.

```python
newest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) > str_to_time(b[2]) else b)
oldest_question = questions.reduce(
        lambda a, b: a if str_to_time(a[2]) < str_to_time(b[2]) else b)
newest_questioner_name = users_rdd.filter(
        lambda user: user[0] == newest_question[6]).collect()[0]
oldest_questioner_name = users_rdd.filter(
        lambda user: user[0] == oldest_question[6]).collect()[0]
```

```python
Newest question: 2020-12-06 03:01:58 by mon
Oldest question: 2014-05-13 23:58:30 by Doorknob
```

### Task 2.3: Find the ids of users who wrote the greatest number of answers and questions.

First we group the answers by UserId, and then count number of posts for each user and reduce to find the UserId the the most answers.

```python
most_answers = answers.groupBy(lambda line: line[6]).map(lambda x: (
        x[0], len(list(x[1])))).sortBy(lambda x: x[1]).reduce(lambda a, b: a if a[1] > b[1] else b)
```

Alternative approach for questions using `reduceByKey()` (also ignoring the userid with NULL).

```python
most_questions = questions.map(lambda a: (a[6], 1)).filter(lambda x: x[0] != "NULL")\
        .reduceByKey(lambda a, b: a + b).sortBy(lambda x: x[1]).reduce(lambda a, b: a if a[1] > b[1] else b)
```

```python
UserID for user with the most answers: 64377
Number of answers: 579

UserID for user with the most questions: 8820
Number of questions: 103
```

### Task 2.4: Calculate the number of users who received less than three badges

Once again using `reduceByKey()` to sum up the badges for each user. Then, filter out each user with less than three badges.

```python
less_than_three_badges = badges_rdd.map(lambda badge: (badge[0], 1)).reduceByKey(
        lambda a, b: a + b).filter(lambda x: x[1] < 3)
```

```python
Users with less than three badges: 37190
```

### Task 2.5: Calculate the Pearson correlation coefficient (or Pearson’s r) between the number of upvotes and downvotes cast by a user.

We created a separate function to calculate the Pearson's r, where `users` is the users RDD.

```python
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
```

```python
Pearson correlation coefficient: 0.268
```

### Task 2.6: Calculate the entropy of id of users who wrote one or more comments.

Yet again we created a separate function to calculate the entropy, where the `comments` parameter is the comments RDD.

```python
def entropy(comments):
    user_in_comments = comments.map(lambda a: (a[4], 1)).reduceByKey(lambda a, b: a + b)
    length_comments = comments.count()
    return -user_in_comments.map(lambda x: x[1] / length_comments *
                                 math.log(x[1] / length_comments, 2)).reduce(lambda a, b: a+b)
```

```python
Entropy: 11.257
```

## Task 3

### **Task 3.1: Create a graph of posts and comments. Nodes are users, and there is an edge from node _i_ to node _j_ if _i_ wrote a comment for _j_’s post. Each edge has a weight _wij_ that is the number of times _i_ has commented a post by _j_.**

We had some challenges when implementing the graph representation. The first concern was with _joins_, because the postId in comments.csv did not necessarily have a unique ID, e.g. if a user commented a certain post several times. We first created a solution using a double for-loop, but this was terribly inefficient and did not take effect of RDD. Eventually, we got a solution using RDD:

1. The first step in our code is to remove all posts with a commentCount less than 1. This filtering step reduced the total rows from around 55k posts to 22k.
2. The next step was to perform a natural join on postId between the two csv files (posts and comments) so that the data was consistent with correct userId on both the sender and receiver of a comment.
3. The next step was to count all incidents in which the same combinations of the sender userId and receiver userId occurred. This was done using the reduceByKey-function. We then added this number as the weight _wij_.

```python

posts_with_comments = posts_rdd.filter(lambda post: post[11] != "0").map(
        lambda post: (post[0], post[6]))

# (PostId, (OwnerUserId, CommentUserId))
posts_and_comment = comments_rdd.join(posts_with_comments)
# (CommentUserId, OwnerUserId)
commentid_ownerid = posts_and_comment.map(lambda post: (post[1][0], post[1][1]))

# Add weight (count comments from i to j) :     (CommentUserId, (OwnerUserId, Weight))
temp_graph = commentid_ownerid.map(lambda row: (
		row, 1)).reduceByKey(lambda a, b: a+b)

graph = temp_graph.map(lambda row: (row[0][0], row[0][1], row[1]))
```

### **Task 3.2: Convert the results of the previous step into a Spark DataFrame (DF) and answer the following subtasks using DataFrame API, namely using Spark SQL**

This subtask was executed by loading the data into a defined schema. We first mapped the data to variable, and then using StructType from the pyspark.sql library, we created a dataframe with the relevant data using the createDataFrame() function.

```python
# DF schema to include name of columns
schema = StructType([
    StructField('CommentOwnerId', StringType(), False),
    StructField('PostOwnerId', StringType(), False),
    StructField('Weight', IntegerType(), False),
])
graphDF = spark.createDataFrame(graph, schema)
graphDF.createOrReplaceTempView("users")
```

### **Task 3.3: Find the user ids of the top 10 users who wrote the most comments.**

This was a fairly simple SQL-query. The key in this subtask was to remember to use SUM() for adding the values in each row and not the aggregate function COUNT() which just sums the amount of occurrences of a value in a field and not the value itself.

```python

spark.sql(
	"""SELECT CommentOwnerId, SUM(Weight) as Amount_of_comments
	FROM users
	GROUP BY CommentOwnerId
	ORDER BY Amount_of_comments
	DESC LIMIT 10"""
).show()
```

### **Task 3.4: Find the display name of top 10 users who their posts received the greatest number of comments.**

The first step in this subtask was to load the spark.sql-query into a variable we could use for later. The next step was very similar to subtask 3.1 where we had to define a schema for the data before creating the dataframe. The drop()-function we have used is just for display-purposes – it removes the column with redundant values. We could probably have avoided this with specifying a left or right join, but it has the same effect.

```python

userIds_with_most_comments = spark.sql(
	"""SELECT PostOwnerId, SUM(Weight) as Amount_of_comments_received
	FROM users
	GROUP BY PostOwnerId
	ORDER BY Amount_of_comments_received
	DESC LIMIT 10""")
```

### **Task 3.5: Save the DF containing the information for the graph of posts and comments (from subtask 2) into a persistence format (like CSV) on your filesystem so that later could loaded back into a Spark application’s workspace.**

For this I/O-operation we just used a try/except-block with a specification of CSV as argument.

```python
try:
    graphDF.repartition(1).write.option("header", "true").format(
        "com.databricks.spark.csv").save("graph.csv")
    print("The graph has been saved as 'graph.csv' in {}".format(os.getcwd()))
except:
    print("Tried to save 'graph.csv' to {} but file already exists.".format(os.getcwd()))
```
