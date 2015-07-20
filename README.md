# Homework 3 Submission
# Table of Contents

1: [Database Dumps](#toc_1)  
2: [Implementation Notes](#toc_2)  
2.1: [Storing Tasks](#toc_2.1)  
2.1.1: [Save tweets from twitter to MongoDB](#toc_2.1.1)  
2.1.2: [Copy chunked tweets from HW 2 to MongoDB](#toc_2.1.2)  
2.2: [Retrieving and Analyzing Tasks](#toc_2.2)  
2.2.1: [Top Retweets](#toc_2.2.1)  
2.2.2: [Lexical Diversity](#toc_2.2.2)  
2.2.3: [Track unfollows](#toc_2.2.3)  
2.2.4: [Sentiment analysis](#toc_2.2.4)  
2.3: [Storing and Retrieving Task](#toc_2.3)  
2.3.1: [Backup & Restore](#toc_2.3.1)  
3: [Resilience](#toc_3)

# 1: Database Dumps
<a name='toc_1'></a>
MongoDB dump files are in S3 bucket [nkrishna-mids205-hw3](https://s3.amazonaws.com/nkrishna-mids205-hw3)

<a name='toc_2'></a>
# 2: Implementation Notes
This section reproduces (at an additional level of nesting) the contents of [proc.ipynb](proc.ipynb). See that ipython notebook for the code for this assignment.

<a name='toc_2.1'></a>
## 2.1: Storing Tasks

<a name='toc_2.1.1'></a>
### 2.1.1: Save tweets from twitter to MongoDB

> Write a python program to automatically retrieve and store the JSON files (associated with the tweets that include #NBAFinals2015 hashtag and the tweets that include #Warriors hashtag) returned by the twitter REST api in a MongoDB database called db_restT. 

**Notes**: The code is in [1.1_acq.py](1.1_acq.py).  
This reuses much of the code from Assignment 2, with the addition of a `MongoDBSink` in `sinks.py` to store tweets to a specified database and collection.


<a name='toc_2.1.2'></a>
### 2.1.2: Copy chunked tweets from HW 2 to MongoDB

> Write a python program to automatically retrieve and store the JSON files (associated with the tweets that include #NBAFinals2015 hashtag and the tweets that include #Warriors hashtag) returned by the twitter REST api in a MongoDB database called db_restT. 

**Notes**: The code is in [1.2_s3tomongo.py](1.2_s3tomongo.py).  
The second simply reads the files from S3 to a string, uses `json.loads` to read the contents into an array, and writes the tweets with pymongo's `insert_many` method.

<a name='toc_2.2'></a>
## 2.2: Retrieving and Analyzing Tasks

<a name='toc_2.2.1'></a>
### 2.2.1: Top Retweets

> Analyze the tweets stored in db_tweets by finding the top 30 retweets as well as their associated usernames (users authored them) and the locations of users.

**Notes**: Creating an index on retweeted_status.id will let us avoid both a collection scan and a sort when grouping retweets by id.
We process the results of the aggregation pipeline by printing each result and adding each user ID to a set of most RTed users

<a name='toc_2.2.2'></a>
### 2.2.2: Lexical Diversity

> Compute the lexical diversity of the texts of the tweets for each of the users in db_restT and store the results back to Mongodb. To compute the lexical diversity of a user, you need to find all the tweets of a particular user (a user's tweets corpus), find the number of unique words in the user's tweets corpus, and divide that number by the total number of words in the user's tweets corpus.
> 
> You need to create a collection with appropriate structure for storing the results of your analysis.

**Notes**: This is the first time we're looking at db_restT, so we start by indexing the tweet collection by user ID for subsequent aggregation on that field.
First, fetch each user's tweets and store them into `db_restT.user_tweets`, omitting RTs since they are not the user's text.
Storing them permits comparison of different tokenizing methods and avoids the need to restart in case of failure.

The `user_timeline` API per-call count limit is 200 statuses, and the call cannot retrieve more than 3200 tweets total. Limiting to 1000 tweets should be enough to be a representative sample, while eliminating 2/3 of the 15 minute rate limit delays.  The acquisition time can be further reduced by limiting the number of users (to 1000 of 6542) for whom we analyze statuses.

(*Note*: I got impatient and interrupted the kernel after 870 users.)

Some notes on implementation:
* it is necessary to retrieve the user IDs from mongodb prior to the loop on `user_timeline` calls.  Otherwise the mongodb cursor in the outer loop will become invalue due to twitter rate limit waits.
* `tweepy.API` has parameters to indicate which API status codes should be retried, but due to implementation details, 104 (connection reset by peer) errors get thrown.  These can be caught, and the `TweepError.response` object will be present, with member `status` set to 104. We can simply retry these, as the library will create a new connection.
* Users with private timelines will fail with an "authorization failed" error, with no `response`.  We found their posts in the DB by hashtag, but are not permitted to enumerate their tweets. For these and all other exceptions, we can simply try going on to the next user.  This is a bit fragile, and will misbehave if, eg, there are `pymongo` exceptions.
Next, we analyze each user's tweets for lexical diversity.  I split on `\W+` (one or more non-"word" character) to tokenize.  This is not always appropriate since we often wish to distinguish hashtags and mentions from other text.  However, since we are interested in words, this seems reasonable here.
Show the collection row counts as quick sanity check, and finally plot the lexical diversity.

<a name='toc_2.2.3'></a>
### 2.2.3: Track unfollows

> Write a python program to create a db called db_followers that stores all the followers for all the users that you find in task 2.1. Then, write a program to find the un-followed friends after a week for the top 10 users( users that have the highest number of followers in task 2.1) since the time that you extracted the tweets. In other words, you need to look for the people following the top 10 users at time X (the time that you extracted the tweets) and then look at the people following the same top 10 users at a later time Y (one-week after X) to see who stopped following the top 10 users.

**Notes**: First, make a db/table of top RT'ed users in which to store follower stats,
Then fetch the list of followers for the top 10 by followers.
The API rate limit is 15 calls per hour, and each call can return 5000 followers.  While this may compromise the efficacy of the assignment there are over 19 million followers for these users, I will only fetch the first 10k followers to avoid spending 11 days fetching all the followers (particularly since they would have changed in that time).
After a week, run the second phase
And report the results
Despite changes to the follower counts, there were no IDs in the final set not in the initial one.  As I had feared, the sample size of 10000 followers was not enough to capture the changed follower IDs.

<a name='toc_2.2.4'></a>
### 2.2.4: Sentiment analysis

> (Bonus task) Write a python program and use NLTK to analyze the top 30 retweets of task 2.1 as positive or negative (sentiment analysis). This is the bonus part of the assignment.

**Notes**: I started to do this by fetching a corpus of sentiment-classified tweets from
[http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/](http://thinknook.com/twitter-sentiment-analysis-training-corpus-dataset-2012-09-22/). (I have omitted this from the git push due to size.).  
I had planned on incorporating ideas from  Kiritchenko et al, Sentiment Analysis of Short Informal Texts to select features: stemmed tokens, POS-tagged tokens, final hashtags,n-grams, etc.  
As a classifier, I planned to use a random forest  from `scikit-learn`.  
I did not have time to complete this activity, howerver.

<a name='toc_2.3'></a>
## 2.3: Storing and Retrieving Task

<a name='toc_2.3.1'></a>
### 2.3.1: Backup & Restore

> Write a python program to create and store the backups of both db_tweets and db_restT to S3. It also should have a capability of loading the backups if necessary.

**Notes**: To backup a database, call `mongodump` to dump the database BSON files, compress them, and copy the archive to a user-provided S3 bucket.

For this assignment, I am using s3 bucket `nkrishna-mids205-hw3`.  I think I forgot to grant ListBucket privs last time, so I have added them to this bucket as well as GetObject.
Restoring will simply invert the steps: fetch the zipped BSON dumps, extract them, and call `mongorestore`
Verify that the older sanity check still works.

# 3: Resilience:
There were a few key areas that needed special error handling:

1. Tweepy would not retry error status 104.  So I added a `try`/`except` inside an infinite loop around each REST API call.  Should a 104 be encountered, the call would continue the infinite loop, effectively retrying a call for that cursor position while allowing Tweepy to reopen a connection.
2. Tweepy raised `TweepError`s with something like 'Authorization denied' for private timelines.  When these were encountered, the desired behavior is to move to the next user.  The exception handler would skip additional processing by breaking out of the infinite loop around the twitter call.
3. When making Twitter API calls in the body of a loop over users, the calls may block for long times to handle rate limits.  If I were looping over a pymongo cursor, it would time out.  To avoid that, I would fetch the list of users to a python variable first, and loop over that to make the Twitter API calls.
