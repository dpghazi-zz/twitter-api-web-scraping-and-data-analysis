#!/usr/bin/env python
# coding: utf-8

# # Project Details

# 1. Data Wrangling
#     * Gathering Data 
#     * Assessing Data
#     * Cleaning Data
# 2. Storing, Analyzing, and Visualizing wrangled data
# 3. Reporting data wrangling efforts, data analyses, and visualizations

# ## Data Wrangling
# ### Gathering Data
# 1. Twitter Archive Enhanced
# 2. Image Predictions
# 3. Twitter API Data

# In[299]:


# import packages 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import requests
import tweepy
import json


# In[300]:


# load given twitter archive
archive = pd.read_csv('twitter-archive-enhanced.csv')


# In[301]:


# count number of tweets from the archive (number of tweets with ratings only)
archive.shape[0]


# In[302]:


# sort and set df showing latest tweets first 
archive.sort_values('timestamp',ascending=False,inplace=True)
archive.head() #inspect the data 


# In[303]:


# download image predictions from Udacity's server
url = 'https://d17h27t6h515a5.cloudfront.net/topher/2017/August/599fd2ad_image-predictions/image-predictions.tsv'
response = requests.get(url)

with open(url.split('/')[-1], mode='wb') as file:
    file.write(response.content)


# In[304]:


# import and load the tsv file by setting the separator to (tab) into a df called images 
images = pd.read_csv('image-predictions.tsv', sep='\t')

# check to see if it was imported correctly 
images.head()


# In[305]:


# count tweets from image-predictions.tsv (number of tweets with images)
images.shape[0]


# In[306]:


# set keys to establish connection (*removed for submission)
consumer_key = 'XXXXX'
consumer_secret = 'XXXXX'
access_token = 'XXXXX'
access_token_secret = 'XXXX'


# In[307]:


# configure connection (http://docs.tweepy.org/en/v3.5.0/auth_tutorial.html)
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# create an API object 
api = tweepy.API(auth,
                 parser = tweepy.parsers.JSONParser(),
                 wait_on_rate_limit = True,
                 wait_on_rate_limit_notify = True)


# In[308]:


# set 2 lists and download Tweepy Status objects (items) via API

# list contains tweets w/ tweet ids
tweets_list = []

# list contains ids w/o tweets 
no_tweets_list = []

count = 0
for tweet_id in archive['tweet_id']:   
    count += 1
    try:
        tweets_list.append(api.get_status(tweet_id))
    except Exception as e:
        no_tweets_list.append(tweet_id)
        
    # keep track of items processed (should say 2350 @ the end)
    if count % 50 == 0:
        print(str(count)+ " items processed ")


# In[309]:


# https://stackabuse.com/writing-files-using-python/

# create tweet_json.txt
with open('tweet_json.txt', 'w') as outfile:   # load JSON data we gathered 
        for tweet_json in tweets_list:
            json.dump(tweet_json, outfile)  # write data (what we constructed) to outfile 
            outfile.write('\n')   # save JSON data as tweets_json.txt file 


# In[312]:


#https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/tweet-object (for attributes)


# create a list of attributes 
attr_list = []

# open the text file and read as JSON file 
with open('tweet_json.txt', 'r') as json_file:
    
    # read the first line for the loop to start below
    line = json_file.readline()
    
    # create loop to get retweets and their links 
    while line:
        json_data = json.loads(line)
        retweeted_status = json_data['retweeted_status'] = json_data.get('retweeted_status', 'original')
        if retweeted_status == 'original':
            url = json_data['text'][json_data['text'].find('https'):]
        else:
            retweeted_status = 'retweet'
            url = 'retweet'
        
        # create a dictionary for the selected attributes 
        attributes = {'tweet_id': str(json_data['id']), 
                            'favorite_count': int(json_data['favorite_count']),
                            'retweet_count': int(json_data['retweet_count']),
                            'followers_count': int(json_data['user']['followers_count']),
                            'friends_count': int(json_data['user']['friends_count']),
                            'url': url,
                            'retweeted_status': str(retweeted_status)}
        
        # add above to our list
        attr_list.append(attributes)
        
        # read the next line of JSON data
        line = json_file.readline()

# convert the tweet JSON data dictionary list to a pandas DataFrame
    api_data = pd.DataFrame(attr_list, 
                               columns = ['tweet_id',
                                    'favorite_count',
                                    'retweet_count',
                                    'followers_count',
                                    'friends_count',
                                    'url',
                                    'retweeted_status'])


# In[313]:


#inspect
api_data.head()


# ## Assessing Data
# ### Visual Assessment using Jupyter

# *As I've gathered each of the above pieces of data, I will assess them visually and programmatically for quality and tidiness issues.*

# In[314]:


archive


# In[317]:


api_data 


# In[318]:


images


# ### Programmatic Assessment

# In[394]:


archive.info()


# In[395]:


sum(archive['tweet_id'].duplicated())


# In[396]:


# check timestamp column type
type(archive.timestamp[0])


# **Issues**
# * timestamp column is in string format 
#  - needs to be in datetime format
# * tweet_id column contains floats 
#  - needs to be strings b/c these are identification numbers  *not for calcuations
# * 181 of the tweets are retweets
#  - need to remove these 181 rows because they are essentailly duplicates of original 
# * 78 of the tweets are replies to other tweets
#  - need to remove these 78 rows b/c it doesn't help with our project motivation 
# * 4 columns for dog stages 
#  - this can be better organized by just having 1 column and have 4 different values 
# 

# In[322]:


# check the ratings
archive.rating_numerator.value_counts()


# *Above shows unique values where 1776 being the highest and weird value like **666**, the devil's number.* 

# In[323]:


#inspect highest rating 
archive.query("rating_numerator == 1776")


# In[324]:


# second highest rating
archive.query("rating_numerator == 960")


# *Above is retweet, showing that ratings vary greatly and not consistent throughout.*
# *Hints that we can use 10 as a common denominator.*

# In[325]:


# check for tweets that were extracted incorrectly 

# check to see if tweets contain decimals, leading to high values or value like "666" vs 6.66 (in text)
archive[archive['text'].str.contains(r'(\d+\.\d+\/\d+)')] 


# **Issues**
# * Extraction isssue 
#  - Dataframe needs to be cleaned where ratings mirror original tweets
# * Numerator isssue  
#  - Needs to be in floats (* i.e., 11.26 was extracted incorrectly and gets a score of 26)
#  - However, cannot change values given (* i.e., 1776 is an actual rating and not a typo) 

# In[326]:


# check the rating_denominator column
archive.rating_denominator.value_counts()


# In[327]:


# query select the tweet where denominator is 0 and view it's tweet ('text')
archive.query("rating_denominator == 0")['text']


# In[328]:


archive.query("rating_denominator == 7")['text']


# In[329]:


archive.query("rating_denominator == 170")['text']


# **Issues**
# * Denominator issues
#  - Set common denominator as 10
#  - Needs to be in floats too to since our numerators need to be changed to floats
#  - Project Moviation states that **"[t]he fact that the rating numerators are greater than the denominators does not need to be cleaned".**

# In[330]:


# check the name column
archive.name.value_counts()


# *Above shows lots of questionable "names" like "a" and "the" which are non-names, signaling extraction error.*

# In[397]:


# check names that aren't capitalized b/c names should be capitalized 

# use regex where names start with lower-case letters 
non_capital_names = archive.name.str.contains(pat='^[a-z]', regex = True) 
archive[non_capital_names].name.value_counts()


# **Issues**
# * Name isssues
#  - Another extraction error
#  - Need to get rid of non-capital names, which aren't names afterall

# In[398]:


images.info()


# **Issues**
#  - tweet_id column should in strings 

# In[399]:


# evaluate a sample of the image predictions
images.sample(5)


# In[400]:


# check for duplicates of url 
images.jpg_url.duplicated().value_counts()


# **Issues**
#  - 66 duplicated jpg_urls 

# In[401]:


# inspect what we generated from Twitter API 
api_data.info()


# In[402]:


# inspect a sample
api_data.sample(5)


# In[403]:


# count for retweets and original tweets 
api_data.retweeted_status.value_counts()


# ## Assessment Overview 
#     
# ### Twitter Archive
#   
# #### Quality
# 
# 
# * Quality Issue (1) : data contains 181 retweets
#  - Get rid of rows that are retweets 
# * Quality Issue (2) : data contains 78  replies
#  - Get rid of rows that are replies 
# * Quality Issue (3) : timestamp column is in string format
#  - Change to datetime format 
# * Quality Issue (4) : tweet_id column contains integers
#  - Change to string format 
# * Quality Issue (5) : rating_numerator column values need to be in floats
#  - Change to floats programmatically 
# * Quality Issue (6) : rating_denominators column needs one consistent value all across (10)
#  - Use lambda function to change all denominators to 10
# * Quality Issue (7) : rating_denominator values need to be in floats 
#  - Change to floats programmatically 
# * Quality Issue (8) : name column contains non-name values
#  - Replace them with string 'None'
#  
# #### Tidiness
# * Tidiness Issue (1) : Data contains 4 columns for dog stages, need just one column and have them as values
# 
#   
# ### Image Prediction
#   
# #### Quality
# * Quality Issue (9) : Data contains 66 duplicated jpg_urls
# * Quality Issue (10) : tweet_id column needs to be in string format 
# 
# #### Tidiness
# * Tidiness Issue (2) : Needs one column each for image prediction and confidence level
# 
#   
# ### Twitter API Data
#   
# #### Quality
# * Quality Issue (11) : Data contains retweets, get rid of them to keep only the originals 
#   
# #### Tidiness
# * Tidiness Issue (3) : Dataframe needs to be joined with the other two dataframes 

# In[414]:


# copy all original data frames
clean_archive = archive.copy()
clean_images = images.copy()
clean_api = api_data.copy()


# ## Data Cleaning

# ### Quality Issue 1: (Twitter Archive) *Remove Retweets*

# #### Define

# *The given Twitter Archive df contains 181 retweets. Remove rows that are retweets.* 

# In[415]:


sum(clean_archive.retweeted_status_id.notnull())


# #### Code

# In[416]:


clean_archive = clean_archive[clean_archive.retweeted_status_id.isna()]


# #### Test

# In[417]:


sum(clean_archive.retweeted_status_id.notnull())


# In[418]:


# we can remove retweet columns from the df 
clean_archive = clean_archive.drop(['retweeted_status_id',
                                    'retweeted_status_user_id',
                                    'retweeted_status_timestamp'], axis = 1)


# ### Quality Issue 2: (Twitter Archive) *Remove Replies*

# #### Define

# *The given Twitter Archive df contains 78 retweets. Remove rows that are replies.*

# In[419]:


sum(clean_archive.in_reply_to_status_id.notnull())


# #### Code

# In[420]:


clean_archive = clean_archive[clean_archive.in_reply_to_status_id.isna()]


# #### Test

# In[421]:


sum(clean_archive.in_reply_to_status_id.notnull())


# In[422]:


# we can also remove replies columns from the df
clean_archive = clean_archive.drop(['in_reply_to_status_id',
                                    'in_reply_to_user_id'], axis = 1)


# ### Quality Issue 3: (Twitter Archive) *Change timestamp column type (string) to datetime*

# #### Define

# *The timestamp column data type is string. Change it to datetime data type.*

# #### Code

# In[423]:


clean_archive['timestamp'] = pd.to_datetime(clean_archive.timestamp)


# #### Test

# In[424]:


clean_archive.info()


# ### Quality Issue 4: (Twitter Archive)     *Change tweet_id data type from integer to string*

# #### Define

# *The tweet_id column data type is integer. Change it to string data type.*

# #### Code

# In[425]:


clean_archive['tweet_id'] = clean_archive['tweet_id'].apply(str)


# #### Test

# In[426]:


clean_archive.info()


# In[427]:


type(clean_archive.tweet_id[0])


# ### Quality Issue 5: (Twitter Archive)  *rating_numerator column values need to be in floats*

# #### Define

# *Change rating_numerator column data type to float.*

# #### Code

# In[428]:


clean_archive['rating_numerator'] = clean_archive['rating_numerator'].astype(float)


# #### Test

# In[429]:


clean_archive.info()


# ### Quality Issue 6 : (Twitter Archive)  *rating_denominator column needs one consistent value all across*

# #### Define

# *As the common denominator should be 10, change all denominators that aren't equal to 10.*

# #### Code

# In[430]:


clean_archive["rating_denominator"] = clean_archive["rating_denominator"].apply(lambda x: 10 if x != 10 else 10)


# #### Test

# In[431]:


clean_archive.query("rating_denominator!=10")


# ### Quality Issue 7 : (Twitter Archive)  *rating_denominator values need to be in floats*

# #### Define

# *To keep consistecy, change rating_denominator to floats.*

# #### Code

# In[432]:


clean_archive['rating_denominator'] = clean_archive['rating_denominator'].astype(float)


# #### Test

# In[433]:


clean_archive.info()


# ### Quality Issue 8 : (Twitter Archive)  *name column contains non-name values*

# #### Define

# *Replace all non-names in the name column with string 'None'.*

# #### Code

# In[434]:


clean_archive.name.value_counts()


# In[435]:


# use regex where names start with lower-case letters 
mask = clean_archive.name.str.contains(pat='^[a-z]', regex = True)  
clean_archive[mask].name.value_counts().sort_index()


# In[436]:


# replace non-names in the name column with string 'None'
clean_archive.loc[mask, 'name'] = "None"


# #### Test

# In[437]:


sum(clean_archive.name.str.contains(pat='^[a-z]', regex = True))


# ### Tidiness Issue 1 : (Twitter Archive) *data contains 4 columns for dog stages*

# #### Define

# *Create one column for dog stages and have them (doggo,floofer,pupper,puppo) as values instead.*

# In[438]:


clean_archive.info()


# In[439]:


clean_archive.doggo.value_counts()


# In[440]:


clean_archive.floofer.value_counts()


# In[441]:


clean_archive.pupper.value_counts()


# In[442]:


clean_archive.puppo.value_counts()


# #### Code

# In[443]:


# convert 'None' to empty strings similarly to what we did before 
clean_archive.doggo.replace('None', '', inplace=True)
clean_archive.floofer.replace('None', '', inplace=True)
clean_archive.pupper.replace('None', '', inplace=True)
clean_archive.puppo.replace('None', '', inplace=True)


# In[444]:


# combine the 4 columns
clean_archive['stage'] = clean_archive.text.str.extract('(doggo|floofer|pupper|puppo)', expand = True)


# In[445]:


# check 
clean_archive.stage.value_counts()


# In[447]:


#double check
clean_archive.info()


# In[448]:


# now drop the 4 columns as they are no longer needed 
clean_archive.drop(['doggo','floofer','pupper','puppo'], axis=1, inplace = True)


# In[449]:


# change datatype from object to category
clean_archive['stage'] = clean_archive['stage'].astype('category')


# #### Test

# In[450]:


clean_archive.info()


# ### Quality Issue 9 : (Image Predctions)  *Data contains 66 duplicated jpg_urls*

# #### Define

# *Dataframe contains 66 duplicated jpg_urls which need to be removed.*

# #### Code

# In[461]:


clean_images = clean_images.drop_duplicates(subset=['jpg_url'], keep='last')


# #### Test

# In[462]:


sum(clean_images.jpg_url.duplicated())


# ### Quality Issue 10 : (Image Predictions) *tweet_id column needs to be in string format*

# #### Define

# *The tweet_id column data type is integer. Change it to string data type to keep it consistent with other data frames.*

# #### Code 

# In[463]:


clean_images['tweet_id'] = clean_images['tweet_id'].apply(str)


# #### Test

# In[464]:


clean_images.info()


# In[465]:


type(clean_images.tweet_id[0])


# ### Tidiness Issue 2 : (Image Predictions) *Needs one column each for image prediction and confidence level*

# #### Define

# *Clean up by having 1 column each for image prediction and confidence level, rather than having them in 3 different columns.*

# #### Code

# In[466]:


clean_images


# In[467]:


clean_images.info()


# In[468]:


clean_images.p1.value_counts()


# In[469]:


clean_images.p2.value_counts()


# In[470]:


clean_images.p3.value_counts()


# In[471]:


clean_images.query("p1_conf < p2_conf or p2_conf < p3_conf or p1_conf < p3_conf")


#  - *Notice how we didn't retrieve anything. This means that the highest confidence is stored in p1 (neural network made with the highest probability), so check by writing a function where the dogtype and confidence value first aligns and sort it in decreasing order ( p1 ->   p2  -> p3  )*

# In[472]:


# get dog breed and its respective confidence 

breed = []
confidence = []


def image(clean_images):
    if clean_images['p1_dog'] == True:
        breed.append(clean_images['p1'])
        confidence.append(clean_images['p1_conf'])
    elif clean_images['p2_dog'] == True:
        breed.append(clean_images['p2'])
        confidence.append(clean_images['p2_conf'])
    elif clean_images['p3_dog'] == True:
        breed.append(clean_images['p3'])
        confidence.append(clean_images['p3_conf'])
    else:  # if they don't match, write "None" b/c tweets beyond August 1st, 2017 don't have image predictions
        breed.append('None')
        confidence.append('None')

clean_images.apply(image, axis=1)

#create new columns
clean_images['breed'] = breed
clean_images['confidence'] = confidence


# In[473]:


clean_images.breed.value_counts()


# In[474]:


# Remove rows where breed is "None" (doens't align to our Project Motivation)
clean_images = clean_images[clean_images['breed'] != 'None']


# In[475]:


# Now, drop these columns as they're no longer needed 
clean_images = clean_images.drop(['img_num','p1','p1_conf','p1_dog','p2','p2_conf','p2_dog','p3','p3_conf',
                                  'p3_dog'], 1)


# #### Test

# In[476]:


clean_images


# ### Quality Issue 11 : (Twitter API Data) *Dataframe contains retweets*

# #### Define

# *Remove retweets to keep only the originals and to keep it consistent with the other dataframes.*

# In[477]:


clean_api.retweeted_status.value_counts()


# In[478]:


clean_api = clean_api.query("retweeted_status=='original'")


# #### Test

# In[479]:


clean_api.retweeted_status.value_counts()


# ### Tidiness Issue 3 : (Twitter API Data) *Dataframe needs to be merged with the other two dataframes*

# #### Define

# *Twitter API dataframe should be merged with the other data frames because they share same entities. Merge the dataframes into one finalized dataframed called twitter_archive_master.*

# #### Code

# In[480]:


#merge the first two dataframes we explored onto tweet_id column 
archive_and_images = pd.merge(clean_archive, 
                      clean_images, 
                      how = 'left', on = ['tweet_id'])


# In[482]:


archive_and_images = archive_and_images[archive_and_images['jpg_url'].notnull()]


# In[483]:


archive_and_images.info()


# In[484]:


twitter_archive_master = pd.merge(archive_and_images, 
                      clean_api, 
                      how = 'left', on = ['tweet_id'])


# In[485]:


twitter_archive_master.info()


# ## Storing

# *As the three dataframes are now merged into one, we want to set it as a file perhaps for later use.* 

# In[486]:


# store the frame as a file
twitter_archive_master.to_csv('twitter_archive_master.csv', 
                 index=False, encoding = 'utf-8')


# ## Analyzing and Visualizing Data 

# ### Insight 1 : *Which breeds have the most ratings on We Rate Dogs Twitter account ?*

# *In order to see which breeds have the most ratings, I will make a new dataframe that contains the breeds with the highest number of tweets assigned.*

# In[496]:


# select breeds with 50 or more tweets 
most_tweeted = twitter_archive_master.groupby('breed').filter(lambda x: len(x) >= 50)

# bar chart
most_tweeted['breed'].value_counts().plot(kind = 'bar')
plt.title("We Rate Dogs's Most Rated Breeds")
plt.xlabel('Breed')
plt.ylabel('Tweets Count')

fig = plt.gcf() 
fig.set_size_inches(15,7)


# *Looking at their tweets count, the most rated breeds on "We Love Dogs" Twitter account are Golden Retriver, Labrador Retriever, Pembroke, Chihuahua, and Pug.*

# In[497]:


# save the figure as file 
fig.savefig('most_rated_breeds.png',bbox_inches='tight');


# ### Insight 2 : Which dog stage is the most represented?

# *In order to show which dog stage is the most represented, I will display the frequencies of the stages via pie chart.*

# In[498]:


twitter_archive_master.stage.value_counts()


# In[499]:


# create pie chart with explode property
# https://medium.com/@kvnamipara/a-better-visualisation-of-pie-charts-by-matplotlib-935b7667d77f

labels = ['pupper', 'doggo', 'puppo', 'floofer']
sizes = [176, 57, 26, 3]

# colors
colors = ['magenta','lightgreen','yellow','orange']
plt.rcParams['text.color'] = 'black'

# explsion
explode = (0.09,0.09,0.09,0.09)
 
plt.pie(sizes, colors = colors, labels=labels, autopct='%1.0f%%', startangle=90, pctdistance=0.85, 
         explode = explode)

# draw circle
centre_circle = plt.Circle((0,0),0.70,fc='white')
fig = plt.gcf()
fig.gca().add_artist(centre_circle)

# plot 
ax1.axis('equal')  # to make sure pie is drawn as a circle
plt.tight_layout()
plt.show()


# *Looking at the numbers of each breed represented via tweets, we see that "pupper" is the most represented with its frequency being 67%. In contrast, "floofer" frequency is 1% which is signficantly low compared to the others. This shows that there may be data extraction issues.*

# ### Insight 3 : Which breeds are most favorited by users? 

# *In order to see which breeds are most favorited by users, I will group breeds with their respective favorite counts.*

# In[509]:


user_fav_breed = twitter_archive_master.query('breed ! = "none"').groupby(['breed'])['favorite_count'].sum().sort_values(ascending = False)


# In[504]:


# select top 5 only 
top5 = user_fav_breed.head(5)

# bar chart 
top5.plot(kind = 'bar')
plt.title('Most Favorited Breeds by Users')
plt.xlabel('Breed')
plt.ylabel('Favourite Count')


fig = plt.gcf() 
fig.set_size_inches(15,7)


# In[505]:


# save figure as file
fig.savefig('most_fav_breeds.png',bbox_inches='tight');


# *As exepcted, the breed with the highest favorite counts is Golden Retriever then followed by Labrador Retriever, Pembroke, Chihuahua. I expected to see Pug at the end but French Bulldog took the 5th most favorited breed.*

#                              End of Wrangle Act. Please see Wrangle Report.
