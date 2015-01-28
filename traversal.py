'''
Twitter Hashtag traversal.

@author Prashant Kumar

@version: 1.0 2014

Copyright (c) 2014

'''

import time
import datetime
import sys
import json
import csv

 
import tweepy
from tweepy import Cursor


consumer_key=""
consumer_secret=""
access_token=""
access_token_secret=""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


#get neo4j set up
#note, you have to have neo4j running and on the default port
from py2neo import neo4j
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data")


# Add uniqueness constraints.
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (t:Tweet) ASSERT t.id IS UNIQUE;").run()
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (u:User) ASSERT u.screen_name IS UNIQUE;").run()
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (h:Hashtag) ASSERT h.name IS UNIQUE;").run()
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (l:Link) ASSERT l.url IS UNIQUE;").run()
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (s:Source) ASSERT s.name IS UNIQUE;").run()
neo4j.CypherQuery(graph_db, "CREATE CONSTRAINT ON (c:State) ASSERT c.name IS UNIQUE;").run()

total_tweets = 0
COUNT = 0
LAST_ID = ''

class Traversal:
  def __init__(self):
    pass


  '''
  addTweets function to add data to our graph database.
  @param: tweets json tweets fetched from the twitter.
  @param: search String hashtag used for the search.
  @param: state String name of the state to get the twitter messages.
  @param: geocode String geo code of the location used for fetching tweets.
  @param: max_id String id of the last tweet fetched from the previous request.
  '''
  def addTweets(self, tweets, search, state='', geocode=None, max_id=None):
      print 'addTweets Called:'
      print 'search=%s' % search
      print 'state=%s' % state
      print 'geocode=%s' % geocode

      tweet_list = []

      i = 0
      global total_tweets
      for tweet in tweets:
          max_id = tweet._json['id']
          tweet_list.append(tweet._json)
          ts = time.strftime('%Y-%m-%d %H:%M:%S',
                              time.strptime(tweet._json['created_at'],
                              '%a %b %d %H:%M:%S +0000 %Y'))

          ts = datetime.datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")


          epoch = datetime.datetime.utcfromtimestamp(0)
          delta = ts - epoch
          total_seconds = delta.days*86400+delta.seconds+delta.microseconds/1e6
          milli_seconds = total_seconds*1000
          tweet._json['created_at'] = milli_seconds
          i +=  1
          total_tweets = total_tweets + 1

      state_name = state
      tweets = tweet_list

      # Pass dict to Cypher and build query.
      query = """
          UNWIND {tweets} AS t
          WITH t
          ORDER BY t.id
          WITH t,
               t.entities AS e,
               t.user AS u,
               t.retweeted_status AS retweet
          MERGE (tweet:Tweet {id:t.id})
          SET tweet.text = t.text,
              tweet.created_at = t.created_at,
              tweet.retweeted = t.retweeted,
              tweet.retweet_count = t.retweet_count
          MERGE (user:User {screen_name:u.screen_name})
          SET user.name = u.name,
              user.location = u.location,
              user.followers = u.followers_count,
              user.following = u.friends_count,
              user.verified = u.verified
          MERGE (user)-[:POSTED]->(tweet)
          MERGE (source:Source {name:t.source})
          MERGE (tweet)-[:USING]->(source)
          MERGE (state: State {name: {state_name}})
          MERGE (tweet)-[:FROM]->(state)
          FOREACH (h IN e.hashtags |
            MERGE (tag:Hashtag {name:h.text})
            MERGE (tag)-[:TAGGED]->(tweet)
          )
          """

      # Send Cypher query.
      neo4j.CypherQuery(graph_db, query).run(tweets=tweets, state_name=state_name)
      print("Tweets added to graph!\n")

      self.searchTweets(search, state, geocode, max_id-1)


  '''
  searchTweets function to make a search request to the twitter api using tweepy.
  @param: search String hashtag used for the search.
  @param: state String name of the state.
  @param: geocode String geocode of the location in a state.
  @param: max_id  String id of the last tweet fetched from the previous search request.
  '''
  def searchTweets(self, search, state, geocode, max_id=None):
      print 'searchTweets called:'
      print 'search tweets for %s' % search
      print 'state=%s' % state
      print 'geocode=%s' % geocode
      global COUNT
      COUNT += 1
      if COUNT < 100:
            tweets = api.search(q=search, count=100, max_id=max_id, geocode=geocode)
            if len(tweets) != 0:
              self.addTweets(tweets, search, state, geocode, max_id)
            else:
              print tweets
              print 'No tweets found!'
      else:
        print "Don't fetch more tweets"
        pass


  '''
  getGeoCodes function to get the geo codes with states and the radius of the area.
  @return: dict dict_object geo codes with state name and the radius.
  '''
  def getGeoCodes(self):
    print 'getGeoCodes called:'
    dict_object = {}
    with open('us_cities.csv') as csvfile:
      reader = csv.reader(csvfile)
      for row in reader:
        state = row[1]
        geocode = row[2] + ',-' + row[3] + ',200mi'
        dict_object[geocode] = state
    return dict_object


  '''
  geoTweets function to fetch tweets based on geo locations.
  @param: search String hashtag used for fetching tweets.
  '''
  def geoTweets(self, search):
      print 'geoTweets called:'
      print 'search tweets for%s' % search
      geocodes = self.getGeoCodes()
      geo_count = 0
      for geocode, state in geocodes.iteritems():
        geo_count = geo_count+1
        if geo_count > 20000 and geo_count < 21000:
          self.searchTweets(search, state, geocode, None)


def main():
  traversal = Traversal()
  #get Tweets.
  xsearch=['#USSenate', '#Senate',
            '#senate2014', 
            '#FlipTheSenate', '#Democrats', '#WHITEHOUSE2014', '#JoniErnst', '#ObamaResign', 
            '#Clinton2016', '#StopHillary', '#Obama']

  search = ['obama'] 

  for value in search:
    COUNT = 0
    print 'search tweets for'
    print value
    traversal.geoTweets(search=value)

  print "total tweets"
  print total_tweets
  print 'requests sent:%s' % COUNT


if __name__ == '__main__':
  main()



