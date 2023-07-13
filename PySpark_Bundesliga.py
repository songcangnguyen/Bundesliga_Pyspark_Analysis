#!/usr/bin/env python
# coding: utf-8

# In[18]:


import findspark 
findspark.init()

import pyspark 
from pyspark.sql import SparkSession 
spark = SparkSession.builder.getOrCreate()


# In[31]:


from pyspark import SparkContext 
from pyspark.sql import Window,Row
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
import matplotlib.pyplot as plt
import calendar


# In[20]:


#SparkSession-entry point to programming Spark with Dataset and API 
spark = SparkSession \
        .builder \
        .appName("firstSpark")\
        .getOrCreate()


# In[21]:


def load_dataframe(filename):
    df = spark.read.format('csv').options(header='True').load(filename)
    return df 

#creating dataframe 
df_matches = load_dataframe('./Data/Matches.csv')
df_matches.limit(5).show()


# In[22]:


#rename columns
old_cols = df_matches.columns[-3:]
new_cols = ["HomeTeamGoals","AwayTeamGoals","FinalResult"]
old_new_cols = [*zip(old_cols, new_cols)]
for old_col, new_col in old_new_cols: 
    df_matches = df_matches.withColumnRenamed(old_col, new_col)
    
df_matches.limit(20).toPandas()


# In[23]:


#1.Winners of D1 division in Germany Football Association (Bundesliga) in last decade 
df_matches = df_matches \
    .withColumn('HomeTeamWin', when(col('FinalResult') == 'H', 1).otherwise(0)) \
    .withColumn('AwayTeamWin', when(col('FinalResult') == 'A', 1).otherwise(0)) \
    .withColumn('GameTie', when(col('FinalResult') == 'D', 1).otherwise(0))


#bundesliga is a D1 division and we are interested in season <= 2010 and >= 2000
bundesliga = df_matches \
                    .filter((col('Season') >= 2000) & 
                            (col('Season') <= 2010) & 
                            (col('Div') == 'D1'))
#Aggregate home and away game results separately creating two dataframes: home and away 
# home team features
home = bundesliga.groupby('Season', 'HomeTeam') \
       .agg(sum('HomeTeamWin').alias('TotalHomeWin'),
            sum('AwayTeamWin').alias('TotalHomeLoss'),
            sum('GameTie').alias('TotalHomeTie'),
            sum('HomeTeamGoals').alias('HomeScoredGoals'),
            sum('AwayTeamGoals').alias('HomeAgainstGoals')) \
       .withColumnRenamed('HomeTeam', 'Team')
 
#away game features 
away =  bundesliga.groupby('Season', 'AwayTeam') \
       .agg(sum('AwayTeamWin').alias('TotalAwayWin'),
            sum('HomeTeamWin').alias('TotalAwayLoss'),
            sum('GameTie').alias('TotalAwayTie'),
            sum('AwayTeamGoals').alias('AwayScoredGoals'),
            sum('HomeTeamGoals').alias('AwayAgainstGoals'))  \
       .withColumnRenamed('AwayTeam', 'Team')


# In[24]:


df_matches = df_matches \
    .withColumn('HomeTeamWin',when(col('FinalResult')=='H',1).otherwise(0))\
    .withColumn('AwayTeamWin',when(col('FinalResult')=='A',1).otherwise(0))\
    .withColumn('GameTie',when(col('FinalResult')=='D',1).otherwise(0))

#Filter D1 and 2000->2010
bundesliga = df_matches.filter((col('Season')>=2000) & (col('Season')<=2010) & (col('Div') == 'D1'))
bundesliga



# In[10]:


#season features 
window = ['Season']
window = Window.partitionBy(window).orderBy(col('WinPct').desc(),col('GoalDIfferentials').desc())
table = home.join(away,['Team','Season'],'inner')\
    .withColumn('GoalsScored',col('HomeScoredGoals')+col('AwayScoredGoals'))\
    .withColumn('GoalsAgainst',col('HomeAgainstGoals')+col('AwayAgainstGoals'))\
    .withColumn('GoalDifferentials',col('GoalsScored')-col('GoalsAgainst'))\
    .withColumn('Win', col('TotalHomeWin') + col('TotalAwayWin')) \
    .withColumn('Loss', col('TotalHomeLoss') + col('TotalAwayLoss')) \
    .withColumn('Tie', col('TotalHomeTie') + col('TotalAwayTie')) \
    .withColumn('WinPct', round((100* col('Win')/(col('Win') + col('Loss') + col('Tie'))), 2)) \
    .drop('HomeScoredGoals', 'AwayScoredGoals', 'HomeAgainstGoals', 'AwayAgainstGoals') \
    .drop('TotalHomeWin', 'TotalAwayWin', 'TotalHomeLoss', 'TotalAwayLoss', 'TotalHomeTie', 'TotalAwayTie') \
    .withColumn('TeamPosition', rank().over(window))

table_df = table.filter(col('TeamPosition') == 1).orderBy(asc('Season')).toPandas()
table_df


# In[25]:


#Window function
#window = Window.partitionBy('Team').orderby('WinPct')
table.groupby('Team').agg(count('Team').alias("TotalChampionships"),
                          avg('WinPct').alias('AvgWinPct'),
                          avg('GoalDifferentials').alias('AvgGD'),
                          avg('Win').alias('AvgWin'),
                          avg('Loss').alias('AvgLoss'),
                          avg('Tie').alias('AvgTie')) \
    .orderBy(desc("TotalChampionships")).toPandas().round(1)


# In[27]:


df_teams = load_dataframe('./Data/Teams.csv')
df_teams.limit(5).toPandas()


# In[28]:


# lets check for 2000s
relegated = table.filter((col('TeamPosition') == 16) | 
             (col('TeamPosition') == 17) |
             (col('TeamPosition') == 18)).orderBy(asc('Season')) 
relegated.filter(col('Season') == 2000).toPandas()


# In[29]:


df_matches.limit(5).toPandas()


# In[35]:


oktoberfest = df_matches \
                .filter(col('Div') == 'D1') \
                .filter((col('Season') >= 2000) &(col('Season') <= 2010)) \
                .withColumn('Month', month(col('Date'))) \
                .groupby('Month') \
                .agg(sum(col('HomeTeamGoals') + col('AwayTeamGoals')).alias('Goals'),
                     sum('GameTie').alias('GameTie'),
                     count(col('FinalResult')).alias('Total')) \
                .withColumn('Goals_to_games_ratio', round(col('Goals')/col('Total'), 1)) \
                .withColumn('GameTie_to_games_ratio', round(col('GameTie')/col('Total'), 1))

oktoberfest_df = oktoberfest.toPandas()

oktoberfest_df = oktoberfest_df.sort_values('Month')
oktoberfest_df['Month'] = [calendar.month_name[val] for val in oktoberfest_df.Month.tolist()]
oktoberfest_df.set_index('Month', drop=True, inplace=True)
oktoberfest_df['Goals_to_games_ratio'].plot.bar(rot=0, color='blue', figsize=(12, 4))
plt.ylabel('Goals_to_games_ratio')
plt.show()


# In[37]:


oktoberfest_df['GameTie_to_games_ratio'].plot.bar(rot=0, color='blue', figsize=(12, 4))
plt.ylabel('GameTie_to_games_ratio')
plt.show()


# In[ ]:




