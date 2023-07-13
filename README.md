# Bundesliga_Pyspark_Analysis
![bundesliga1](https://github.com/songcangnguyen/Bundesliga_Pyspark_Analysis/assets/109171837/028f2b6c-3d5b-4062-bd87-b131d1a351c6)

D1 Division of the Germany Football Association (D1) witnessed significant developments and changes, making it an intriguing subject for analysis. I will delve deeper into the data and statistics of this division to identify patterns, key factors that have influenced team performance and league standings. 

## Data Source 

## Questions
1. Who are the winners of the D1 Division in the Germany Football Association (Bundesliga) in the last decade?
2. Which teams have been relegated in the past 10 years?
3. Does Octoberfest affect the performance of the Bundesliga?
4. Which seasons of Bundesliga were most competitive in the last decade?
5. What's the best month to watch Bundesliga?

## Approach and Analysis
1. Data manipulation: Aggregate the home and away game results separately creating two dataframes: home and away..
2. Join two dataframes on 'Team' and 'Season' fields to create single dataframe containing game level aggregation: table.
3. Window function to aggregate game statistics on season level and rank them based on winning percentage and goal differentials. 
