# Fast & Furious

University of Chicago | Spring 2018

Repository for CMSC 12300 (Computer Science with Applications-3) Group Project

Traffic Pattern Detection Based on New York Yellow Taxi Dataset

This project looks at causal relationship between several factors and traffic time by using big data technique mapreduce and mpi. Factors include weather, time and location. Different algorithms are also used to predict traffic time by using those variables. The project looks at single trip and pair of trip.

# Structure of repo
- <code>report</code>: project proposal, final presentation and final report can be found here.
- <code>code</code>: all the code we write can be found here.
  - index - generates index on weather, location, and time (month, weekday and hour)
  - single trip - predicts traffict time by multiple regression
  - matching pair - looks at causal effect of six variables on traffic time difference by simple linear regression and predicts difference of traffic time by matching pair
  - passenger privacy  - deannonymizing interesting trip
- <code>data</code>: sample data used or generated can be found here.


# References
1. http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml
2. http://toddwschneider.com/posts/analyzing-1-1-billion-nyc-taxi-and-uber-trips-with-a-vengeance/


## Contributors
**Hyun Ki Kim** [coreanokim](https://github.com/coreanokim)

**Andi Liao** [liaoandi](https://github.com/liaoandi)

**Winston Zunda Xu** [zundaxu](https://github.com/zundaxu)

**Weiwei Zheng** [ZhengErWei](https://github.com/ZhengErWei)

We would like to express our wholehearted gratitude to **Dr. Matthew Wachs** for 
his support on our project.
