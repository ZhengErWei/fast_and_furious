# Fast & Furious

University of Chicago | Spring 2018

Repository for CMSC 12300 (Computer Science with Applications-3) Group Project

Traffic Pattern Detection Based on New York Yellow Taxi Dataset

This project looks at causal relationship between several factors and traffic time by using big data technique mapreduce and mpi. Factors include weather, time and location. Different algorithms are also used to predict traffic time by using those variables. The project looks at single trip and pair of trip.

# Structure of repo
- <code>report</code>: project proposal, final presentation and final report can be found here.

- <code>code</code>: all the code we wrote can be found here.
  - index - generates index on weather, location, and time over traffic time and tip rate
  - single trip - predicts traffic time on single trip
  - matching pair - looks at causality and predicts difference in traffic time between matched pair of trips
  - passenger privacy  - deanonymizing interesting trips
  
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
his support for our project.
