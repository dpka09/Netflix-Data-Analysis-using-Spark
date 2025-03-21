### Overview
This project uses PySpark to analyze a dataset of Netflix titles (netflix_titles.json). It performs various operations, such as filtering movies, analyzing cast appearances, and identifying movies based on specific attributes like duration and release year. The processed results are saved in specific directories for further use.


## Prerequisites
Ensure the following are installed:

- Python 3.x
- PySpark
- findspark
- Hadoop (if using HDFS)
---
## Installation Steps
Install Python libraries:

    pip install pyspark findspark
    
Set up Hadoop if required.

---
***Usage Instructions***

***Setup Environment***

1. Initialize the Spark environment:

        import findspark
        findspark.init()
        from pyspark import SparkContext
        sc = SparkContext()

2. Load the dataset (netflix_titles.json):
    
        rdd = sc.textFile("/home/deepika/Downloads/netflix_titles.json")
        data = rdd.map(lambda x: json.loads(x))

---
## Question

- Calculate the number of titles in which each actor/actress has appeared.
- Filter movies released in 2008 and count.
- List movies with duration > 100 minutes.
- Retrieve movies where "Kareena Kapoor" acted.
---


***Notes***

Ensure correct paths for file input and output.

Use persist(StorageLevel.MEMORY_ONLY) to optimize performance during repeated operations.

Adjust filters or mappings based on custom requirements.
