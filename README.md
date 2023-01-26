# Databricks-partitioning
Data partitioning in databricks
Explore partitioning of file-based RDDs
1. Review /FileStore/accounts/ dataset you created in a previous lab using dbutils. Take note of the number of files
– you should do this in two ways:
(a) By listing the files in a directory
(b) A Pythonic way that outputs the number of files in a directory. (Hint: len may be your friend.)
2. Create an RDD based on a single file in the dataset, e.g. /FileStore/accounts/part-m-00000, and then call toDebugString
on the RDD, which displays the number of partitions in parentheses () before the RDD id. How many partitions are
in the resulting RDD?
3. Repeat this process, but specify a minimum of three partitions to sc.textFile. Does the RDD correctly have three
partitions?
4. Finally, create an RDD based on all the files in the accounts dataset. How does the number of files in the dataset
compare to the number of partitions in the RDD?
Set up the job
5. Create an RDD of accounts from the contents of the /FileStore/accounts/ directory, keyed by ID (the first field in
the file) and with the string “first name, last name” for the value. I.e. your output should look something like this:
[( ’1 ’ , ’ Becton , Donald ’) ,
( ’2 ’ , ’ Jones , Donna ’) ,
( ’3 ’ , ’ Chalmers , Dorthy ’) ,
...
6. Construct a userreqs RDD from the data in /FileStore/logs/ directory with the total number of web hits for each
user ID – i.e. the number of webpages a user (in the third field) has looked at. The result should look something like
this:
[( ’80283 ’ , 64) , ( ’1 ’ , 1240) , ( ’70 ’ , 1302) ...
7. Join the two RDDs by user ID, and construct a new RDD based on first name, last name and total hits, getting
something like this:
[( ’ Whiteside , Pauline ’ , 26) , ( ’ Durbin , Judith ’ , 8) , ( ’ Simon , Bryce ’ , 28) ...
8. Print the results of accounthits.toDebugString and review the output. Based on this, see if you can determine
(a) How many stages are in this job?
(b) Which stages are dependent on which?
(c) How many tasks will each stage consist of?
9. Save the resulting accounthits to /FileStore/accounts. Note: you may want to account for reusability – i.e. delete
the directory if it’s present – first, as saving to a directory will fail if you already have files in that location.
Difference between reduceByKey and groupByKey
We’ll take a look at the timing differences between reduceByKey and groupByKey.
10. Create an RDD using paralelize which consists of the numbers 0 to 1000000 (those can be accessed using Python’s
range function).
11. All those records are distinct, and not in the pair RDD format. Since we’re wanting to compare pair RDDs with
multiple occurences of the same key, we’ll use the modulo % operation to give us RDDs with some nice repeats as
follows:
repeatsRDD = numericalRDD .map( lambda x: (x % 5000 , 1))
12. You should examine the RDD you’ve just created (you could first look at the shape when you use x % 5 instead to see
the form of the created RDD).
13. If you put %%time at the beginning of your python cell, you will get a timing for the execution of the entire cell contents.
Don’t forget that you need to add an action in for anything to happen! You should time the execution of:
(a) reduceByKey
(b) groupByKey
14. Do you spot any running time differences? Or any differences in toDebugString output?
