# 
## Deliverable #1:

This project is about to create a script that extracts JSON data from a public API, 
which is going to be stored in a table created in Redshift for the subsequent data load.

This code will be used in the final project as an initial ETL (Extract, Transform, Load) script in a Python dictionary format.

The table must be created in Redshift. The chosen distribution style was KEY distribution (the rows are distributed according to the values in one column, 
so  this way, matching values from the common columns are physically stored together).

And the chosen SORT KEY was pubDate (article's published date), because the ETLs objective is to get daily news on the topic of the war in Ukraine.

The used source is https://newsdata.io/