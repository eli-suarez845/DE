# 
## Data Engineering Project:

This project is about to create a script that extracts JSON data from a public API, 
which is going to be stored in a table created in Redshift for the subsequent data load.

This code will be used in the final project as an initial ETL (Extract, Transform, Load) script in a Python dictionary 
format. The project also runs in a Docker Compose, embedded in a Airflow DAG. To run it you can create a 
'./venv/config:/opt/airflow/config' 
directory and store your credentials in a config.ini archive. The used source is https://newsdata.io/.

The table is created in Redshift. The chosen distribution style was KEY distribution (the rows are distributed 
according to the values in one column, so  this way, matching values from the common columns are physically stored 
together).

And the chosen SORT KEY was pubDate (article's published date), because the ETLs objective is to get daily news on the 
topic of the war in Ukraine.


A 'run.cmd' file is included and could be executed in order to run the docker compose with the specified services for 
Docker. This will expose an Airflow Web Server in the 8080 port in localhost. The default credentials are 
airflow/airflow.

In order to run the mail alerts, please create an .env file with your SMTP_KEY variable and your e-mail variable: SENDER_EMAIL.

In the next link, you can see how to create the token for your gmail account to set it up the SMTP_Client:
https://support.google.com/accounts/answer/185833?hl=en .

