Repository Link: https://github.com/cancanasoyak/Airflow-DatabaseMerge-Examples

This file contains information on how to Create the airflow structure and connect to the databases, any modifications I made to the original airflow-docker-image are listed at the bottom.

First be sure you are at the directory where the docker-compose.yaml file is.

Be sure you have folders are present:
	- ./config
	- ./dags
	- ./logs
	- ./plugins

On Linux, the quick-start needs to know your host user id and needs to have group id set to 0. Otherwise the files created in dags, logs and plugins will be created with root user ownership. You have to make sure to configure them for the docker-compose, run:

	mkdir -p ./dags ./logs ./plugins ./config
	echo -e "AIRFLOW_UID=$(id -u)" > .env

For other operating systems, you may get a warning that AIRFLOW_UID is not set, but you can safely ignore it.

This repository already has them but be sure they are present:
	- ./data
	- ./data/tmp

./data folder is where you should have your store_sales.csv file.
If you have different names or want to use another directory, feel free to edit the ./dags/main_task.py (Be aware that if you want to use another folder, you'll have to mount the volumes inside the docker-compose.yaml folder.) 

Lastly, before using docker compose, make sure the DAG is present under dags folder:
	- ./dags/main_task.py

Now we will compose the docker.


First, run:

	docker-compose up airflow-init

this will create an airflow user with name and password "airflow".


after it finishes with Exit code 0, run:

	docker-compose up -d

The default port for airflow-webserver is 8080 (it might take a minute before the web UI is usable),

There should be 7 containers created inside the network, airflow-init can be inactive.


After composing the Airflow docker, first check that you have the DAG main_task is present.

after that you need to create 2 variables from the airflow web UI to connect to your databases(for online sales and warehouse).

Go to the Admin -> Variables and create two variables:

	-key == "salesdb_connstr", value == "host=*your_host(localhost for me)* port=*your_port(default is 5432)* dbname= *your_db_name* user=*your_user* 	password=*your_pw*"

	-key == "warehousedb_connstr", value == "host=*your_host(localhost for me)* port=*your_port(default is 5432)* dbname= *your_db_name* user=*your_user* 	password=*your_pw*"



I'm using 2 different databases in the same server in my local machine for convenience, so my value's look like this (if you are using localhost for the databases, docker might not be able to access to databases using localhost, in that situation try using "host.docker.internal"):

![Alt text](/imgs/airflow_variable_ex.png)


After creating the variables, your DAG is ready to be run, if it still says the values are missing, wait a few seconds.



the functions:
	- ./fill_database.py
	- ./check_database.py

are helper functions for you to put data into the databases or read them.




Changes done to the default docker-compose.yaml file:

changed:
	AIRFLOW__CORE__LOAD_EXAMPLES: 'true'
to:
	AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
in line 62.

added:
	- ${AIRFLOW_PROJ_DIR:-.}/data:/data
after line 76 in the volumes section.

