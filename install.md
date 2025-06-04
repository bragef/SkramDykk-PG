# Installing 

The programs needed to run the software are:

* Python3
* PIP 
* PostgreSQL

For Python it is required to install several libraries.
To install the Python libraries run the command:

```
pip install -r requirements.txt
```

pgsql_init/create_database.sql

The PostgreSQL database will currently have to be created manually from the `pgsql_init/create_database.sql`

Before you start running the code make sure you modify some paths:

* The path to CD to in the update.sh script
* The LOCALDIR in the fetchdata/fetchdata.py script


