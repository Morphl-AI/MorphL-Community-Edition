## Integrating the MorphL data science project with Cassandra

### Assumptions

In the following, we assume that the reader is using an OS environment where Python 3 and its corresponding `pip` helper tool are working correctly and without throwing errors.  
This is rarely the case when using the default Python distribution that comes with modern OSes, the main reason being the confusion that is sometimes caused by having both Python 2 and 3 installed, using `pip` and `pip3` interchangeably, missing OS-level dependencies, inconsistencies in Homebrew setups etc.  
One solution to this problem is to use [Anaconda](https://en.wikipedia.org/wiki/Anaconda_(Python_distribution)), a Python distribution that was created primarily to help data scientists build their solutions on a standard Python environment.  
Anaconda installs well-tested versions of a curated set of Python packages that are known to work well together, including Pandas, Numpy, Jupyter Notebook, SciKit Learn, Flask and many others.  
Anaconda installers can be found [here](https://repo.continuum.io/archive/).  
At the time of this writing (July 2018), the most recent distribution of `Anaconda3` for Python 3 / MacOS is:
```
https://repo.continuum.io/archive/Anaconda3-5.2.0-MacOSX-x86_64.pkg
```
After installing `Anaconda3`, open a new terminal window and confirm locations and versions with:
```
which python
python -V
which pip
pip -V
```
After this, upgrade pip and install the Python packages required by the MorphL project:
```
pip install --upgrade pip
pip install google-auth google-api-python-client tensorflow cassandra-driver
```
Side Note: The MorphL server environment runs exclusively on `Anaconda3`.

### Cassandra configuration and secrets are passed through environment variables

At this point you should have already received these environment variables from one of your coworkers:
```
export MORPHL_SERVER_IP_ADDRESS=???.???.???.???
export MORPHL_CASSANDRA_PASSWORD=?????
```
Place these commands in your `~/.bash_profile` and open a new terminal window.  
Confirm that the environment variables are now set:
```
env | grep MORPHL
```
This setup will mirror on your laptop the server-side environment where the Python programs will run.

### Working with Cassandra directly

Cassandra features a command-line tool called `cqlsh` that is useful for viewing and operating on data in Cassandra.  
The following is the recommended way to install it:
```
mkdir ~/opt
curl -so ~/opt/cassandra.tgz http://www-eu.apache.org/dist/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz
tar -xf ~/opt/cassandra.tgz -C ~/opt
mv ~/opt/apache-cassandra-* ~/opt/cassandra
rm ~/opt/cassandra.tgz
```
You can now work with Cassandra directly by running the following command:
```
/usr/bin/python ~/opt/cassandra/bin/cqlsh.py ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD}
```
The output you see should be similar to:
```
Connected to MorphLCluster at ???.???.???.???:9042.
[cqlsh 5.0.1 | Cassandra 3.11.3 | CQL spec 3.4.4 | Native protocol v4]
Use HELP for help.
morphl@cqlsh>
```
Note: `cqlsh` needs Python 2, hence why we have to use `/usr/bin/python`.

Cassandra tables exist within keyspaces (`keyspace` is the term Cassandra uses to designate a namespace).  
To switch to the already defined `morphl` keyspace, run the following command:
```
USE morphl;
```
The `cqlsh` prompt should change to reflect the name of the keyspace:
```
morphl@cqlsh> USE morphl;
morphl@cqlsh:morphl>
```
List the tables in this keyspace:
```
DESC TABLES;
```
The output:
```
morphl@cqlsh:morphl> DESC TABLES;

ps_area

morphl@cqlsh:morphl>
```
View the definition of the `ps_area` table:
```
DESC ps_area;
```
The output:
```
morphl@cqlsh:morphl> DESC ps_area;

CREATE TABLE morphl.ps_area (
  client_id text,
  day_of_data_capture date,
  days_since_last_seen int,
  PRIMARY KEY (client_id, day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC)
```
Note: The above is not the complete output. We choose to ignore the rest of the output because it doesn't pertain to our data modeling concerns.

Note: The way we initially created table `morphl.ps_area` is this:
```
CREATE TABLE morphl.ps_area (
  client_id text,
  day_of_data_capture date,
  days_since_last_seen int,
  PRIMARY KEY ((client_id), day_of_data_capture)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);
```
There is one subtle difference, and we are going to cover it later.

### Executing all CQL commands stored in a file

`cqlsh` provides the flag `-f` which can be used like this:
```
/usr/bin/python ~/opt/cassandra/bin/cqlsh.py ${MORPHL_SERVER_IP_ADDRESS} -u morphl -p ${MORPHL_CASSANDRA_PASSWORD} -f /tmp/abc.cql
```
where `/tmp/abc.cql` is a file that contains CQL statements.

### Differences between Cassandra and the relational databases you know

From a storage point of view, a Cassandra table is spread across multiple partitions.  
In our example, all data for one particular `client_id` is stored in its own partition, physically separate from all other partitions.  
This is reflected in how we designed the primary key:
```
PRIMARY KEY ((client_id), day_of_data_capture)
```
Here the `client_id` is the partitioning key, and this was made explicit by surrounding the column name with an extra set of parentheses: `(client_id)`.

The second part of the primary key is the clustering key, in our case it's the column `day_of_data_capture`.  
The clustering key is used to sort the data within a partition, in our case in descending order, as reflected by the clause:
```
WITH CLUSTERING ORDER BY (day_of_data_capture DESC)
```

### `SELECT` statements

As a consequence of Cassandra's design, we are in a position to run, for certain use cases, `SELECT` queries that look slightly different from what you are used to:
```
SELECT * FROM ps_area PER PARTITION LIMIT 1;
```
The output:
```
morphl@cqlsh:morphl> SELECT * FROM ps_area PER PARTITION LIMIT 1;

 client_id | day_of_data_capture | days_since_last_seen
-----------+---------------------+----------------------
       GA1 |          2018-07-05 |                    4
       GA2 |          2018-07-10 |                    6

(2 rows)
```
This query will extract the first record from each `client_id` partition, and because data within each partition is sorted by `day_of_data_capture` in descending order, saying "the first record" is equivalent to saying "the most recently captured record".

Obviously, we can ask for the entire contents of the table:
```
SELECT * FROM ps_area;
```
The output:
```
morphl@cqlsh:morphl> SELECT * FROM ps_area;

 client_id | day_of_data_capture | days_since_last_seen
-----------+---------------------+----------------------
       GA1 |          2018-07-05 |                    4
       GA1 |          2018-07-01 |                   15
       GA2 |          2018-07-10 |                    6
       GA2 |          2018-07-03 |                   25

(4 rows)
```

### `INSERT` statements

Inserts also behave slightly differently in Cassandra, as exemplified by these operations:
```
INSERT INTO ps_area (client_id,day_of_data_capture)
VALUES ('GA5','2018-06-25');

SELECT * FROM ps_area;

INSERT INTO ps_area (client_id,day_of_data_capture,days_since_last_seen)
VALUES ('GA5','2018-06-25',43);

SELECT * FROM ps_area;
```
The output:
```
morphl@cqlsh:morphl> INSERT INTO ps_area (client_id,day_of_data_capture)
          ... VALUES ('GA5','2018-06-25');
morphl@cqlsh:morphl> SELECT * FROM ps_area;

 client_id | day_of_data_capture | days_since_last_seen
-----------+---------------------+----------------------
       GA5 |          2018-06-25 |                 null
       GA1 |          2018-07-05 |                    4
       GA1 |          2018-07-01 |                   15
       GA2 |          2018-07-10 |                    6
       GA2 |          2018-07-03 |                   25

(5 rows)
morphl@cqlsh:morphl> INSERT INTO ps_area (client_id,day_of_data_capture,days_since_last_seen)
          ... VALUES ('GA5','2018-06-25',43);
morphl@cqlsh:morphl> SELECT * FROM ps_area;

 client_id | day_of_data_capture | days_since_last_seen
-----------+---------------------+----------------------
       GA5 |          2018-06-25 |                   43
       GA1 |          2018-07-05 |                    4
       GA1 |          2018-07-01 |                   15
       GA2 |          2018-07-10 |                    6
       GA2 |          2018-07-03 |                   25

(5 rows)
```
A relational database would have accepted the first `INSERT` statement, but would have rejected the second one, claiming a violation of the primary key uniqueness constraint.  
Cassandra will not complain about the second `INSERT` statement, and will change `days_since_last_seen` from `null` (non-existent) to `43`.

As a consequence of this design, in Cassandra all `INSERT` statements are in fact `UPSERT` statements (`INSERT` if absent, `UPDATE` if present).  
Although Cassandra does in principle allow syntax for `UPDATE` statements, there is a generally accepted idiomatic practice to only use `INSERT` statements.

### Inserting data into Cassandra with Python

Assumptions:  
a) We have already installed the Cassandra driver for Python:
```
pip install cassandra-driver
```
b) We have the necessary environment variables, please make sure that they are now set:
```
env | grep MORPHL
```
When inserting into Cassandra from Python, it is good practice to "prepare once, bind in a loop".  
This is for both performance and security reasons.  
Preparing means parsing the syntax and compiling the `INSERT` statement into an intermediate form.  
Preparing is a computationally expensive operation that is typically performed once, before entering a loop.  
Binding turns the intermediate form generated above into an executable form by supplying actual values to insert.  
Binding is a computationally cheap operation that is typically performed inside of a loop.  
The following is setup code that is needed before we can execute any operations in Cassandra.  
Notice how the Python program takes the IP address of the Cassandra server from the OS environment, and also needs a user/password combination, as well as the name of the Cassandra keyspace to use):
```
from os import getenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

MORPHL_SERVER_IP_ADDRESS = getenv('MORPHL_SERVER_IP_ADDRESS')
MORPHL_CASSANDRA_USERNAME = 'morphl'
MORPHL_CASSANDRA_PASSWORD = getenv('MORPHL_CASSANDRA_PASSWORD')
MORPHL_CASSANDRA_KEYSPACE = 'morphl'

auth_provider = PlainTextAuthProvider(
    username=MORPHL_CASSANDRA_USERNAME, password=MORPHL_CASSANDRA_PASSWORD)

cluster = Cluster(
    contact_points=[MORPHL_SERVER_IP_ADDRESS], auth_provider=auth_provider)

session = cluster.connect(MORPHL_CASSANDRA_KEYSPACE)
```
Now that we have a connected `session` object, we prepare a statement, enter a loop, then repeatedly bind some values and execute the insert:
```
insert_statement = 'INSERT INTO ps_area (client_id,day_of_data_capture,days_since_last_seen) VALUES (?,?,?)'
prep_stmt_for_ps_area_insert = session.prepare(insert_statement)
# Begin Loop
bind_list_for_ps_area_insert = ['GA8','2018-07-15',28]
session.execute(prep_stmt_for_ps_area_insert, bind_list_for_ps_area_insert)
# End Loop
```
If we now go back and look at the data, this is what we see:
```
morphl@cqlsh:morphl> SELECT * FROM ps_area;

 client_id | day_of_data_capture | days_since_last_seen
-----------+---------------------+----------------------
       GA8 |          2018-07-15 |                   28
       GA5 |          2018-06-25 |                   43
       GA1 |          2018-07-05 |                    4
       GA1 |          2018-07-01 |                   15
       GA2 |          2018-07-10 |                    6
       GA2 |          2018-07-03 |                   25

(6 rows)
```
