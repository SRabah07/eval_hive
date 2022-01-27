# Evaluation Hive

Here we are going to use the [IMDB](https://www.imdb.com/interfaces/) DataBase and the data of `name.basics.tsv.gz`

```
name.basics.tsv.gz – Contains the following information for names:
nconst (string) - alphanumeric unique identifier of the name/person
primaryName (string)– name by which the person is most often credited
birthYear – in YYYY format
deathYear – in YYYY format if applicable, else '\N'
primaryProfession (array of strings)– the top-3 professions of the person
knownForTitles (array of tconsts) – titles the person is known for
```

Please note that in report, I am including the `TSV` file as it's about `600M0`. But I provided `init.sh` that download the dataset and do the right stuff. The others commands can be found later in this readme, but as reminder:

- `docker-compose up -d`: Once the `init.sh` launched and the `TSV` file retrieved
- `docker exec -it hive-server bash` to connect into the `Hive` server, you will need at least two consoles. the first to work with `HDFS` and a second to start hive, just run `hive`
- `hdfs dfs -ls /` to list the root folder of HDFS. 

More can found below. 

## Environment Setup

I am going to use the `docker compose` Hive, provided during the courses. 

```bash
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env
```

We need to create local directory called `data` used as a volume in the docker-compose. 


```bash
(base) ➜  Eval_Hive wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml
--2022-01-26 16:03:46--  https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml
Resolving dst-de.s3.eu-west-3.amazonaws.com (dst-de.s3.eu-west-3.amazonaws.com)... 52.95.156.60
Connecting to dst-de.s3.eu-west-3.amazonaws.com (dst-de.s3.eu-west-3.amazonaws.com)|52.95.156.60|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 1657 (1.6K) [binary/octet-stream]
Saving to: ‘docker-compose.yml’

docker-compose.yml                             100%[=================================================================================================>]   1.62K  --.-KB/s    in 0s

2022-01-26 16:03:46 (35.9 MB/s) - ‘docker-compose.yml’ saved [1657/1657]

(base) ➜  Eval_Hive wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env
--2022-01-26 16:03:54--  https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env
Resolving dst-de.s3.eu-west-3.amazonaws.com (dst-de.s3.eu-west-3.amazonaws.com)... 52.95.156.60
Connecting to dst-de.s3.eu-west-3.amazonaws.com (dst-de.s3.eu-west-3.amazonaws.com)|52.95.156.60|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 1589 (1.6K) [binary/octet-stream]
Saving to: ‘hadoop-hive.env’

hadoop-hive.env                                100%[=================================================================================================>]   1.55K  --.-KB/s    in 0s

2022-01-26 16:03:54 (47.4 MB/s) - ‘hadoop-hive.env’ saved [1589/1589]

(base) ➜  Eval_Hive mkdir data
(base) ➜  Eval_Hive ls -lrt
total 24
-rw-r--r--  1   staff  1589 Jan 14 16:33 hadoop-hive.env
-rw-r--r--  1   staff  1657 Jan 14 16:33 docker-compose.yml
-rw-r--r--  1   staff   580 Jan 26 16:01 README.md
drwxr-xr-x  2   staff    64 Jan 26 16:04 data
(base) ➜  Eval_Hive

```

## Download TCV File

The dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name. 

```bash
(base) ➜  Eval_Hive wget https://datasets.imdbws.com/name.basics.tsv.gz
--2022-01-26 16:06:29--  https://datasets.imdbws.com/name.basics.tsv.gz
Resolving datasets.imdbws.com (datasets.imdbws.com)... 2600:9000:2171:c200:3:3082:af00:93a1, 2600:9000:2171:a800:3:3082:af00:93a1, 2600:9000:2171:8600:3:3082:af00:93a1, ...
Connecting to datasets.imdbws.com (datasets.imdbws.com)|2600:9000:2171:c200:3:3082:af00:93a1|:443... connected.
HTTP request sent, awaiting response... 200 OK
Length: 222373665 (212M) [binary/octet-stream]
Saving to: ‘name.basics.tsv.gz’

name.basics.tsv.gz                             100%[=================================================================================================>] 212.07M  47.7MB/s    in 4.5s

2022-01-26 16:06:33 (47.1 MB/s) - ‘name.basics.tsv.gz’ saved [222373665/222373665]

(base) ➜  Eval_Hive gunzip name.basics.tsv.gz
(base) ➜  Eval_Hive mv name.basics.tsv data/
(base) ➜  Eval_Hive ls -lrt data
total 1348480
-rw-r--r--  1  staff  680727370 Jan 25 14:24 name.basics.tsv
(base) ➜  Eval_Hive

```

- Show file
  
```bash
(base) ➜  Eval_Hive head data/name.basics.tsv
nconst	primaryName	birthYear	deathYear	primaryProfession	knownForTitles
nm0000001	Fred Astaire	1899	1987	soundtrack,actor,miscellaneous	tt0050419,tt0072308,tt0031983,tt0053137
nm0000002	Lauren Bacall	1924	2014	actress,soundtrack	tt0038355,tt0117057,tt0037382,tt0071877
nm0000003	Brigitte Bardot	1934	\N	actress,soundtrack,music_department	tt0056404,tt0049189,tt0057345,tt0054452
nm0000004	John Belushi	1949	1982	actor,soundtrack,writer	tt0080455,tt0078723,tt0077975,tt0072562
nm0000005	Ingmar Bergman	1918	2007	writer,director,actor	tt0083922,tt0069467,tt0060827,tt0050986
nm0000006	Ingrid Bergman	1915	1982	actress,soundtrack,producer	tt0034583,tt0036855,tt0077711,tt0038109
nm0000007	Humphrey Bogart	1899	1957	actor,soundtrack,producer	tt0033870,tt0034583,tt0037382,tt0043265
nm0000008	Marlon Brando	1924	2004	actor,soundtrack,director	tt0047296,tt0070849,tt0068646,tt0078788
nm0000009	Richard Burton	1925	1984	actor,soundtrack,producer	tt0087803,tt0061184,tt0057877,tt0059749
(base) ➜  Eval_Hive

```

## Startup

### Start Docker Compose


```bash
(base) ➜  Eval_Hive docker-compose up -d

Creating namenode ... done
Creating datanode ... done
Creating hive-metastore-postgresql ... done
Creating hive-metastore            ... done
Creating hive-server               ... done

(base) ➜  Eval_Hive docker container ls | grep hive
ba74b599cf13   bde2020/hive:2.3.2-postgresql-metastore           "entrypoint.sh /bin/…"   44 seconds ago   Up 43 seconds             0.0.0.0:10000->10000/tcp, 10002/tcp            hive-server
bcdf327bad48   bde2020/hive:2.3.2-postgresql-metastore           "entrypoint.sh /opt/…"   44 seconds ago   Up 44 seconds             10000/tcp, 0.0.0.0:9083->9083/tcp, 10002/tcp   hive-metastore
cd60a6362ed4   bde2020/hive-metastore-postgresql:2.3.0           "/docker-entrypoint.…"   45 seconds ago   Up 44 seconds             5432/tcp                                       hive-metastore-postgresql
(base) ➜  Eval_Hive
```

#### Connect to `hive-server` / `HDFS`

Using `docker exec` to access the running container. This console will be used for `HDFS`

```bash
(base) ➜  Eval_Hive docker exec -it hive-server bash
root@ba74b599cf13:/opt# pwd
/opt
root@ba74b599cf13:/opt# ls -lrt
total 16
drwxr-xr-x 1 20415 input 4096 Feb  5  2018 hadoop-2.7.4
drwxr-xr-x 1 root  root  4096 Feb  5  2018 hive
root@ba74b599cf13:/opt#
```

Copy the TCV file into `HDFS` using `hdfs dfs -copyFromLocal /data/name.basics.tsv /`


```bash
root@ba74b599cf13:/opt# hdfs dfs -ls /
Found 2 items
drwxrwxr-x   - root supergroup          0 2022-01-26 15:10 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-26 15:10 /user
root@ba74b599cf13:/opt# hdfs dfs -copyFromLocal /data/name.basics.tsv /
root@ba74b599cf13:/opt# hdfs dfs -ls /
Found 3 items
-rw-r--r--   3 root supergroup  680727370 2022-01-26 15:14 /name.basics.tsv
drwxrwxr-x   - root supergroup          0 2022-01-26 15:10 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-26 15:10 /user
root@ba74b599cf13:/opt# hdfs dfs -ls -h /
Found 3 items
-rw-r--r--   3 root supergroup    649.2 M 2022-01-26 15:14 /name.basics.tsv
drwxrwxr-x   - root supergroup          0 2022-01-26 15:10 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-26 15:10 /user
root@ba74b599cf13:/opt#
```

#### Connect to `hive-server` / `Hive`

This second console will be used to interact with `Hive` using CLI


```bash
(base) ➜  Eval_Hive docker exec -it hive-server bash
root@ba74b599cf13:/opt# hive
SLF4J: Class path contains multiple SLF4J bindings.
SLF4J: Found binding in [jar:file:/opt/hive/lib/log4j-slf4j-impl-2.6.2.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: Found binding in [jar:file:/opt/hadoop-2.7.4/share/hadoop/common/lib/slf4j-log4j12-1.7.10.jar!/org/slf4j/impl/StaticLoggerBinder.class]
SLF4J: See http://www.slf4j.org/codes.html#multiple_bindings for an explanation.
SLF4J: Actual binding is of type [org.apache.logging.slf4j.Log4jLoggerFactory]

Logging initialized using configuration in file:/opt/hive/conf/hive-log4j2.properties Async: true
Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
hive> SHOW DATABASES;
OK
default
Time taken: 0.87 seconds, Fetched: 1 row(s)
hive>

```

#### Modeling

##### Create Database

As first step let create a new database called `imdb`

```bash
hive> CREATE DATABASE imdb;
OK
Time taken: 0.304 seconds
hive> SHOW DATABASES;
OK
default
imdb
Time taken: 0.012 seconds, Fetched: 2 row(s)
hive> USE imdb;
OK
Time taken: 0.056 seconds
hive> SHOW TABLES;
OK
Time taken: 0.04 seconds
hive>

```

##### Create Table

As reminder the TSV is a Tabulation Separator Value and each row contains the information below

- `nconst` (string) - alphanumeric unique identifier of the name/person
- `primaryName` (string)– name by which the person is most often credited
- `birthYear` – in YYYY format
- `deathYear` – in YYYY format if applicable, else '\N'
- `primaryProfession` (array of strings)– the top-3 professions of the person
- `knownForTitles` (array of tconsts) – titles the person is known for


As first step, I am going to create a temporary table called `persons_tmp` it will help me to analysis the data.

- Script in HiveQL

```hive
CREATE TABLE persons_tmp
(
    tconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession STRING,
    knownForTitles STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;
```

The separator is Tab `\t`. And let upload the `name.basics.tsv.gz`

- Hive Console 

```bash
hive> CREATE TABLE persons_tmp
    > (
    >     tconst STRING,
    >     primaryName STRING,
    >     birthYear INT,
    >     deathYear INT,
    >     primaryProfession STRING,
    >     knownForTitles STRING
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.536 seconds
hive> SHOW TABLES;
OK
persons_tmp
Time taken: 0.024 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(*) FROM persons_tmp;
OK
0
Time taken: 1.48 seconds, Fetched: 1 row(s)
hive> LOAD DATA INPATH '/name.basics.tsv' INTO TABLE persons_tmp;
Loading data to table imdb.persons_tmp
OK
Time taken: 0.678 seconds
hive> SELECT COUNT(*) FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126152634_b3edaa21-2868-448c-97aa-d47c4c03176f
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:26:36,236 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:26:38,258 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:26:41,283 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local698491594_0001
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 2166957796 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
11360021
Time taken: 6.856 seconds, Fetched: 1 row(s)
hive>

```

We have `11 360 021` (More then 11 millions rows).

- HDFS Console

```bash
root@ba74b599cf13:/opt# hdfs dfs -ls /user/hive/warehouse/
Found 1 items
drwxrwxr-x   - root supergroup          0 2022-01-26 15:25 /user/hive/warehouse/imdb.db
root@ba74b599cf13:/opt# hdfs dfs -ls /user/hive/warehouse/imdb.db
Found 1 items
drwxrwxr-x   - root supergroup          0 2022-01-26 15:26 /user/hive/warehouse/imdb.db/persons_tmp
root@ba74b599cf13:/opt# hdfs dfs -ls /user/hive/warehouse/imdb.db/persons_tmp
Found 1 items
-rwxrwxr-x   3 root supergroup  680727370 2022-01-26 15:14 /user/hive/warehouse/imdb.db/persons_tmp/name.basics.tsv
root@ba74b599cf13:/opt# hdfs dfs -ls -R /user/hive/warehouse/
drwxrwxr-x   - root supergroup          0 2022-01-26 15:25 /user/hive/warehouse/imdb.db
drwxrwxr-x   - root supergroup          0 2022-01-26 15:26 /user/hive/warehouse/imdb.db/persons_tmp
-rwxrwxr-x   3 root supergroup  680727370 2022-01-26 15:14 /user/hive/warehouse/imdb.db/persons_tmp/name.basics.tsv
root@ba74b599cf13:/opt#

```

###### Some Basic Queries

As first query let check how many rows we have where on of the two dates are `null` (in the TSV `\N`)

```bash
hive> SELECT COUNT(*) FROM persons_tmp WHERE birthYear IS NULL OR deathYear IS NULL;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126152933_d37cfede-7c41-49de-9181-417a9535b72e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:29:35,144 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:29:37,150 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:29:41,168 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local156354204_0002
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 4889949196 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
11173528
Time taken: 7.55 seconds, Fetched: 1 row(s)
hive>

```

We have a lot rows where one of the two dates is empty / `null`. 


```bash
hive> SELECT COUNT(*) FROM persons_tmp WHERE birthYear IS NOT NULL
    > ;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126153202_296e2ca4-7830-493f-8949-f5a96a7cdee0
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:32:03,749 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:32:05,753 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:32:07,765 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local536392585_0003
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 7612940596 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
542011
Time taken: 5.382 seconds, Fetched: 1 row(s)


hive> SELECT COUNT(*) FROM persons_tmp WHERE deathYear IS NOT NULL
    > ;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126153241_468ea63d-181b-46b3-8156-77334a10a5dc
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:32:43,304 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:32:44,312 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:32:47,326 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1609176616_0004
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 10335931996 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
197616
Time taken: 5.38 seconds, Fetched: 1 row(s)
hive>

```

There are `542011` rows where the `birthYear` is present and  `197616` where the `deathYear` is present. 

- How many row are distinct for `birthYear`  and `deathYear`

```bash
hive> SELECT COUNT(DISTINCT (birthYear)) FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126153431_d80cda2a-1a66-4737-872e-6ffc5ea40f9e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:34:33,239 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:34:35,244 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:34:38,257 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local530824280_0005
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 13058923396 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
506
Time taken: 6.368 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(DISTINCT (deathYear)) FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126153449_e37d48cd-7fe8-4711-876e-a556cd104e7f
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:34:50,610 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:34:52,621 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:34:55,612 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local139363996_0006
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 15781914796 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
444
Time taken: 7.327 seconds, Fetched: 1 row(s)
hive>

```

We have `506` unique `birthYear` and `444` unique `deathYear`

From this analysis, we could try to create `partitions` or `buckets` to get better performances when making queries on those two fields.

For the parts below I am going to create a new table called `persons`. 

### Using Partitions

As we saw, we have many rows (more then `11` millions). As first idea, I tried to create partitions on both years (`birth` and `death`).

```hive
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE persons
(
    tconst STRING,
    primaryName STRING,
    primaryProfession STRING,
    knownForTitles STRING
)
partitioned by (birthYear int, deathYear int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;
```

Trying to upload data using the `TSV` file or `persons_tmp` table


```hive
INSERT INTO TABLE
persons
PARTITION(birthYear, deathYear)
SELECT tconst, primaryName, primaryProfession, knownForTitles, birthYear, deathYear
FROM persons_tmp;

```

I got exception, where the Job was failed. And a `NullPointerException` when trying to upload from file

```hive
    > LOAD DATA INPATH '/name.basics.tsv'
    > INTO TABLE persons PARTITION (birthYear, deathYear);
FAILED: NullPointerException null

```

After investigating the issue, it seems that there is a limitation in a number of partition peer Hive Node. [Here](https://cwiki.apache.org/confluence/display/Hive/Tutorial).

For example I was able to create about `100` partitions with the query below, but no more. We can confirm this by using
`set -v;` to show all configurations (hadoop, hive) and we can see `hive.exec.max.dynamic.partitions.pernode=100` we could increase


```hive
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE TABLE persons
(
    tconst STRING,
    primaryName STRING,
    primaryProfession STRING,
    knownForTitles STRING,
    deathYear INT
)
PARTITIONED BY  (birthYear int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE persons
PARTITION (birthYear)
SELECT tconst, primaryName, primaryProfession, knownForTitles, deathYear, birthYear
FROM persons_tmp WHERE deathYear IS NOT NULL AND birthYear IS NOT NULL LIMIT 247;
```


We could conclude that using a partitions in this case is not suitable. As we have many rows and over `500` distinct birth year. 

The second solution is the usage of `Buckets`


### Using Buckets

As we saw, we have many rows where on of the two years is empty. Which it means that we will have one big bucket. Let try by creating `500` buckets and see what we will get 


```hive
CREATE TABLE persons
(
    tconst STRING,
    primaryName STRING,
    primaryProfession STRING,
    knownForTitles STRING,
    birthYear int, 
    deathYear INT
)
CLUSTERED BY (birthYear, deathYear) INTO 500 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

```


```bash
hive> CREATE TABLE persons
    > (
    >     tconst STRING,
    >     primaryName STRING,
    >     primaryProfession STRING,
    >     knownForTitles STRING,
    >     birthYear int,
    >     deathYear INT
    > )
    > CLUSTERED BY (birthYear, deathYear) INTO 500 BUCKETS
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.094 seconds
hive> INSERT INTO TABLE persons
    > SELECT tconst, primaryName, primaryProfession, knownForTitles, birthYear, deathYear
    > FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126155011_89ef8b7a-6e0a-40a7-af28-fd4037185b87
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 500
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:50:12,737 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:50:18,768 Stage-1 map = 11%,  reduce = 0%
2022-01-26 15:50:20,812 Stage-1 map = 22%,  reduce = 0%
2022-01-26 15:50:21,827 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:50:27,904 Stage-1 map = 56%,  reduce = 0%
2022-01-26 15:50:30,953 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:50:34,017 Stage-1 map = 67%,  reduce = 0%
2022-01-26 15:50:36,222 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:50:56,981 Stage-1 map = 100%,  reduce = 1%
.....
2022-01-26 15:52:11,884 Stage-1 map = 100%,  reduce = 95%
2022-01-26 15:52:12,932 Stage-1 map = 100%,  reduce = 97%
2022-01-26 15:52:13,976 Stage-1 map = 100%,  reduce = 98%
2022-01-26 15:52:15,014 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1174400171_0007
Loading data to table imdb.persons
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 2396373507458 HDFS Write: 329348218924 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 128.984 seconds
hive> SELECT COUNT(*) FROM persons;
OK
11360021
Time taken: 0.155 seconds, Fetched: 1 row(s)
hive>

```

Let list the buckets in HDFS


```bash
root@ba74b599cf13:/opt# hdfs dfs -ls -h /user/hive/warehouse/imdb.db/persons
Found 500 items
-rwxrwxr-x   3 root supergroup    606.0 M 2022-01-26 15:50 /user/hive/warehouse/imdb.db/persons/000000_0
-rwxrwxr-x   3 root supergroup     50.3 K 2022-01-26 15:50 /user/hive/warehouse/imdb.db/persons/000001_0
-rwxrwxr-x   3 root supergroup     47.2 K 2022-01-26 15:50 /user/hive/warehouse/imdb.db/persons/000002_0
-rwxrwxr-x   3 root supergroup     ........
-rwxrwxr-x   3 root supergroup     41.1 K 2022-01-26 15:52 /user/hive/warehouse/imdb.db/persons/000499_0
root@ba74b599cf13:/opt#

```

As said earlier we have one huge buckets and smalls ones. It will be better to reduce their number.

- First we need to clear our HDFS folder `hdfs dfs -rm /user/hive/warehouse/imdb.db/persons/*`

```hive

hive>
    > DROP TABLE persons;
OK
Time taken: 1.08 seconds
hive> CREATE TABLE persons
    > (
    >     tconst STRING,
    >     primaryName STRING,
    >     primaryProfession STRING,
    >     knownForTitles STRING,
    >     birthYear int,
    >     deathYear INT
    > )
    > CLUSTERED BY (birthYear, deathYear) INTO 20 BUCKETS
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.084 seconds
hive> INSERT INTO TABLE persons
    > SELECT tconst, primaryName, primaryProfession, knownForTitles, birthYear, deathYear
    > FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126155731_068bcd96-2949-4cd9-acc9-9c8cea9974e4
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 20
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 15:57:33,528 Stage-1 map = 0%,  reduce = 0%
2022-01-26 15:57:39,531 Stage-1 map = 11%,  reduce = 0%
2022-01-26 15:57:42,847 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:57:48,853 Stage-1 map = 44%,  reduce = 0%
2022-01-26 15:57:51,856 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:57:55,840 Stage-1 map = 67%,  reduce = 0%
2022-01-26 15:57:56,841 Stage-1 map = 100%,  reduce = 0%
2022-01-26 15:58:02,851 Stage-1 map = 100%,  reduce = 3%
2022-01-26 15:58:21,901 Stage-1 map = 100%,  reduce = 20%
2022-01-26 15:58:22,905 Stage-1 map = 100%,  reduce = 45%
2022-01-26 15:58:23,913 Stage-1 map = 100%,  reduce = 75%
2022-01-26 15:58:24,895 Stage-1 map = 100%,  reduce = 95%
2022-01-26 15:58:25,899 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local189822697_0008
Loading data to table imdb.persons
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 124703025471 HDFS Write: 28840439886 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 54.67 seconds
hive>

```

- Until now we mainly discussed the `birthYear` and `deathYear`. But we could improve our modeling by using indices on `nconst` as it's unique peer person, and a `primaryName`. [Hive Indices Documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/AlterIndex) as part of Hive 2 (which is our case)

```bash
root@b0da2869f344:/# hive --version
Hive 2.3.2
Git git://stakiar-MBP.local/Users/stakiar/Desktop/scratch-space/apache-hive -r 857a9fd8ad725a53bd95c1b2d6612f9b1155f44d
Compiled by stakiar on Thu Nov 9 09:11:39 PST 2017
From source with checksum dc38920061a4eb32c4d15ebd5429ac8a

```

Pay attention to the fact that since Hive 3, indices are removed [Here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Indexing) and [HIVE-18448](https://issues.apache.org/jira/browse/HIVE-18448)


```hive
hive> set hive.cli.print.header=true;
hive> select * from persons limit 1;
OK
persons.tconst	persons.primaryname	persons.primaryprofession	persons.knownfortitles	persons.birthyear	persons.deathyear
nm8201491	Sophie-Anne Scherrer	actress	tt4720654	NULL	NULL
Time taken: 0.092 seconds, Fetched: 1 row(s)
hive>

```

` set hive.cli.print.header=true;` to enable the header of the table.

```hive
hive> CREATE INDEX tconst_index ON TABLE persons (tconst) AS 'COMPACT' WITH DEFERRED REBUILD;
OK
Time taken: 0.208 seconds

hive> CREATE INDEX primaryName_index ON TABLE persons (primaryName) AS 'COMPACT' WITH DEFERRED REBUILD;
OK
Time taken: 0.123 seconds
hive>

```

- Below some queries on those two columns

```hive
hive> select * from persons WHERE primaryName = 'Fred Astaire';
OK
persons.tconst	persons.primaryname	persons.primaryprofession	persons.knownfortitles	persons.birthyear	persons.deathyear
nm12584561	Fred Astaire		NULL	NULL	NULL
nm0000001	Fred Astaire	soundtrack,actor,miscellaneous	tt0050419,tt0072308,tt0031983,tt0053137	1899	1987
Time taken: 0.096 seconds, Fetched: 2 row(s)
hive> select * from persons_tmp WHERE primaryName = 'Fred Astaire';
OK
persons_tmp.tconst	persons_tmp.primaryname	persons_tmp.birthyear	persons_tmp.deathyear	persons_tmp.primaryprofession	persons_tmp.knownfortitles
nm0000001	Fred Astaire	1899	1987	soundtrack,actor,miscellaneous	tt0050419,tt0072308,tt0031983,tt0053137
nm12584561	Fred Astaire	NULL	NULL		NULL
Time taken: 0.108 seconds, Fetched: 2 row(s)
hive> select * from persons WHERE tconst = 'nm0000001';
OK
persons.tconst	persons.primaryname	persons.primaryprofession	persons.knownfortitles	persons.birthyear	persons.deathyear
nm0000001	Fred Astaire	soundtrack,actor,miscellaneous	tt0050419,tt0072308,tt0031983,tt0053137	1899	1987
Time taken: 0.111 seconds, Fetched: 1 row(s)
hive> select * from persons_tmp WHERE tconst = 'nm0000001';
OK
persons_tmp.tconst	persons_tmp.primaryname	persons_tmp.birthyear	persons_tmp.deathyear	persons_tmp.primaryprofession	persons_tmp.knownfortitles
nm0000001	Fred Astaire	1899	1987	soundtrack,actor,miscellaneous	tt0050419,tt0072308,tt0031983,tt0053137
Time taken: 0.092 seconds, Fetched: 1 row(s)
hive>

```

- Below some queries on dates:


```hive
hive> SELECT COUNT(*) FROM persons_tmp WHERE birthYear > 1920;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126162211_4d03c7b6-cd69-47bf-8e01-f6ed89283540
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 16:22:13,300 Stage-1 map = 0%,  reduce = 0%
2022-01-26 16:22:15,301 Stage-1 map = 100%,  reduce = 0%
2022-01-26 16:22:18,304 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1070392753_0009
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 45734936892 HDFS Write: 5445969192 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
_c0
448431
Time taken: 6.483 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(*) FROM persons WHERE birthYear > 1920;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126162228_029eacb7-c02b-4006-8a22-b065a6bb2eeb
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 16:22:29,903 Stage-1 map = 0%,  reduce = 0%
2022-01-26 16:22:30,904 Stage-1 map = 100%,  reduce = 0%
2022-01-26 16:22:34,907 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local529632651_0010
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 48457919972 HDFS Write: 5445969192 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
_c0
448431
Time taken: 6.38 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(*) FROM persons WHERE birthYear > 1920 AND deathYear < 2022;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126162258_a43fe2dc-83bb-43d4-91fb-f7702f9adff9
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 16:22:59,543 Stage-1 map = 0%,  reduce = 0%
2022-01-26 16:23:01,545 Stage-1 map = 100%,  reduce = 0%
2022-01-26 16:23:04,547 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1040713388_0011
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 51180894732 HDFS Write: 5445969192 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
_c0
97924
Time taken: 6.338 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(*) FROM persons_tmp WHERE birthYear > 1920 AND deathYear < 2022;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126162317_a498703f-4679-40f2-a5f8-21ff5b13b6d3
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 16:23:18,985 Stage-1 map = 0%,  reduce = 0%
2022-01-26 16:23:20,987 Stage-1 map = 100%,  reduce = 0%
2022-01-26 16:23:23,969 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local292739024_0012
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 53903877812 HDFS Write: 5445969192 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
_c0
97924
Time taken: 6.498 seconds, Fetched: 1 row(s)
hive>
```

We observe a better performances using `buckets` even if the difference in the duration is importants. This is due to the fact that `persons` has a huge bucket for `null` columns and `persons_tmp`.

- Query using Group By

The query below, retrieves each persons and group its professions

```hive
> SELECT primaryName, collect_set(primaryProfession) FROM persons GROUP BY primaryName ORDER BY primaryName DESC LIMIT 5;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126211846_9347b510-8938-4317-946d-37491d7d1654
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 21:18:48,206 Stage-1 map = 0%,  reduce = 0%
2022-01-26 21:18:53,209 Stage-1 map = 100%,  reduce = 0%
2022-01-26 21:19:03,261 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local808752865_0018
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 21:19:04,475 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local821073421_0019
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 97471755976 HDFS Write: 8168953788 SUCCESS
Stage-Stage-2:  HDFS Read: 32675970804 HDFS Write: 2722984596 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
primaryname	_c1
​Rebekah Brooks	[]
þórður Bogason	["actor,miscellaneous"]
þórunn Ósk Morinósdóttir	["music_department"]
álvaro Sousa	["sound_department"]
Þörir Marrow	["location_management"]
Time taken: 17.623 seconds, Fetched: 5 row(s)
hive>

```

Same idea but for known movies

```hive
hive> SELECT primaryName, collect_set(knownForTitles) as knownForTitles FROM persons GROUP BY primaryName ORDER BY primaryName DESC LIMIT 5;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220126212127_f4e48b56-150a-4b99-b8e2-ee8b80b241f6
Total jobs = 2
Launching Job 1 out of 2
Number of reduce tasks not specified. Estimated from input data size: 3
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 21:21:28,368 Stage-1 map = 0%,  reduce = 0%
2022-01-26 21:21:34,373 Stage-1 map = 100%,  reduce = 0%
2022-01-26 21:21:43,380 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1732987405_0020
Launching Job 2 out of 2
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-26 21:21:44,587 Stage-2 map = 100%,  reduce = 100%
Ended Job = job_local1459787205_0021
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 101556218116 HDFS Write: 8168953788 SUCCESS
Stage-Stage-2:  HDFS Read: 34037458184 HDFS Write: 2722984596 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
primaryname	knownfortitles
​Rebekah Brooks	[]
þórður Bogason	["tt9806258"]
þórunn Ósk Morinósdóttir	["tt0808285"]
álvaro Sousa	["tt3538776"]
Þörir Marrow	[]
Time taken: 17.568 seconds, Fetched: 5 row(s)
hive>

```


---

### Tables Creation

First let upload into HDFS the datasets

```bash
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.akas.tsv /
root@8cc681711ae0:/opt# hdfs dfs -ls /
Found 3 items
-rw-r--r--   3 root supergroup 1535986357 2022-01-27 15:29 /title.akas.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 15:19 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-27 15:19 /user
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.basics.tsv /
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.crew.tsv /
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.episode.tsv /
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.principals.tsv /
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/name.basics.tsv /
root@8cc681711ae0:/opt# hdfs dfs -ls /
Found 8 items
-rw-r--r--   3 root supergroup  680831344 2022-01-27 15:33 /name.basics.tsv
-rw-r--r--   3 root supergroup 1535986357 2022-01-27 15:29 /title.akas.tsv
-rw-r--r--   3 root supergroup  738652304 2022-01-27 15:31 /title.basics.tsv
-rw-r--r--   3 root supergroup  280057014 2022-01-27 15:31 /title.crew.tsv
-rw-r--r--   3 root supergroup  167206109 2022-01-27 15:31 /title.episode.tsv
-rw-r--r--   3 root supergroup 2138352514 2022-01-27 15:33 /title.principals.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 15:19 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-27 15:19 /user
root@8cc681711ae0:/opt# hdfs dfs -copyFromLocal /data/title.ratings.tsv /
root@8cc681711ae0:/opt# hdfs dfs -ls /
Found 9 items
-rw-r--r--   3 root supergroup  680831344 2022-01-27 15:33 /name.basics.tsv
-rw-r--r--   3 root supergroup 1535986357 2022-01-27 15:29 /title.akas.tsv
-rw-r--r--   3 root supergroup  738652304 2022-01-27 15:31 /title.basics.tsv
-rw-r--r--   3 root supergroup  280057014 2022-01-27 15:31 /title.crew.tsv
-rw-r--r--   3 root supergroup  167206109 2022-01-27 15:31 /title.episode.tsv
-rw-r--r--   3 root supergroup 2138352514 2022-01-27 15:33 /title.principals.tsv
-rw-r--r--   3 root supergroup   20817965 2022-01-27 15:34 /title.ratings.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 15:19 /tmp
drwxr-xr-x   - root supergroup          0 2022-01-27 15:19 /user
root@8cc681711ae0:/opt#

```

### `title.akas.tsv.gz - Contains the following information for titles:`

- `titleId (string)` - a tconst, an alphanumeric unique identifier of the title
- `ordering (integer)` – a number to uniquely identify rows for a given titleId
- `title (string)` – the localized title
- `region (string)` - the region for this version of the title
- `language (string)` - the language of the title
- `types (array)` - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
- `attributes (array)` - Additional terms to describe this alternative title, not enumerated
- `isOriginalTitle (boolean)` – 0: not original title; 1: original title


```
CREATE TABLE movies_tmp
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    isOriginalTitle BOOLEAN
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA INPATH '/title.akas.tsv' INTO TABLE movies_tmp;


```

```hive
hive> CREATE TABLE movies_tmp
    > (
    >     titleId STRING,
    >     ordering INT,
    >     title STRING,
    >     region STRING,
    >     language STRING,
    >     types STRING,
    >     attributes STRING,
    >     isOriginalTitle BOOLEAN
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.75 seconds
hive> LOAD DATA INPATH '/title.akas.tsv' INTO TABLE movies_tmp;
Loading data to table imdb.movies_tmp
OK
Time taken: 1.111 seconds
hive> SELECT COUNT(*) FROM movies_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127153726_987361da-6ca9-4749-946e-86b14d20547a
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 15:37:30,045 Stage-1 map = 0%,  reduce = 0%
2022-01-27 15:37:32,049 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:37:42,129 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local748640378_0001
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 7098947062 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
30802936
Time taken: 15.715 seconds, Fetched: 1 row(s)
hive>

```


```bash
root@8cc681711ae0:/opt# hdfs dfs -ls -R /user/hive/warehouse/
drwxrwxr-x   - root supergroup          0 2022-01-27 15:37 /user/hive/warehouse/imdb.db
drwxrwxr-x   - root supergroup          0 2022-01-27 15:37 /user/hive/warehouse/imdb.db/movies_tmp
-rwxrwxr-x   3 root supergroup 1535986357 2022-01-27 15:29 /user/hive/warehouse/imdb.db/movies_tmp/title.akas.tsv
root@8cc681711ae0:/opt#

```

Let try to create buckets for this table using for example `language`

```hive
hive> SELECT COUNT(DISTINCT language) FROM movies_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127154218_8b72e639-45eb-4e36-b8a5-b3d545446b0e
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 1
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 15:42:19,881 Stage-1 map = 0%,  reduce = 0%
2022-01-27 15:42:21,882 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:42:32,947 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1278105049_0004
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 39355606735 HDFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
106
Time taken: 14.444 seconds, Fetched: 1 row(s)
hive>

```

We could create a new table by using clusters on the column `language`


```hive
CREATE TABLE movies
(
    titleId STRING,
    ordering INT,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    isOriginalTitle BOOLEAN
)
CLUSTERED BY (language) INTO 100 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

```

```hive
INSERT INTO TABLE movies
SELECT titleId, ordering, title, region, language, types, attributes, isOriginalTitle
FROM movies_tmp;

hive> INSERT INTO TABLE movies
    > SELECT titleId, ordering, title, region, language, types, attributes, isOriginalTitle
    > FROM movies_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127155023_e8f8f3f6-38b5-4bed-ad85-2ff365a21ae8
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 100
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 15:50:24,867 Stage-1 map = 0%,  reduce = 0%
2022-01-27 15:50:30,872 Stage-1 map = 6%,  reduce = 0%
2022-01-27 15:50:35,865 Stage-1 map = 16%,  reduce = 0%
2022-01-27 15:50:36,869 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:50:42,890 Stage-1 map = 22%,  reduce = 0%
2022-01-27 15:50:48,920 Stage-1 map = 32%,  reduce = 0%
2022-01-27 15:50:49,924 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:50:55,945 Stage-1 map = 39%,  reduce = 0%
2022-01-27 15:51:01,944 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:51:07,966 Stage-1 map = 56%,  reduce = 0%
2022-01-27 15:51:13,999 Stage-1 map = 66%,  reduce = 0%
2022-01-27 15:51:15,007 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:51:21,058 Stage-1 map = 72%,  reduce = 0%
2022-01-27 15:51:27,081 Stage-1 map = 80%,  reduce = 0%
2022-01-27 15:51:28,085 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:51:34,085 Stage-1 map = 91%,  reduce = 0%
2022-01-27 15:51:37,095 Stage-1 map = 98%,  reduce = 0%
2022-01-27 15:51:38,171 Stage-1 map = 100%,  reduce = 0%
2022-01-27 15:51:44,234 Stage-1 map = 100%,  reduce = 1%
2022-01-27 15:51:59,380 Stage-1 map = 100%,  reduce = 2%
2022-01-27 15:52:03,422 Stage-1 map = 100%,  reduce = 8%
....
2022-01-27 15:53:22,766 Stage-1 map = 100%,  reduce = 93%
2022-01-27 15:53:23,775 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1242414560_0005
Loading data to table imdb.movies
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 810447924601 HDFS Write: 87325560779 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 183.305 seconds
hive>

```

```bash
root@8cc681711ae0:/opt# hdfs dfs -ls -R /user/hive/warehouse/imdb.db/movies
drwxrwxr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1
drwxr-xr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1/-ext-10001
drwxr-xr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1/_tmp.-ext-10000
root@8cc681711ae0:/opt#

```

### `title.basics.tsv.gz - Contains the following information for titles:`

- `titleId (string)` - alphanumeric unique identifier of the title
- `titleType (string)` – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
- `primaryTitle (string)` – the more popular title / the title used by the filmmakers on promotional materials at the point of release
- `originalTitle (string)` - original title, in the original language
- `isAdult (boolean)` - 0: non-adult title; 1: adult title
- `startYear (YYYY)` – represents the release year of a title. In the case of TV Series, it is the series start year
- `endYear (YYYY)` – TV Series end year. ‘\N’ for all other title types
- `runtimeMinutes` – primary runtime of the title, in minutes
- `genres (string array)` – includes up to three genres associated with the title


```hive

```hive
CREATE TABLE movies_details_tmp
(
    titleId STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult BOOLEAN,
    language STRING,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres STRING

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

CREATE TABLE movies_details
(
    titleId STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult BOOLEAN,
    language STRING,
    startYear INT,
    endYear INT,
    runtimeMinutes INT,
    genres STRING

)
CLUSTERED BY (startYear, endYear) INTO 100 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

```

`LOAD DATA INPATH '/title.basics.tsv' INTO TABLE movies_details_tmp;`

```
INSERT INTO TABLE movies_details
SELECT titleId, titleType, primaryTitle, originalTitle, isAdult, language, startYear, endYear, runtimeMinutes, genres
FROM movies_details_tmp;
```

```hive

hive> CREATE TABLE movies_details_tmp
    > (
    >     titleId STRING,
    >     titleType STRING,
    >     primaryTitle STRING,
    >     originalTitle STRING,
    >     isAdult BOOLEAN,
    >     language STRING,
    >     startYear INT,
    >     endYear INT,
    >     runtimeMinutes INT,
    >     genres STRING
    >
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.088 seconds
hive>
    > CREATE TABLE movies_details
    > (
    >     titleId STRING,
    >     titleType STRING,
    >     primaryTitle STRING,
    >     originalTitle STRING,
    >     isAdult BOOLEAN,
    >     language STRING,
    >     startYear INT,
    >     endYear INT,
    >     runtimeMinutes INT,
    >     genres STRING
    >
    > )
    > CLUSTERED BY (startYear, endYear) INTO 100 BUCKETS
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.092 seconds
hive> LOAD DATA INPATH '/title.basics.tsv' INTO TABLE movies_details_tmp;
Loading data to table imdb.movies_details_tmp
OK
Time taken: 0.522 seconds
hive> INSERT INTO TABLE movies_details
    > SELECT titleId, titleType, primaryTitle, originalTitle, isAdult, language, startYear, endYear, runtimeMinutes, genres
    > FROM movies_details_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127160035_19816faf-389f-4c4c-902c-7bde2fcefd20
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 100
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:00:37,011 Stage-1 map = 0%,  reduce = 0%
2022-01-27 16:00:45,018 Stage-1 map = 11%,  reduce = 0%
2022-01-27 16:00:48,024 Stage-1 map = 22%,  reduce = 0%
2022-01-27 16:00:50,033 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:00:56,052 Stage-1 map = 44%,  reduce = 0%
2022-01-27 16:01:02,048 Stage-1 map = 56%,  reduce = 0%
2022-01-27 16:01:04,055 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:01:09,073 Stage-1 map = 81%,  reduce = 0%
2022-01-27 16:01:12,083 Stage-1 map = 89%,  reduce = 0%
2022-01-27 16:01:14,094 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:01:20,136 Stage-1 map = 100%,  reduce = 1%
2022-01-27 16:01:29,204 Stage-1 map = 100%,  reduce = 2%
2022-01-27 16:01:30,213 Stage-1 map = 100%,  reduce = 4%
2022-01-27 16:01:31,202 Stage-1 map = 100%,  reduce = 7%
....
2022-01-27 16:01:54,515 Stage-1 map = 100%,  reduce = 90%
2022-01-27 16:01:55,523 Stage-1 map = 100%,  reduce = 97%
2022-01-27 16:01:56,537 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local444868854_0006
Loading data to table imdb.movies_details
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 866476086382 HDFS Write: 224369274111 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 82.859 seconds
hive>
```

### `title.crew.tsv.gz – Contains the director and writer information for all the titles in IMDb:`

- `titleId (string)` - alphanumeric unique identifier of the title
- `directors (array of nconsts)` - director(s) of the given title
- `writers (array of nconsts)` – writer(s) of the given title

```hive
CREATE TABLE crew
(
    titleId STRING,
    directors STRING,
    writers STRING

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;
LOAD DATA INPATH '/title.crew.tsv' INTO TABLE crew;

```

`LOAD DATA INPATH '/title.crew.tsv' INTO TABLE crew;`

```hive
hive> CREATE TABLE crew
    > (
    >     titleId STRING,
    >     directors STRING,
    >     writers STRING
    >
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.149 seconds
hive> LOAD DATA INPATH '/title.crew.tsv' INTO TABLE crew;
Loading data to table imdb.crew
OK
Time taken: 0.401 seconds
hive>

```

### `title.episode.tsv.gz – Contains the tv episode information. Fields include`

- `titleId (string)` - alphanumeric identifier of episode
- `parentTconst (string)` - alphanumeric identifier of the parent TV Series
- `seasonNumber (integer)` – season number the episode belongs to
- `episodeNumber (integer)` – episode number of the tconst in the TV series


```hive
CREATE TABLE episodes_tmp
     (
         titleId STRING,
         parentTconst STRING,
         seasonNumber INT,
         episodeNumber INT
    
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;
LOAD DATA INPATH '/title.episode.tsv' INTO TABLE episodes_tmp;
CREATE TABLE episodes
     (
         titleId STRING,
         parentTconst STRING,
         seasonNumber INT,
         episodeNumber INT
    
     )
     CLUSTERED BY (seasonNumber, episodeNumber) INTO 100 BUCKETS
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;

INSERT INTO TABLE episodes
SELECT titleId, parentTconst, seasonNumber, episodeNumber
FROM episodes;
```

```hive
hive> CREATE TABLE episodes_tmp
    >      (
    >          titleId STRING,
    >          parentTconst STRING,
    >          seasonNumber INT,
    >          episodeNumber INT
    >
    >      )
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.068 seconds
hive> LOAD DATA INPATH '/title.episode.tsv' INTO TABLE episodes_tmp;
Loading data to table imdb.episodes_tmp
OK
Time taken: 0.375 seconds
hive> CREATE TABLE episodes
    >      (
    >          titleId STRING,
    >          parentTconst STRING,
    >          seasonNumber INT,
    >          episodeNumber INT
    >
    >      )
    >      CLUSTERED BY (seasonNumber, episodeNumber) INTO 100 BUCKETS
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.063 seconds
hive>
    > INSERT INTO TABLE episodes
    > SELECT titleId, parentTconst, seasonNumber, episodeNumber
    > FROM episodes;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127161732_b94499ec-bdaf-485c-8185-b192aaba3952
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 100
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:17:33,831 Stage-1 map = 0%,  reduce = 10%
2022-01-27 16:17:34,840 Stage-1 map = 0%,  reduce = 23%
2022-01-27 16:17:35,850 Stage-1 map = 0%,  reduce = 40%
2022-01-27 16:17:36,861 Stage-1 map = 0%,  reduce = 46%
2022-01-27 16:17:37,870 Stage-1 map = 0%,  reduce = 59%
2022-01-27 16:17:38,876 Stage-1 map = 0%,  reduce = 67%
2022-01-27 16:17:39,886 Stage-1 map = 0%,  reduce = 74%
2022-01-27 16:17:40,896 Stage-1 map = 0%,  reduce = 79%
2022-01-27 16:17:41,907 Stage-1 map = 0%,  reduce = 85%
2022-01-27 16:17:42,917 Stage-1 map = 0%,  reduce = 87%
2022-01-27 16:17:43,928 Stage-1 map = 0%,  reduce = 91%
2022-01-27 16:17:44,937 Stage-1 map = 0%,  reduce = 100%
Ended Job = job_local1139142049_0007
Loading data to table imdb.episodes
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 841898740200 HDFS Write: 226379020450 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 13.952 seconds
hive>
```

### `title.principals.tsv.gz – Contains the principal cast/crew for titles`

- titleId (string) - alphanumeric unique identifier of the title
- ordering (integer) – a number to uniquely identify rows for a given titleId
- nconst (string) - alphanumeric unique identifier of the name/person
- category (string) - the category of job that person was in
- job (string) - the specific job title if applicable, else '\N'
- characters (string) - the name of the character played if applicable, else '\N'


```hive
CREATE TABLE principals
     (
         titleId STRING,
         ordering STRING,
         nconst STRING,
         category STRING,
         job STRING,
         characters STRING
    
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;
LOAD DATA INPATH '/title.principals.tsv' INTO TABLE principals;
```

```hive
hive> CREATE TABLE principals
    >      (
    >          titleId STRING,
    >          ordering STRING,
    >          nconst STRING,
    >          category STRING,
    >          job STRING,
    >          characters STRING
    >
    >      )
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.118 seconds
hive> LOAD DATA INPATH '/title.principals.tsv' INTO TABLE principals;
Loading data to table imdb.principals
OK
Time taken: 0.446 seconds
hive>

```

### `title.ratings.tsv.gz – Contains the IMDb rating and votes information for titles`

- `tconst (string)` - alphanumeric unique identifier of the title
- `averageRating` – weighted average of all the individual user ratings
- `numVotes` - number of votes the title has received


```hive
CREATE TABLE ratings_tmp
     (
         titleId STRING,
         averageRating DOUBLE,
         numVotes INT
    
     )
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;
LOAD DATA INPATH '/title.ratings.tsv' INTO TABLE ratings_tmp;
CREATE TABLE ratings
     (
         titleId STRING,
         averageRating DOUBLE,
         numVotes INT
    
     )
     CLUSTERED BY (averageRating) INTO 10 BUCKETS
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;

INSERT INTO TABLE ratings
SELECT titleId, averageRating, numVotes
FROM ratings_tmp;
```

```hive
hive> CREATE TABLE ratings_tmp
    >      (
    >          titleId STRING,
    >          averageRating DOUBLE,
    >          numVotes INT
    >
    >      )
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.126 seconds
hive> LOAD DATA INPATH '/title.ratings.tsv' INTO TABLE ratings_tmp;
Loading data to table imdb.ratings_tmp
OK
Time taken: 0.475 seconds
hive> CREATE TABLE ratings
    >      (
    >          titleId STRING,
    >          averageRating DOUBLE,
    >          numVotes INT
    >
    >      )
    >      CLUSTERED BY (averageRating) INTO 10 BUCKETS
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.08 seconds
hive> INSERT INTO TABLE ratings
    > SELECT titleId, averageRating, numVotes
    > FROM ratings_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127163307_1cce0050-32d8-44ed-a43a-087e036a6a58
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 10
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:33:08,628 Stage-1 map = 0%,  reduce = 0%
2022-01-27 16:33:10,596 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:33:11,597 Stage-1 map = 100%,  reduce = 10%
2022-01-27 16:33:12,599 Stage-1 map = 100%,  reduce = 20%
2022-01-27 16:33:14,601 Stage-1 map = 100%,  reduce = 30%
2022-01-27 16:33:15,603 Stage-1 map = 100%,  reduce = 40%
2022-01-27 16:33:17,606 Stage-1 map = 100%,  reduce = 50%
2022-01-27 16:33:18,608 Stage-1 map = 100%,  reduce = 60%
2022-01-27 16:33:19,610 Stage-1 map = 100%,  reduce = 70%
2022-01-27 16:33:20,611 Stage-1 map = 100%,  reduce = 80%
2022-01-27 16:33:22,614 Stage-1 map = 100%,  reduce = 90%
2022-01-27 16:33:23,617 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1084435492_0008
Loading data to table imdb.ratings
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 92838623273 HDFS Write: 25017371840 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 16.942 seconds
hive>

```

### `name.basics.tsv.gz – Contains the following information for names:`

- `nconst` (string) - alphanumeric unique identifier of the name/person
- `primaryName` (string)– name by which the person is most often credited
- `birthYear` – in YYYY format
- `deathYear` – in YYYY format if applicable, else '\N'
- `primaryProfession` (array of strings)– the top-3 professions of the person
- `knownForTitles` (array of tconsts) – titles the person is known for


```hive
CREATE TABLE persons_tmp
(
    tconst STRING,
    primaryName STRING,
    birthYear INT,
    deathYear INT,
    primaryProfession STRING,
    knownForTitles STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA INPATH '/name.basics.tsv' INTO TABLE persons_tmp;

CREATE TABLE persons
(
    tconst STRING,
    primaryName STRING,
    primaryProfession STRING,
    knownForTitles STRING,
    birthYear int, 
    deathYear INT
)
CLUSTERED BY (birthYear, deathYear) INTO 20 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE persons
SELECT tconst, primaryName, primaryProfession, knownForTitles, birthYear, deathYear
FROM persons_tmp;
```


```hive
hive> CREATE TABLE persons_tmp
    > (
    >     tconst STRING,
    >     primaryName STRING,
    >     birthYear INT,
    >     deathYear INT,
    >     primaryProfession STRING,
    >     knownForTitles STRING
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.102 seconds
hive>
    > LOAD DATA INPATH '/name.basics.tsv' INTO TABLE persons_tmp;
Loading data to table imdb.persons_tmp
OK
Time taken: 0.426 seconds
hive> CREATE TABLE persons
    > (
    >     tconst STRING,
    >     primaryName STRING,
    >     primaryProfession STRING,
    >     knownForTitles STRING,
    >     birthYear int,
    >     deathYear INT
    > )
    > CLUSTERED BY (birthYear, deathYear) INTO 20 BUCKETS
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.081 seconds
hive> INSERT INTO TABLE persons
    > SELECT tconst, primaryName, primaryProfession, knownForTitles, birthYear, deathYear
    > FROM persons_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127163646_cc1af5a6-ba88-4371-8de0-61e7cd0b3de0
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 20
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:36:48,495 Stage-1 map = 0%,  reduce = 0%
...
2022-01-27 16:39:07,659 Stage-1 map = 100%,  reduce = 80%
2022-01-27 16:39:08,662 Stage-1 map = 100%,  reduce = 95%
2022-01-27 16:39:09,632 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1132467771_0009
Loading data to table imdb.persons
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 209221112402 HDFS Write: 65730826822 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
Time taken: 143.826 seconds
hive>

```

## HDFS

```bash
root@8cc681711ae0:/opt# hdfs dfs -ls -R /user/hive/warehouse/imdb.db/
drwxrwxr-x   - root supergroup          0 2022-01-27 16:12 /user/hive/warehouse/imdb.db/crew
-rwxrwxr-x   3 root supergroup  280057014 2022-01-27 15:31 /user/hive/warehouse/imdb.db/crew/title.crew.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000000_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000001_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000002_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000003_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000004_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000005_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000006_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000007_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000008_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000009_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000010_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000011_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000012_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000013_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000014_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000015_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000016_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000017_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000018_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000019_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000020_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000021_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000022_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000023_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000024_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000025_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000026_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000027_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000028_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000029_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000030_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000031_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000032_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000033_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000034_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000035_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000036_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000037_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000038_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000039_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000040_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000041_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000042_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000043_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000044_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000045_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000046_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000047_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000048_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000049_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000050_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000051_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000052_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000053_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000054_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000055_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000056_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000057_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000058_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000059_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000060_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000061_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000062_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000063_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000064_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000065_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000066_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000067_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000068_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000069_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000070_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000071_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000072_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000073_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000074_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000075_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000076_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000077_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000078_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000079_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000080_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000081_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000082_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000083_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000084_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000085_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000086_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000087_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000088_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000089_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000090_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000091_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000092_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000093_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000094_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000095_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000096_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000097_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000098_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes/000099_0
drwxrwxr-x   - root supergroup          0 2022-01-27 16:17 /user/hive/warehouse/imdb.db/episodes_tmp
-rwxrwxr-x   3 root supergroup  167206109 2022-01-27 15:31 /user/hive/warehouse/imdb.db/episodes_tmp/title.episode.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies
-rwxrwxr-x   3 root supergroup  328418165 2022-01-27 15:51 /user/hive/warehouse/imdb.db/movies/000000_0
-rwxrwxr-x   3 root supergroup  147822929 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000001_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000002_0
-rwxrwxr-x   3 root supergroup        298 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000003_0
-rwxrwxr-x   3 root supergroup      63059 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000004_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000005_0
-rwxrwxr-x   3 root supergroup         83 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000006_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000007_0
-rwxrwxr-x   3 root supergroup        766 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000008_0
-rwxrwxr-x   3 root supergroup     329408 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000009_0
-rwxrwxr-x   3 root supergroup    1565719 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000010_0
-rwxrwxr-x   3 root supergroup         97 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000011_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000012_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000013_0
-rwxrwxr-x   3 root supergroup       2848 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000014_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000015_0
-rwxrwxr-x   3 root supergroup         60 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000016_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000017_0
-rwxrwxr-x   3 root supergroup      77513 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000018_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000019_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000020_0
-rwxrwxr-x   3 root supergroup      62090 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000021_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000022_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000023_0
-rwxrwxr-x   3 root supergroup       3074 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000024_0
-rwxrwxr-x   3 root supergroup     395292 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000025_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000026_0
-rwxrwxr-x   3 root supergroup       6110 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000027_0
-rwxrwxr-x   3 root supergroup     935869 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000028_0
-rwxrwxr-x   3 root supergroup  221822602 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000029_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000030_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000031_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000032_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000033_0
-rwxrwxr-x   3 root supergroup      33437 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000034_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000035_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000036_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000037_0
-rwxrwxr-x   3 root supergroup      86048 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000038_0
-rwxrwxr-x   3 root supergroup       4343 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000039_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000040_0
-rwxrwxr-x   3 root supergroup   19799862 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000041_0
-rwxrwxr-x   3 root supergroup        705 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000042_0
-rwxrwxr-x   3 root supergroup      31490 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000043_0
-rwxrwxr-x   3 root supergroup         52 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000044_0
-rwxrwxr-x   3 root supergroup       9339 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000045_0
-rwxrwxr-x   3 root supergroup  160793310 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000046_0
-rwxrwxr-x   3 root supergroup       1285 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000047_0
-rwxrwxr-x   3 root supergroup      23776 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000048_0
-rwxrwxr-x   3 root supergroup       1467 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000049_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000050_0
-rwxrwxr-x   3 root supergroup    2167495 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000051_0
-rwxrwxr-x   3 root supergroup         37 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000052_0
-rwxrwxr-x   3 root supergroup      19408 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000053_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000054_0
-rwxrwxr-x   3 root supergroup      25389 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000055_0
-rwxrwxr-x   3 root supergroup       3939 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000056_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000057_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000058_0
-rwxrwxr-x   3 root supergroup     165933 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000059_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000060_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000061_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000062_0
-rwxrwxr-x   3 root supergroup        263 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000063_0
-rwxrwxr-x   3 root supergroup       1329 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000064_0
-rwxrwxr-x   3 root supergroup        376 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000065_0
-rwxrwxr-x   3 root supergroup     141057 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000066_0
-rwxrwxr-x   3 root supergroup       4865 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000067_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000068_0
-rwxrwxr-x   3 root supergroup       3522 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000069_0
-rwxrwxr-x   3 root supergroup        168 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000070_0
-rwxrwxr-x   3 root supergroup  160257807 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000071_0
-rwxrwxr-x   3 root supergroup      24522 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000072_0
-rwxrwxr-x   3 root supergroup      44106 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000073_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000074_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000075_0
-rwxrwxr-x   3 root supergroup  164839374 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000076_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000077_0
-rwxrwxr-x   3 root supergroup         49 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000078_0
-rwxrwxr-x   3 root supergroup     249697 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000079_0
-rwxrwxr-x   3 root supergroup        703 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000080_0
-rwxrwxr-x   3 root supergroup        284 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000081_0
-rwxrwxr-x   3 root supergroup         53 2022-01-27 15:52 /user/hive/warehouse/imdb.db/movies/000082_0
-rwxrwxr-x   3 root supergroup  188281987 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000083_0
-rwxrwxr-x   3 root supergroup      63820 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000084_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000085_0
-rwxrwxr-x   3 root supergroup       4010 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000086_0
-rwxrwxr-x   3 root supergroup      46898 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000087_0
-rwxrwxr-x   3 root supergroup  168036768 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000088_0
-rwxrwxr-x   3 root supergroup        233 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000089_0
-rwxrwxr-x   3 root supergroup       9131 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000090_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000091_0
-rwxrwxr-x   3 root supergroup         64 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000092_0
-rwxrwxr-x   3 root supergroup      54343 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000093_0
-rwxrwxr-x   3 root supergroup      14386 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000094_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000095_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000096_0
-rwxrwxr-x   3 root supergroup      32469 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000097_0
-rwxrwxr-x   3 root supergroup          0 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000098_0
-rwxrwxr-x   3 root supergroup       1503 2022-01-27 15:53 /user/hive/warehouse/imdb.db/movies/000099_0
drwxrwxr-x   - root supergroup          0 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details
-rwxrwxr-x   3 root supergroup  510177486 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000000_0
-rwxrwxr-x   3 root supergroup    1853611 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000001_0
-rwxrwxr-x   3 root supergroup    2475354 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000002_0
-rwxrwxr-x   3 root supergroup    3678330 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000003_0
-rwxrwxr-x   3 root supergroup    3658275 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000004_0
-rwxrwxr-x   3 root supergroup    4389533 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000005_0
-rwxrwxr-x   3 root supergroup    3361685 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000006_0
-rwxrwxr-x   3 root supergroup    3476254 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000007_0
-rwxrwxr-x   3 root supergroup    3289316 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000008_0
-rwxrwxr-x   3 root supergroup    2761487 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000009_0
-rwxrwxr-x   3 root supergroup    5048808 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000010_0
-rwxrwxr-x   3 root supergroup    3710174 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000011_0
-rwxrwxr-x   3 root supergroup    3128437 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000012_0
-rwxrwxr-x   3 root supergroup    2568637 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000013_0
-rwxrwxr-x   3 root supergroup    2338080 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000014_0
-rwxrwxr-x   3 root supergroup    4189546 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000015_0
-rwxrwxr-x   3 root supergroup    1783073 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000016_0
-rwxrwxr-x   3 root supergroup    1784625 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000017_0
-rwxrwxr-x   3 root supergroup    1795728 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000018_0
-rwxrwxr-x   3 root supergroup    1774498 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000019_0
-rwxrwxr-x   3 root supergroup    5207370 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000020_0
-rwxrwxr-x   3 root supergroup    4359676 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000021_0
-rwxrwxr-x   3 root supergroup    7250124 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000022_0
-rwxrwxr-x   3 root supergroup    4337933 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000023_0
-rwxrwxr-x   3 root supergroup    4774172 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000024_0
-rwxrwxr-x   3 root supergroup    4377321 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000025_0
-rwxrwxr-x   3 root supergroup    2333227 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000026_0
-rwxrwxr-x   3 root supergroup    1977206 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000027_0
-rwxrwxr-x   3 root supergroup    2535613 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000028_0
-rwxrwxr-x   3 root supergroup    1708249 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000029_0
-rwxrwxr-x   3 root supergroup    9045663 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000030_0
-rwxrwxr-x   3 root supergroup     723697 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000031_0
-rwxrwxr-x   3 root supergroup     742035 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000032_0
-rwxrwxr-x   3 root supergroup     622663 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000033_0
-rwxrwxr-x   3 root supergroup     711186 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000034_0
-rwxrwxr-x   3 root supergroup     919704 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000035_0
-rwxrwxr-x   3 root supergroup     614389 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000036_0
-rwxrwxr-x   3 root supergroup     662935 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000037_0
-rwxrwxr-x   3 root supergroup     712058 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000038_0
-rwxrwxr-x   3 root supergroup     613258 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000039_0
-rwxrwxr-x   3 root supergroup    1890058 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000040_0
-rwxrwxr-x   3 root supergroup    1571702 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000041_0
-rwxrwxr-x   3 root supergroup    2877500 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000042_0
-rwxrwxr-x   3 root supergroup    3593263 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000043_0
-rwxrwxr-x   3 root supergroup    5267596 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000044_0
-rwxrwxr-x   3 root supergroup    4223646 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000045_0
-rwxrwxr-x   3 root supergroup    1347900 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000046_0
-rwxrwxr-x   3 root supergroup    1253364 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000047_0
-rwxrwxr-x   3 root supergroup    1246014 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000048_0
-rwxrwxr-x   3 root supergroup     901305 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000049_0
-rwxrwxr-x   3 root supergroup    2412513 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000050_0
-rwxrwxr-x   3 root supergroup     975297 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000051_0
-rwxrwxr-x   3 root supergroup    1732973 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000052_0
-rwxrwxr-x   3 root supergroup     924647 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000053_0
-rwxrwxr-x   3 root supergroup    1121329 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000054_0
-rwxrwxr-x   3 root supergroup    1432305 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000055_0
-rwxrwxr-x   3 root supergroup     748850 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000056_0
-rwxrwxr-x   3 root supergroup     782408 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000057_0
-rwxrwxr-x   3 root supergroup    1259844 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000058_0
-rwxrwxr-x   3 root supergroup     938754 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000059_0
-rwxrwxr-x   3 root supergroup    7736210 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000060_0
-rwxrwxr-x   3 root supergroup     528496 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000061_0
-rwxrwxr-x   3 root supergroup     586578 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000062_0
-rwxrwxr-x   3 root supergroup     515574 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000063_0
-rwxrwxr-x   3 root supergroup     443912 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000064_0
-rwxrwxr-x   3 root supergroup     830749 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000065_0
-rwxrwxr-x   3 root supergroup     393941 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000066_0
-rwxrwxr-x   3 root supergroup     402599 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000067_0
-rwxrwxr-x   3 root supergroup     402949 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000068_0
-rwxrwxr-x   3 root supergroup     387985 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000069_0
-rwxrwxr-x   3 root supergroup    1018376 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000070_0
-rwxrwxr-x   3 root supergroup     433002 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000071_0
-rwxrwxr-x   3 root supergroup     683995 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000072_0
-rwxrwxr-x   3 root supergroup     509006 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000073_0
-rwxrwxr-x   3 root supergroup     465672 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000074_0
-rwxrwxr-x   3 root supergroup    1065400 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000075_0
-rwxrwxr-x   3 root supergroup     519408 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000076_0
-rwxrwxr-x   3 root supergroup     483229 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000077_0
-rwxrwxr-x   3 root supergroup     576338 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000078_0
-rwxrwxr-x   3 root supergroup     532108 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000079_0
-rwxrwxr-x   3 root supergroup    1763627 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000080_0
-rwxrwxr-x   3 root supergroup     519755 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000081_0
-rwxrwxr-x   3 root supergroup     720618 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000082_0
-rwxrwxr-x   3 root supergroup     706090 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000083_0
-rwxrwxr-x   3 root supergroup     770949 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000084_0
-rwxrwxr-x   3 root supergroup    1286267 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000085_0
-rwxrwxr-x   3 root supergroup     838055 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000086_0
-rwxrwxr-x   3 root supergroup     883733 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000087_0
-rwxrwxr-x   3 root supergroup     988184 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000088_0
-rwxrwxr-x   3 root supergroup    1180418 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000089_0
-rwxrwxr-x   3 root supergroup    4022129 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000090_0
-rwxrwxr-x   3 root supergroup     720826 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000091_0
-rwxrwxr-x   3 root supergroup     810886 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000092_0
-rwxrwxr-x   3 root supergroup     844394 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000093_0
-rwxrwxr-x   3 root supergroup     692237 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000094_0
-rwxrwxr-x   3 root supergroup    1160288 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000095_0
-rwxrwxr-x   3 root supergroup     917709 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000096_0
-rwxrwxr-x   3 root supergroup     617150 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000097_0
-rwxrwxr-x   3 root supergroup     599763 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000098_0
-rwxrwxr-x   3 root supergroup     455749 2022-01-27 16:01 /user/hive/warehouse/imdb.db/movies_details/000099_0
drwxrwxr-x   - root supergroup          0 2022-01-27 15:59 /user/hive/warehouse/imdb.db/movies_details_tmp
-rwxrwxr-x   3 root supergroup  738652304 2022-01-27 15:31 /user/hive/warehouse/imdb.db/movies_details_tmp/title.basics.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 15:37 /user/hive/warehouse/imdb.db/movies_tmp
-rwxrwxr-x   3 root supergroup 1535986357 2022-01-27 15:29 /user/hive/warehouse/imdb.db/movies_tmp/title.akas.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons
-rwxrwxr-x   3 root supergroup  637731972 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000000_0
-rwxrwxr-x   3 root supergroup    2286748 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000001_0
-rwxrwxr-x   3 root supergroup    2302870 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000002_0
-rwxrwxr-x   3 root supergroup    2158342 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000003_0
-rwxrwxr-x   3 root supergroup    2295620 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000004_0
-rwxrwxr-x   3 root supergroup    2200121 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000005_0
-rwxrwxr-x   3 root supergroup    2297685 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000006_0
-rwxrwxr-x   3 root supergroup    2236138 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000007_0
-rwxrwxr-x   3 root supergroup    2269008 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000008_0
-rwxrwxr-x   3 root supergroup    2277610 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000009_0
-rwxrwxr-x   3 root supergroup    2294855 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000010_0
-rwxrwxr-x   3 root supergroup    2317618 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000011_0
-rwxrwxr-x   3 root supergroup    2252667 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000012_0
-rwxrwxr-x   3 root supergroup    2284331 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000013_0
-rwxrwxr-x   3 root supergroup    2220015 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000014_0
-rwxrwxr-x   3 root supergroup    2284330 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000015_0
-rwxrwxr-x   3 root supergroup    2217048 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000016_0
-rwxrwxr-x   3 root supergroup    2308671 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000017_0
-rwxrwxr-x   3 root supergroup    2328820 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000018_0
-rwxrwxr-x   3 root supergroup    2266811 2022-01-27 16:39 /user/hive/warehouse/imdb.db/persons/000019_0
drwxrwxr-x   - root supergroup          0 2022-01-27 16:36 /user/hive/warehouse/imdb.db/persons_tmp
-rwxrwxr-x   3 root supergroup  680831344 2022-01-27 15:33 /user/hive/warehouse/imdb.db/persons_tmp/name.basics.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 16:29 /user/hive/warehouse/imdb.db/principals
-rwxrwxr-x   3 root supergroup 2138352514 2022-01-27 15:33 /user/hive/warehouse/imdb.db/principals/title.principals.tsv
drwxrwxr-x   - root supergroup          0 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings
-rwxrwxr-x   3 root supergroup    2224989 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000000_0
-rwxrwxr-x   3 root supergroup    1969181 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000001_0
-rwxrwxr-x   3 root supergroup    2749825 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000002_0
-rwxrwxr-x   3 root supergroup     924887 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000003_0
-rwxrwxr-x   3 root supergroup    3027604 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000004_0
-rwxrwxr-x   3 root supergroup    1528968 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000005_0
-rwxrwxr-x   3 root supergroup    2315152 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000006_0
-rwxrwxr-x   3 root supergroup    1767757 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000007_0
-rwxrwxr-x   3 root supergroup    2524883 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000008_0
-rwxrwxr-x   3 root supergroup    1784702 2022-01-27 16:33 /user/hive/warehouse/imdb.db/ratings/000009_0
drwxrwxr-x   - root supergroup          0 2022-01-27 16:32 /user/hive/warehouse/imdb.db/ratings_tmp
-rwxrwxr-x   3 root supergroup   20817965 2022-01-27 15:34 /user/hive/warehouse/imdb.db/ratings_tmp/title.ratings.tsv
root@8cc681711ae0:/opt#


```

## Queries

```hive
hive> SHOW TABLES;
OK
crew
episodes
episodes_tmp
movies
movies_details
movies_details_tmp
movies_tmp
persons
persons_tmp
principals
ratings
ratings_tmp
Time taken: 0.078 seconds, Fetched: 12 row(s)
hive>

```


- Query giving all field of movies (`title.akas.tsv`) and movies details (`title.basics.tsv`)


```hive
hive> SELECT * FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId LIMIT 10;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127164239_9f5ccf59-da0b-40db-bd63-44b1bc673d64
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 9
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:42:41,690 Stage-1 map = 0%,  reduce = 0%
2022-01-27 16:42:47,692 Stage-1 map = 1%,  reduce = 0%
2022-01-27 16:42:53,697 Stage-1 map = 3%,  reduce = 0%
2022-01-27 16:42:56,699 Stage-1 map = 5%,  reduce = 0%
2022-01-27 16:43:04,707 Stage-1 map = 8%,  reduce = 0%
2022-01-27 16:43:07,711 Stage-1 map = 10%,  reduce = 0%
2022-01-27 16:43:09,680 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:43:15,686 Stage-1 map = 12%,  reduce = 0%
2022-01-27 16:43:21,695 Stage-1 map = 15%,  reduce = 0%
2022-01-27 16:43:24,698 Stage-1 map = 16%,  reduce = 0%
2022-01-27 16:43:33,708 Stage-1 map = 19%,  reduce = 0%
2022-01-27 16:43:36,711 Stage-1 map = 21%,  reduce = 0%
2022-01-27 16:43:37,714 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:43:43,687 Stage-1 map = 22%,  reduce = 0%
2022-01-27 16:43:49,693 Stage-1 map = 26%,  reduce = 0%
2022-01-27 16:44:01,706 Stage-1 map = 30%,  reduce = 0%
2022-01-27 16:44:04,709 Stage-1 map = 32%,  reduce = 0%
2022-01-27 16:44:06,711 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:44:12,694 Stage-1 map = 33%,  reduce = 0%
2022-01-27 16:44:15,698 Stage-1 map = 37%,  reduce = 0%
2022-01-27 16:44:24,709 Stage-1 map = 41%,  reduce = 0%
2022-01-27 16:44:27,713 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:44:33,722 Stage-1 map = 48%,  reduce = 0%
2022-01-27 16:44:42,700 Stage-1 map = 53%,  reduce = 0%
2022-01-27 16:44:43,702 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:44:49,710 Stage-1 map = 59%,  reduce = 0%
2022-01-27 16:45:01,724 Stage-1 map = 62%,  reduce = 0%
2022-01-27 16:45:04,727 Stage-1 map = 64%,  reduce = 0%
2022-01-27 16:45:06,730 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:45:12,702 Stage-1 map = 71%,  reduce = 0%
2022-01-27 16:45:21,711 Stage-1 map = 76%,  reduce = 0%
2022-01-27 16:45:22,776 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:45:28,783 Stage-1 map = 83%,  reduce = 0%
2022-01-27 16:45:31,788 Stage-1 map = 85%,  reduce = 0%
2022-01-27 16:45:33,791 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:45:35,793 Stage-1 map = 89%,  reduce = 0%
2022-01-27 16:45:36,794 Stage-1 map = 100%,  reduce = 0%
2022-01-27 16:45:41,821 Stage-1 map = 100%,  reduce = 11%
2022-01-27 16:45:45,827 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:45:46,829 Stage-1 map = 100%,  reduce = 22%
2022-01-27 16:45:49,834 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:45:50,836 Stage-1 map = 100%,  reduce = 33%
2022-01-27 16:45:54,842 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:45:55,844 Stage-1 map = 100%,  reduce = 44%
2022-01-27 16:45:59,853 Stage-1 map = 100%,  reduce = 56%
2022-01-27 16:46:03,859 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:46:04,862 Stage-1 map = 100%,  reduce = 67%
2022-01-27 16:46:08,829 Stage-1 map = 100%,  reduce = 78%
2022-01-27 16:46:12,953 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:46:13,955 Stage-1 map = 100%,  reduce = 89%
2022-01-27 16:46:17,966 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local73517776_0010
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 197748350021 HDFS Write: 53377985790 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
tt0000008	2	フレッド・オット－のくしゃみ	JP	ja	imdbDisplay	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	8	Чхання Фреда Отта	UA	NULL	imdbDisplay	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	7	Чих, записанный на кинетоскоп Эдисона	RU	NULL	imdbDisplay	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	10	Edison kinetoskopische Aufnahme eines Niesens	DE	NULL	NULL	literal title	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	1	Edison asszisztense tüsszent	HU	NULL	imdbDisplay	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	3	Edison Kinetoscopic Record of a Sneeze, January 7, 1894	US	NULL	NULL	complete title	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	4	Edison Kinetoscopic Record of a Sneeze	US	NULL	imdbDisplay	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	5	Edison Kinetoscopic Record of a Sneeze	NULL	NULL	original	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	6	Fred Otts Niesen	DE	NULL	NULL	literal title	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
tt0000008	9	Fred Ott's Sneeze	NULL	NULL	NULL	NULL	NULL	tt0000008	short	Edison Kinetoscopic Record of a Sneeze	Edison Kinetoscopic Record of a Sneeze	NULL	1894	NULL	1	NULL	NULL
Time taken: 218.001 seconds, Fetched: 10 row(s)
hive>

```

- Same query then previous but with projections

```
set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId LIMIT 3;

hive> set hive.cli.print.header=true;
hive> SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear
    > FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId LIMIT 3;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127165135_ed6fecf1-44f0-44b1-98e0-b08727a684cf
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 9
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 16:51:36,605 Stage-1 map = 0%,  reduce = 0%
2022-01-27 16:51:41,574 Stage-1 map = 1%,  reduce = 0%
2022-01-27 16:51:44,576 Stage-1 map = 3%,  reduce = 0%
...
2022-01-27 16:54:10,583 Stage-1 map = 100%,  reduce = 22%
2022-01-27 16:54:14,589 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:54:15,591 Stage-1 map = 100%,  reduce = 33%
2022-01-27 16:54:18,597 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:54:19,598 Stage-1 map = 100%,  reduce = 44%
2022-01-27 16:54:22,632 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:54:23,634 Stage-1 map = 100%,  reduce = 56%
2022-01-27 16:54:26,640 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:54:27,641 Stage-1 map = 100%,  reduce = 67%
2022-01-27 16:54:31,647 Stage-1 map = 100%,  reduce = 78%
2022-01-27 16:54:35,695 Stage-1 map = 100%,  reduce = 100%
2022-01-27 16:54:36,697 Stage-1 map = 100%,  reduce = 89%
2022-01-27 16:54:39,668 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local211627547_0011
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 238497168953 HDFS Write: 53377985790 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
m.titleid	m.title	m.region	m.language	md.titletype	md.primarytitle	md.language	md.startyear	md.endyear
tt0000008	フレッド・オット－のくしゃみ	JP	ja	short	Edison Kinetoscopic Record of a Sneeze	1894	NULL	1
tt0000008	Edison kinetoskopische Aufnahme eines Niesens	DE	NULL	short	Edison Kinetoscopic Record of a Sneeze	1894	NULL	1
tt0000008	Чих, записанный на кинетоскоп Эдисона	RU	NULL	short	Edison Kinetoscopic Record of a Sneeze	1894	NULL	1
Time taken: 185.666 seconds, Fetched: 3 row(s)
hive>
```

- details on movies

```hive
set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear,
c.directors, c.writers, r.averageRating, r.numVotes
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN crew c ON c.titleId = md.titleId
INNER JOIN ratings r ON r.titleId = md.titleId

LIMIT 3;

hive> set hive.cli.print.header=true;
hive> SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear,
    > c.directors, c.writers, r.averageRating, r.numVotes
    > FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId
    > INNER JOIN crew c ON c.titleId = md.titleId
    > INNER JOIN ratings r ON r.titleId = md.titleId
    >
    > LIMIT 3;
No Stats for imdb@movies, Columns: titleid, language, title, region
No Stats for imdb@movies_details, Columns: titletype, titleid, startyear, language, endyear, primarytitle
No Stats for imdb@crew, Columns: titleid, directors, writers
No Stats for imdb@ratings, Columns: averagerating, titleid, numvotes
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127170147_17d916ac-8aac-4912-9ca4-9f40c4a53b23
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks not specified. Estimated from input data size: 11
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
...
2022-01-27 17:06:02,758 Stage-1 map = 100%,  reduce = 89%
2022-01-27 17:06:03,760 Stage-1 map = 100%,  reduce = 91%
2022-01-27 17:06:07,733 Stage-1 map = 100%,  reduce = 100%
Ended Job = job_local1495599393_0012
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 363318369011 HDFS Write: 68205204065 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
m.titleid	m.title	m.region	m.language	md.titletype	md.primarytitle	md.language	md.startyear	md.endyear	c.directors	c.writers	r.averagerating	r.numvotes
tt0000002	Клоун и его собаки	RU	NULL	short	Le clown et ses chiens	1892	NULL	5	nm0721526	NULL	6.0	241
tt0000002	Clovnul si cainii sai	RO	NULL	short	Le clown et ses chiens	1892	NULL	5	nm0721526	NULL	6.0	241
tt0000002	Der Clown und seine Hunde	DE	NULL	short	Le clown et ses chiens	1892	NULL	5	nm0721526	NULL	6.0	241
Time taken: 261.368 seconds, Fetched: 3 row(s)
hive>
```

- Crew And Movies

For this part, we observe that the writers and directors are defined as a list in the `crew` table, one solution that I found (I don't know if it's the better) is
to create two new tables by flatten the two columns as below, which allows me to join into `persons` table



I wasn't able to find another solution to be able to joi

```
CREATE TABLE writers
(
    titleId STRING,
    writerId STRING

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

CREATE TABLE directors
(
    titleId STRING,
    directorId STRING

)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


INSERT INTO TABLE writers
SELECT titleId, v.writerId
FROM crew c
lateral view explode(split(c.writers, ',')) v as writerId;


INSERT INTO TABLE directors
SELECT titleId, v.directorId
FROM crew c
lateral view explode(split(c.directors, ',')) v as directorId;

```


```
hive> CREATE TABLE writers
    > (
    >     titleId STRING,
    >     writerId STRING
    >
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.19 seconds
hive>
    > CREATE TABLE directors
    > (
    >     titleId STRING,
    >     directorId STRING
    >
    > )
    > ROW FORMAT DELIMITED
    > FIELDS TERMINATED BY "\t"
    > LINES TERMINATED BY "\n"
    > STORED AS TEXTFILE;
OK
Time taken: 0.084 seconds
hive> INSERT INTO TABLE writers
    > SELECT titleId, v.writerId
    > FROM crew c
    > lateral view explode(split(c.writers, ',')) v as writerId;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127175545_9792d2a9-6c89-41a6-9bdc-024942fd0a70
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2022-01-27 17:55:47,518 Stage-1 map = 0%,  reduce = 0%
2022-01-27 17:56:31,508 Stage-1 map = 25%,  reduce = 0%
2022-01-27 17:57:12,466 Stage-1 map = 100%,  reduce = 0%
Ended Job = job_local380154532_0016
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://namenode:8020/user/hive/warehouse/imdb.db/writers/.hive-staging_hive_2022-01-27_17-55-45_909_7192408020119550831-1/-ext-10000
Loading data to table imdb.writers
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 39092693856 HDFS Write: 6801113860 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
titleid	v.writerid
Time taken: 91.261 seconds
hive> INSERT INTO TABLE directors
    > SELECT titleId, v.directorId
    > FROM crew c
    > lateral view explode(split(c.directors, ',')) v as directorId;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127175840_b79de295-ec60-42f5-ab87-ade14bf3facb
Total jobs = 3
Launching Job 1 out of 3
Number of reduce tasks is set to 0 since there's no reduce operator
Job running in-process (local Hadoop)
2022-01-27 17:58:42,165 Stage-1 map = 0%,  reduce = 0%
2022-01-27 17:59:11,143 Stage-1 map = 25%,  reduce = 0%
2022-01-27 17:59:38,122 Stage-1 map = 100%,  reduce = 0%
Ended Job = job_local2071632951_0017
Stage-4 is selected by condition resolver.
Stage-3 is filtered out by condition resolver.
Stage-5 is filtered out by condition resolver.
Moving data to directory hdfs://namenode:8020/user/hive/warehouse/imdb.db/directors/.hive-staging_hive_2022-01-27_17-58-40_680_6963433886087530232-1/-ext-10000
Loading data to table imdb.directors
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 39652824582 HDFS Write: 7069019000 SUCCESS
Total MapReduce CPU Time Spent: 0 msec
OK
titleid	v.directorid
Time taken: 60.931 seconds
hive>

```

Now we can use the query to get the writers names


```
set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear, p.primaryName as writers
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN writers w ON w.titleId = md.titleId
INNER JOIN persons p ON p.tconst = w.writerId 
LIMIT 3;


set hive.cli.print.header=true;
SELECT m.titleId, m.title, collect_set(p.primaryName) as writers
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN writers w ON w.titleId = md.titleId
INNER JOIN persons p ON p.tconst = w.writerId 
GROUP BY m.titleId, m.title
LIMIT 3;

```









---

```



set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear,
c.directors, c.writers, r.averageRating, r.numVotes
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN crew c ON c.titleId = md.titleId
INNER JOIN persons p ON p.tconst IN 
INNER JOIN ratings r ON r.titleId = md.titleId

LIMIT 3;


SELECT * FROM crew c INNER JOIN persons p ON p.tconst IN c.directors LIMIT 2;


SELECT titleId, new_Subs_clmn
FROM crew c view explode(split(c.directors,',')) c.directors AS directors LIMIT 3;



SELECT explode(split(c.writers, ',')) FROM crew c LIMIT 5;


SELECT c. FROM crew c INNER JOIN persons p ON p.tconst IN c.directors LIMIT 2;



set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN crew c ON c.titleId = m.titleId
LIMIT 3;


set hive.cli.print.header=true;
SELECT m.titleId, m.title, m.region, m.language, md.titleType, md.primaryTitle, md.language, md.startYear, md.endYear, 
explode(split(c.writers, ',')) as writers
FROM movies m INNER JOIN movies_details md ON m.titleId = md.titleId 
INNER JOIN crew c ON c.titleId = m.titleId
INNER JOIN persons p ON p.tconst = writers
LIMIT 3;



INNER JOIN (SELECT c.titleId, explode(split(c.writers, ',')) as writers FROM crew c) AS cr ON cr.titleId = m.titleId;

INNER JOIN crew c ON c.titleId = md.titleId
INNER JOIN persons p ON p.tconst IN 

```