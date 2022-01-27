# Hive Evaluation

In this evaluation, we are going create a Data Warehouse [IMDB](https://www.imdb.com/interfaces/) DataBase. Using Hive.

Please note that in report, I am not including the `TSV` files as they are huge. But I am providing `init.sh` that download the whole datasets and does the right stuff. 

Below some command that will be useful

- `docker-compose up -d`: Once the `init.sh` launched and the `TSV` file retrieved
- `docker exec -it hive-server bash` to connect into the `Hive` server, you will need at least two consoles. the first one is to work with `HDFS` and a second to start hive, just run `hive`
- `hdfs dfs -ls /` to list the root folder of HDFS.

## Environment Setup

I am going to use the `docker compose` Hive, provided during the courses. 

```bash
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env
```

Please that those two files are provided in the given `TAR`.

We need to create local directory called `data` used as a volume in the docker-compose. This directory is present in the provided `TAR` but it's empty. 

## Download TCV File

The dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set. The first line in each file contains headers that describe what is in each column. A ‘\N’ is used to denote that a particular field is missing or null for that title/name. Please, use the `init.sh` to download the whole IMDB files. Just run `./init.sh`

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

- Connect to `hive-server` / `HDFS`

Using `docker exec` to access the running container. This console will be used for `HDFS`

```bash
docker exec -it hive-server bash
root@ba74b599cf13:/opt# pwd
/opt
root@ba74b599cf13:/opt# ls -lrt
total 16
drwxr-xr-x 1 20415 input 4096 Feb  5  2018 hadoop-2.7.4
drwxr-xr-x 1 root  root  4096 Feb  5  2018 hive
root@ba74b599cf13:/opt#
```

- Connect to `hive-server` / `Hive`

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
## Modeling

After an analysis on the files, their size, what kind of optimization we could use: partitions vs bucket. Below the keys points of my modeling:

1. Create a table peer TSV file:
2. I tried to use partitions, but I found that there is a limitation peer Hive Node, as we cannot create more then `100` partitions [Here](https://cwiki.apache.org/confluence/display/Hive/Tutorial). This is limitation is defined through the property `hive.exec.max.dynamic.partitions.pernode=100` you list it using the command `set -v;` in the Hive console.
3. I used Bucket on some table like `movies` (`title.akas.tsv.gz`) on the column `language`, on table `movies_details` (`title.basics.tsv.gz`) on two columns  `startYear` and `endYear`, on table `episodes` (`title.episode.tsv.gz`) on two columns `seasonNumber` and `episodeNumber`, on table `ratings` (`title.ratings.tsv.gz`) on column `averageRating`, on table `persons` (`name.basics.tsv.gz`) on columns `birthYear` and `deathYear`. I used fixed the buckets by analyzing the table, as for each case I create, a temporary table and I made queries, to see what could be the best value.
4. I created a temporary table each time to understand its structure and try to create bucket on the right column.
5. I am using the Tab separator `FIELDS TERMINATED BY "\t"` as we have a `TSV`

In the section below, I am going to provide the scripting for each table.

### Creating D.B Model / HDFS

As first step, I have had to upload the downloaded files into HDFS. As example to upload the `title.akas.tsv` into HDFS is `dfs -copyFromLocal /data/title.akas.tsv /` where `/data/` is mounted volume within docker (`init.sh` is in responsible of the download, unzip and the copy into `data`)

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

#### Create a new Database

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
#### `Movies` table from `title.akas.tsv`

`title.akas.tsv.gz` Contains the following information for titles: I created a `movies` table for it. 

- `titleId (string)` - a tconst, an alphanumeric unique identifier of the title
- `ordering (integer)` – a number to uniquely identify rows for a given titleId
- `title (string)` – the localized title
- `region (string)` - the region for this version of the title
- `language (string)` - the language of the title
- `types (array)` - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
- `attributes (array)` - Additional terms to describe this alternative title, not enumerated
- `isOriginalTitle (boolean)` – 0: not original title; 1: original title

First, I created a `temporary` table

```sql
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

The execution tace

```sql
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

And in HDFS

```bash
root@8cc681711ae0:/opt# hdfs dfs -ls -R /user/hive/warehouse/
drwxrwxr-x   - root supergroup          0 2022-01-27 15:37 /user/hive/warehouse/imdb.db
drwxrwxr-x   - root supergroup          0 2022-01-27 15:37 /user/hive/warehouse/imdb.db/movies_tmp
-rwxrwxr-x   3 root supergroup 1535986357 2022-01-27 15:29 /user/hive/warehouse/imdb.db/movies_tmp/title.akas.tsv
root@8cc681711ae0:/opt#
```

Let try to create buckets for this table using for example `language`, as users could use try to find movies within a given language. Also ti will reduce the number of files. 

Using a simple query to compute a number of distinct languages

```sql
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

We have `106` different languages. We could create a new table by using clusters on the column `language`

```sql
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

The execution below is to creating the table and inserting into it using the `movies_tmp`. Please note that I am using `...` to simplify the trace.

```sql
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

HDFS part is

```bash
root@8cc681711ae0:/opt# hdfs dfs -ls -R /user/hive/warehouse/imdb.db/movies
drwxrwxr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1
drwxr-xr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1/-ext-10001
drwxr-xr-x   - root supergroup          0 2022-01-27 15:50 /user/hive/warehouse/imdb.db/movies/.hive-staging_hive_2022-01-27_15-50-23_455_1824641544142350672-1/_tmp.-ext-10000
root@8cc681711ae0:/opt#
```

#### `Movies Details` table from `title.basics.tsv`

- `titleId (string)` - alphanumeric unique identifier of the title
- `titleType (string)` – the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
- `primaryTitle (string)` – the more popular title / the title used by the filmmakers on promotional materials at the point of release
- `originalTitle (string)` - original title, in the original language
- `isAdult (boolean)` - 0: non-adult title; 1: adult title
- `startYear (YYYY)` – represents the release year of a title. In the case of TV Series, it is the series start year
- `endYear (YYYY)` – TV Series end year. ‘\N’ for all other title types
- `runtimeMinutes` – primary runtime of the title, in minutes
- `genres (string array)` – includes up to three genres associated with the title

Same logic then the `movies` table, I creating a temporary table and then I create a buckets on `(startYear, endYear)`

```sql
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

LOAD DATA INPATH '/title.basics.tsv' INTO TABLE movies_details_tmp;

INSERT INTO TABLE movies_details
SELECT titleId, titleType, primaryTitle, originalTitle, isAdult, language, startYear, endYear, runtimeMinutes, genres
FROM movies_details_tmp;
```

The console trace

```sql

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
.......
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

#### `Crew` table from `title.crew.tsv`

Contains the director and writer information for all the titles in IMDb:

- `titleId (string)` - alphanumeric unique identifier of the title
- `directors (array of nconsts)` - director(s) of the given title
- `writers (array of nconsts)` – writer(s) of the given title

```sql
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

execution trace is
```sql
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

#### `Episodes` table from `title.episode.tsv.gz`

Contains the tv episode information. Fields include:

- `titleId (string)` - alphanumeric identifier of episode
- `parentTconst (string)` - alphanumeric identifier of the parent TV Series
- `seasonNumber (integer)` – season number the episode belongs to
- `episodeNumber (integer)` – episode number of the tconst in the TV series

Here I am creating a temporary table, as I'll add buckets on `seasonNumber` and `episodeNumber`

the queries are

```sql
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
     CLUSTERED BY (seasonNumber, episodeNumber) INTO 250 BUCKETS
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY "\t"
     LINES TERMINATED BY "\n"
     STORED AS TEXTFILE;

INSERT INTO TABLE episodes
SELECT titleId, parentTconst, seasonNumber, episodeNumber
FROM episodes_tmp;
```

And the execution trace is: 

```sql
    >          parentTconst STRING,
    >          seasonNumber INT,
    >          episodeNumber INT
    >
    >      )
    >      CLUSTERED BY (seasonNumber, episodeNumber) INTO 250 BUCKETS
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. AlreadyExistsException(message:Table episodes already exists)
hive> DROP TABLE episodes;
OK
Time taken: 0.137 seconds
hive> !clear;

hive> CREATE TABLE episodes
    >      (
    >          titleId STRING,
    >          parentTconst STRING,
    >          seasonNumber INT,
    >          episodeNumber INT
    >
    >      )
    >      CLUSTERED BY (seasonNumber, episodeNumber) INTO 250 BUCKETS
    >      ROW FORMAT DELIMITED
    >      FIELDS TERMINATED BY "\t"
    >      LINES TERMINATED BY "\n"
    >      STORED AS TEXTFILE;
OK
Time taken: 0.078 seconds
hive> INSERT INTO TABLE episodes
    > SELECT titleId, parentTconst, seasonNumber, episodeNumber
    > FROM episodes_tmp;
WARNING: Hive-on-MR is deprecated in Hive 2 and may not be available in the future versions. Consider using a different execution engine (i.e. spark, tez) or using Hive 1.X releases.
Query ID = root_20220127195155_71202983-5f32-48b2-a4f3-764799ce9298
Total jobs = 1
Launching Job 1 out of 1
Number of reduce tasks determined at compile time: 250
In order to change the average load for a reducer (in bytes):
  set hive.exec.reducers.bytes.per.reducer=<number>
In order to limit the maximum number of reducers:
  set hive.exec.reducers.max=<number>
In order to set a constant number of reducers:
  set mapreduce.job.reduces=<number>
Job running in-process (local Hadoop)
2022-01-27 19:51:57,235 Stage-1 map = 0%,  reduce = 0%
Ended Job = job_local514559823_0038 with errors
Error during job, obtaining debugging information...
FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
MapReduce Jobs Launched:
Stage-Stage-1:  HDFS Read: 0 HDFS Write: 0 FAIL
Total MapReduce CPU Time Spent: 0 msec
hive>
```

As we see above, we got errors on `episodes` insertion. I'll try to investigate this part later. 

### `Principals` table from `title.principals.tsv`

Contains the principal cast/crew for titles: 

- `titleId (string)` - alphanumeric unique identifier of the title
- `ordering (integer)` – a number to uniquely identify rows for a given titleId
- `nconst (string)` - alphanumeric unique identifier of the name/person
- `category (string)` - the category of job that person was in
- `job (string)` - the specific job title if applicable, else '\N'
- `characters (string)` - the name of the character played if applicable, else '\N'

The table creation and the execution trace:

```sql
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

```sql
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

### `Ratings` table from `title.ratings.tsv`

Contains the IMDb rating and votes information for titles

- `tconst (string)` - alphanumeric unique identifier of the title
- `averageRating` – weighted average of all the individual user ratings
- `numVotes` - number of votes the title has received

The queries are

```sql
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

And the execution trace is

```sql
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
....
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

### `Persons` table from `title.basics.tsv`

Contains the following information for names:

- `nconst` (string) - alphanumeric unique identifier of the name/person
- `primaryName` (string)– name by which the person is most often credited
- `birthYear` – in YYYY format
- `deathYear` – in YYYY format if applicable, else '\N'
- `primaryProfession` (array of strings)– the top-3 professions of the person
- `knownForTitles` (array of tconsts) – titles the person is known for

The queries are:

```sql
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
And the execution trace is:

```sql
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
......
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
...
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

Below the list of table of the `imdb` table

```sql
hive> SHOW TABLES;
OK
tab_name
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
hive>
```

- Some Basic Queries (Counting, ...)

```sql
hive> SELECT COUNT(*) FROM movies;
OK
_c0
30802936
Time taken: 0.441 seconds, Fetched: 1 row(s)
hive> SELECT COUNT(*) FROM movies_details;
OK
_c0
8641212

hive> SELECT * FROM crew LIMIT 2;
OK
crew.titleid	crew.directors	crew.writers
tconst	directors	writers
tt0000001	nm0005690	NULL
Time taken: 0.129 seconds, Fetched: 2 row(s)
hive> SELECT * FROM crew LIMIT 4;
OK
crew.titleid	crew.directors	crew.writers
tconst	directors	writers
tt0000001	nm0005690	NULL
tt0000002	nm0721526	NULL
tt0000003	nm0721526	NULL
Time taken: 0.119 seconds, Fetched: 4 row(s)
hive> SELECT * FROM episodes LIMIT 4;
OK
episodes.titleid	episodes.parenttconst	episodes.seasonnumber	episodes.episodenumber
Time taken: 0.133 seconds
hive> SELECT * FROM episodes_tmp LIMIT 4;
OK
episodes_tmp.titleid	episodes_tmp.parenttconst	episodes_tmp.seasonnumber	episodes_tmp.episodenumber
tconst	parentTconst	NULL	NULL
tt0020666	tt15180956	1	2
tt0020829	tt15180956	1	1
tt0021166	tt15180956	1	3
Time taken: 0.111 seconds, Fetched: 4 row(s)
hive> SELECT * FROM ratings LIMIT 4;
OK
ratings.titleid	ratings.averagerating	ratings.numvotes
tt0272080	6.5	9
tt4633122	7.1	27
tt10050970	6.8	55
tt7325822	6.5	10
Time taken: 0.272 seconds, Fetched: 4 row(s)
hive> SELECT * FROM principals LIMIT 4;
OK
principals.titleid	principals.ordering	principals.nconst	principals.category	principals.job	principals.characters
tconst	ordering	nconst	category	job	characters
tt0000001	1	nm1588970	self	NULL	["Self"]
tt0000001	2	nm0005690	director	NULL	NULL
tt0000001	3	nm0374658	cinematographer	director of photography	NULL
Time taken: 0.149 seconds, Fetched: 4 row(s)

```


- Query giving all field of movies (`title.akas.tsv`) and movies details (`title.basics.tsv`)


```sql
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
...
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

```sql
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

```sql
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

For this part, we observe that the writers and directors are defined as a list in the `crew` table, one solution that I found (I don't know if it's the better) is to create two new tables by flatten the two columns as below, which allows me to join into `persons` table. I am using the `lateral view explode` syntax to achieve this purpose.

The queries are:

```sql
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

And the execution trace is:

```sql
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

Now we can use the query to get the writers / directors names (But it fails)

```sql
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

## Others

Until now we mainly discussed the creation of buckets. But we could improve our modeling by using indices on `titleId`. [Hive Indices Documentation](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Create/Drop/AlterIndex) as part of Hive 2 (which is our case)

```bash
root@b0da2869f344:/# hive --version
Hive 2.3.2
Git git://stakiar-MBP.local/Users/stakiar/Desktop/scratch-space/apache-hive -r 857a9fd8ad725a53bd95c1b2d6612f9b1155f44d
Compiled by stakiar on Thu Nov 9 09:11:39 PST 2017
From source with checksum dc38920061a4eb32c4d15ebd5429ac8a

```

Pay attention to the fact that since Hive 3, indices are removed [Here](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Indexing) and [HIVE-18448](https://issues.apache.org/jira/browse/HIVE-18448)

Some sample of indices creation on `persons` table

` set hive.cli.print.header=true;` to enable the header of the table.

```sql
hive> CREATE INDEX tconst_index ON TABLE persons (tconst) AS 'COMPACT' WITH DEFERRED REBUILD;
OK
Time taken: 0.208 seconds

hive> CREATE INDEX primaryName_index ON TABLE persons (primaryName) AS 'COMPACT' WITH DEFERRED REBUILD;
OK
Time taken: 0.123 seconds
hive>

```

```sql
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


```sql
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

```sql
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

```sql
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