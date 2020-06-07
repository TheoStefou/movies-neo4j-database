# How to run


Download the dataset from here: https://www.kaggle.com/rounakbanik/the-movies-dataset <br>
Run parse_csv_files.py to create the intermediate files. <br>

```shell
$ vagrant up
$ vagrant ssh
```
Load the files inside the docker container, go to the browser endpoint and run the initialization queries that can be found in the queries.txt file. <br>
<br>
```shell
$ export FLASK_APP=neo4jAPI.py
$ flask run
```

You can find curl examples in the curl_examples.txt file. <br>
