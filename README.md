# ISA-Spark-Coviews
Spark Coviews

Instructions:

In a terminal:

docker-compose up

In a different terminal:

docker exec -it spark-master bin/spark-submit --master spark://spark-master:7077 --total-executor-cores 2 --executor-memory 512m /tmp/data/hello.py
