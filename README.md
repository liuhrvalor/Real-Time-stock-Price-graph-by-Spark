
#dependency
pip install -r requirements.txt

#stop and remove existing docker container
docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

#host ip(your local ip if you run docker on your computer)
$hostIP = your host ip

#start zookeeper container

sudo docker run -d -p 2181:2181 -p 2888:2888 -p 3888:3888 --name zookeeper confluent/zookeeper

#start kalka container 
sudo docker run -d -p 9092:9092 -e KAFKA_ADVERTISED_HOST_NAME=$hostIP \
-e KAFKA_ADVERTISED_PORT=9092 --name kafka --link zookeeper:zookeeper confluent/kafka


#start redis container
 sudo docker run -d -p 6379:6379 --name redis redis:alpine


#set configure for kafka

export ENV_CONFIG_FILE='pwd'/kafka/config/dev.cfg

#start kafk code

python data_get.py


#start spark code
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.2.jar stream.py stock-analyzer average-stock-price $hostIP:9092


#start redis code 
python redis-publisher.py average-stock-price $hostIP:9092 average-stock-price $hostIP 6379


#nodejs code
node index.js --port=3000 --redis_host=192.168.1.108 --redis_port=6379 --subscribe_topic=average-stock-price



