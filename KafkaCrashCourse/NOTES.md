>pip3 install confluent-kafka

# (kafka topics cli) search in google for "kafka topics cli" 
to find the command line tool to manage topics in Kafka. 
You can use the following commands to list and describe topics:



# (From producer side)
> docker exec -it kafkacrashcourse-kafka-1 kafka-topics --bootstrap-server localhost:9092 --help   
> docker exec -it sample-python-kafka-kafka8 kafka-topics --bootstrap-server localhost:9092 --help   
> docker exec -it app-kafka8 kafka-topics --bootstrap-server localhost:9092 --help   


# (From consumer side)
> docker exec -it kafkacrashcourse-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning


> docker exec -it kafkacrashcourse-kafka-1  kafka-topics --list --bootstrap-server localhost:9092
> docker exec -it kafkacrashcourse-kafka-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic orders
docker exec -it python-kafka-kafka8 kafka-topics --bootstrap-server localhost:9092 --describe --topic orders


> docker exec -it kafka kafka-topics --topic test --bootstrap-server localhost:9092

> docker exec -it sample-python-kafka-kafka-1 kafka-topics --topic test --bootstrap-server localhost:9092
> 
> 
> docker exec -it kafka kafka-console-producer --topic test --bootstrap-server localhost:9092
> docker exec -it kafka kafka-console-consumer --topic test --bootstrap-server localhost:9092 --from-beginning

> https://gitlab.com/twn-youtube/kafka-crash-course