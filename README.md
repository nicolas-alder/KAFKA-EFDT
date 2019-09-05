# Extremely Fast Decision Trees 
# - An Apache Kafka Implementation -

## Build & Run Project
1. bin/zookeeper-server-start.sh config/zookeeper.properties
2. bin/kafka-server-start.sh config/server.properties --override delete.topic.enable=true 
3. bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aggregatedinput
4. mvn package
5. java -jar target/EFDT-1.0-SNAPSHOT-jar-with-dependencies.jar <path to dataset>
## Motivation 

## Related Work

## Approach/Implementation
- Architecture
- Problems
-> Parallelization
-> Scaling...

## Goals/Contributions/Research Question

## References

