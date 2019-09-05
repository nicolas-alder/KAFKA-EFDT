# Extremely Fast Decision Trees 
# - An Apache Kafka Implementation -

## Build & Run Project
0. Clone repository and download Apache Kafka and Zookeeper as described here: https://kafka.apache.org/quickstart
1. bin/zookeeper-server-start.sh config/zookeeper.properties
2. bin/kafka-server-start.sh config/server.properties --override delete.topic.enable=true 
3. bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic aggregatedinput
4. mvn package
5. java -jar target/EFDT-1.0-SNAPSHOT-jar-with-dependencies.jar "\<path to dataset\>"
## Motivation 

## Related Work

## Approach/Implementation
- Architecture
- Problems
-> Parallelization
-> Scaling...

### Architecture and Components
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/architecture.png" width="400"/>

The architecture of this implementation consists of five main components: The input topic, the tree app, the local state store, and the query app. Those components are capsulated within a rest API layer. We further explain those components in the following.

#### The input topic
Apache Kafka stores data in so-called topics.  They serve to ensure elasticity, scalability, high performance, and fault tolerance (cite 7). At this point, it is not important to fully deep-dive into the concept of the topics but to know they are the main abstraction used for storing data within Kafka. All data that is fed into the Extremely Fast Decision Tree is, therefore, send to the input topic at the very beginning via a provided rest API endpoint. The input topic is created via the command line (see Build & Run Project). Internally its referenced as "aggregatedinput". The name can be changed by changing the command together with the topology definition in the "Treeworker" class.

#### The tree application
Apache Kafka organizes data streams in so-called topologies (cite 7). The topology concept helps to denote the computational logic behind the transformation of an input data stream to any output (data stream). They are represented as a graph structure. This topology graph may contain source-, stream- or sink-processor nodes. While stream nodes receive their input from other nodes and send their output over to other nodes, source nodes receive their input from topics as well as sink nodes send their output to topics. We use the low-level processor API (cite 8) of Apache Kafka to define our topology. Our graph consists only of one processor node, our tree application node, and is, therefore, source- and sink-node at the same time. Each input record that is stored in the input topic is read, one record at a time, into the tree application processor node. All computational logic to operate the tree, described in "The Algorithm" is implemented in the tree application processor node. The corresponding java class is "TreeworkerProcessor". The topology with the embedded processor node is "Treeworker".

#### The Local State Store
By default, data streams are processed in a stateless way within a Kafka topology. This means that any input data is processed in a way that is independent of any former input. As we obviously must learn from former input and maintain a decision tree structure, we are performing stateful operations and must store the tree structure. This is done with the help of a local state store. A local state store is organized as a key-value store (think of a dictionary structure) and bound to our specific processor node. Anything that must be saved to represent the decision tree, that is learned from the data or saved within the tree must be stored in the local state store. Below, you find a short overview of how we organized the key-value store.

<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/kv-store.png" width="700"/>

Currently we make use of a persistent local state store. The settings of the state store (e.g. using it in-memory) can be easily changed in the "Treeworker" class.
#### REST API LAYER
Concrete description of how input data must be structured.
The processing of the records is designed in a way that it is possible to feed 



## Goals/Contributions/Research Question

## References

