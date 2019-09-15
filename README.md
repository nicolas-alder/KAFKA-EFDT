# Extremely Fast Decision Tree 
# - An Apache Kafka Implementation -

## Abstract

In 2018 Manapragada et al. [1] published the paper "Extremely Fast Decision Tree", which is an extension of the previous state-of-the-art solution, the Hoeffding Tree. With this solution it was possible to achieve a much higher accuracy with only small additional computational costs and also to include the concept drift. For their implementation they used the existing solution of the MOA framework and added their extensions.  
We implemented a full working prototype based on that Paper on Apache Kafka [3] and elaborated on the algorithm and the theoretical prerequesites. An architectural representation based on the concepts of Apache Kafka was found and illustrated. 
Furthermore we evaluated the performance of the EFDT algorithm of our implementation and provided a pipeline for (pre)processing and evaluating arbitrary datasets for the prototype. A adapting live-visualization of the decision tree enables manual tracking and exploration of the tree structure development within the running data stream.
The limitations and possibilities for scaling the algorithm as well as the corresponding possible approaches to implement them on Apache Kafka were discussed in the end of the paper and can be addressed in future works.

## Build & Run Project 
0. Clone this repository and download Apache Kafka and Zookeeper as described here: https://kafka.apache.org/quickstart
1. Change to Kafka directory with terminal.
2. bin/zookeeper-server-start.sh config/zookeeper.properties
3. bin/kafka-server-start.sh config/server.properties --override delete.topic.enable=true 
4. bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic input
5. Change to repository directory
6. mvn package
7. java -jar target/EFDT-1.0-SNAPSHOT-jar-with-dependencies.jar "\<path to dataset\>" "\<security threshold (e.g. 0.95)\>"

## Technical FAQ
#### A second/multiple tree-visualization window(s) randomly appear(s). 

Multiple visualization windows may appear if the kafka broker or another kafka component fails/dies and the processor node, i.e. the whole prototype, is restored. The init() method of the processor node in "TreeworkerProcessor" is triggered, resulting in opening another visualization window. REST requests are not possible in the time of restoration. We have not evaluated the correctness of the restored StateStore and the corresponding tree and recommend a restart of the prototype.

#### I receive an error that my port is already in use

Open the command line and type "lsof -i :7070". Subsequently kill the responsible process with its PID and try to restart the prototype again. This is at your own risk.

#### How do I delete a topic and its contents?

Please run the script "kafka-del-topics.sh" that is provided in the root directory of this repository from within the kafka distribution "bin" directory. 

#### How can I contact you?

You can contact us via nicolas.alder@student.hpi.de or henrik.wenck@student.hpi.de .

## Motivation
In the age of big data, we find ourselves in a situation where more and more data is being produced. The sequential procedure of a traditional data pipeline - the collection of a data set, its preparation, and subsequent analysis - is increasingly no longer sufficient to meet the requirements placed on live data. Today, data that is collected in real-time. Therefore, it must be analyzed in real-time to make immediate decisions. Data streams can also grow to such sizes that storage is no longer economical or possible and therefore on-the-fly analysis is a sensible way to make use of it. Applications can be found in industrial production, for example, to predict failures due to sensor data on production machines. But there are also numerous applications for fraud detection in financial transactions and all other areas that produce large amounts of real-time data.
An example of a decision tree algorithm is the "Extremely Fast Decision Tree Algorithm" for streaming data by Manapragada et al. [1, 2].

In addition to the algorithms, we need a powerful framework for streaming data. The choice for this work is Apache Kafka. Apache Kafka is an open-source streaming platform, which is built according to a publish/subscribe principle and can process data streams fault-tolerantly and scalably at the time of their appearance [3]. A good introduction to the concepts of Kafka can be found in "Kafka - The Definitive Guide" that is freely provided by Confluent [4]. 

Our contribution is the implemented prototype of the "Extremely Fast Decision Tree" by Manapragada et al. [1] with Apache Kafka [3] as streaming framework. In this ReadMe, we aim for an overview of our implementation and its evaluation, the algorithms rough theoretical background, concepts and challenges we faced implementing this algorithm on Apache Kafka.

## Related Work
The theoretical background of this work is building in particular on three papers.
The theoretical basis of this implementation is the algorithm "Extremely Fast Decision Tree" (EFDT) by Manapragada et al.[1]. This is an extension of the "Very Fast Decision Tree" (VFDT) by Domingos and Hulten [5]. An important concept for both algorithms is Hoeffding inequality [6]. This work focuses on the implementation of the "Extremely Fast Decision Tree" on Apache Kafka, therefore we dispense with a further listing of alternative decision tree algorithms on streaming data. The interested reader will find a summary of the most popular algorithms in the paper by Rosset [7]. The VFDT is also implemented in the Massive Online Analysis (MOA) Framework [8], a framework that provides machine learning algorithms for data stream mining, especially for concept drift. We will come back to the concept of concept drift succeeding.
The EFDT can be found as an implemented extension for the MOA framework, based on the VDFT implementation on the Github page of Manapragada et al. [2]. Our paper analysis has shown that none of these algorithms has been implemented on Apache Kafka so far. Generally, we did not find any information about direct implementations of decision trees on Apache Kafka without embedding external frameworks. Therefore our work is to be understood as an attempt to implement the "Extremely Fast Decision Tree" algorithm with Apache Kafka, on the one hand, and as an evaluation of how tree structures can be reasonably mapped to Apache Kafka, on the other hand.

## The Approach

### Operations

The algorithm presented by Manapragada et al. [1] uses the individual records of the stream data to iteratively create a tree structure of nodes and edges, which finally allows a classification of this data. A record consists of a set of attributes, which have different attribute values. The following section gives an overview of the four basic components of the algorithm. The algorithm can be found in the implementation in TreeworkerProcessor and EFDT_InfoGain.

### Initialization 

At the beginning there are two steps. First, a node is initialized, which simultaneously acts as root of the tree and second, a count statistic is created based on the possible attribute-attribute value combinations with 0 as initial value.

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Initialize.jpg" width="300"/>
</p>

##### The following steps are done iteratively for each record.

### Insertion

The record is inserted into the existing tree structure and the count statistics are updated on the way to the leaf.

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Insert.jpg" width="700"/>
</p>

### AttemptToSplit

Once it has arrived at the leaf, the system checks whether there is a pure division. Is the case, the leaf is labeled with the most frequently occurring label. If there is no pure division, an InformationGain value is calculated based on the count statistics for each attribute. This value indicates how much the respective attribute would reduce the existing impurity and is calculated using the following formula: 

<a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;IG(node,Attribute)=H(node)-H(node&space;\mid&space;Attribute)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;IG(node,Attribute)=H(node)-H(node&space;\mid&space;Attribute)" title="IG(node,Attribute)=H(node)-H(node \mid Attribute)" /></a> with <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;H(node)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;H(node)" title="H(node)" /></a> as the entropy of the node and <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;H(node&space;\mid&space;Attribute)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;H(node&space;\mid&space;Attribute)" title="H(node \mid Attribute)" /></a> as the entropy in case one would split at that attribute. The calculations can be found in EFDT_InfoGain.

The attribute with the highest value, <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;G(X_A)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;G(X_A)" title="G(X_A)" /></a> and the value of the nullsplit <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;G(X_0)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;G(X_0)" title="G(X_0)" /></a> are selected and a weighted average with the previously calculated values is determined <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\rightarrow&space;\overline{G}(X_A),&space;\overline{G}(X_0)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\rightarrow&space;\overline{G}(X_A),&space;\overline{G}(X_0)" title="\rightarrow \overline{G}(X_A), \overline{G}(X_0)" /></a>. 

Afterwards the Hoeffding criterion has to be checked, where we look if <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\overline{G}(X_A)&space;-&space;\overline{G}(X_0)>\varepsilon" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\overline{G}(X_A)&space;-&space;\overline{G}(X_0)>\varepsilon" title="\overline{G}(X_A) - \overline{G}(X_0)>\varepsilon" /></a>. Here <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\varepsilon" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\varepsilon" title="\varepsilon" /></a> describes a safety threshold, which in turn is described in the following formula. 

<a href="https://www.codecogs.com/eqnedit.php?latex=\varepsilon=\sqrt{\frac{R^2&space;\ln(1/\delta)}{2n}}" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\varepsilon=\sqrt{\frac{R^2&space;\ln(1/\delta)}{2n}}" title="\varepsilon=\sqrt{\frac{R^2 \ln(1/\delta)}{2n}}" /></a>

Included are the number of records <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;n" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;n" title="n" /></a> that have arrived in the node so far and a user-defined security <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\delta" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\delta" title="\delta" /></a> that specifies the probability to which <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;X_A" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;X_A" title="X_A" /></a> truly is the optimal splitting attribute. The definition of the <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;R^2" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;R^2" title="R^2" /></a>, however, was not clearly described in the original paper, which is why <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;R" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;R" title="R" /></a> was set after careful consideration to the number of classes to be predicted, in our case 2.

If the Hoeffding criterion and <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;X_A&space;\neq&space;X_0" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;X_A&space;\neq&space;X_0" title="X_A \neq X_0" /></a> are fulfilled, the leaf gets the attribute as a label and gets leafs assigned by the number of the respective attribute values. A new count statistic is also initialized in each of these leafs.

### ReEvaluate

As previously described, there can also be changes within characteristics in a data stream, such as a new behavior when measuring click behavior.  This phenomenon is called "Concept Drift" and the ReEvaluate function enables exactly this flexibility. 

It does this in a very similar way to the AttemptToSplit function but with one decisive difference: It checks the Hoeffding criterion not in the leaf but on the way to the leaf. Thus, <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;G(X_A)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;G(X_A)" title="G(X_A)" /></a> or <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\overline{G}(X_A)" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\overline{G}(X_A)" title="\overline{G}(X_A)" /></a> is calculated on the basis of the stored count statistics and compared with the currently dividing attribute <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;G(X_{current})" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;G(X_{current})" title="G(X_{current})" /></a> or <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\overline{G}(X_{current})" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\overline{G}(X_{current})" title="\overline{G}(X_{current})" /></a> within the Hoeffding criterion (below). 

<a href="https://www.codecogs.com/eqnedit.php?latex=\overline{G}(X_A)&space;-&space;\overline{G}(X_{current})&space;>&space;\varepsilon" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\overline{G}(X_A)&space;-&space;\overline{G}(X_{current})&space;>&space;\varepsilon" title="\overline{G}(X_A) - \overline{G}(X_{current}) > \varepsilon" /></a>


If this criterion is true, there are two possibilities. First, <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;X_A" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;X_A" title="X_A" /></a> could be the nullsplit which leads to the so called subtreekill where the node becomes a reinitialized leaf and its child nodes are deleted. In the other case, the child nodes are deleted again, but the current node is overwritten with the new best attribute instead of the leaf.

### Summarizing representation

Finally, the algorithm will be summarized and illustrated with the help of a graphic. It shows an incoming record, which is inserted into the tree structure. At each passed node the evaluation is performed or an attempt is made to split in the leaf. If the Hoeffding criterion is fulfilled in the reevaluation or in the split attempt, the node or leaf is then replaced and new child nodes are initialized.

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Summary.png" width="600"/>
</p>

## Further explanations of the concept drift

Since the capture of the concept drift has a fundamental part in the algorithm, it will be explained in the following section.  Basically, the concept drift can only be found in stream data mining, because unlike batch data mining, the total amount of data is not available at any time. Therefore, one must conclude that the records observed do not have to be compellingly representative for the records arriving in the future.

In the Extremly Fast Decision Tree algorithm, these changes are made in the reevaluation. The epsilon has a very important role as clarified from the following diagram.

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Epsilon_Curve.png" />
</p>

In this graph one can see that the higher the safety threshold <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;$\delta" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;$\delta" title="$\delta" /></a> is set, the later the limit of the Hoeffding criterion converges towards 0. This in turn has the consequence that a deviation appears later as a signifier and thus the following reaction is only carried out if one no longer wants to assume any temporary property. 

## Architecture and Components
<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/architecture-with-rest-layer.png" width="400"/>
</p>
The architecture of this implementation consists of five main components: The input topic, the tree app, the local state store, and the query app. Those components are capsulated within a rest API layer. We explain those components in the following.

### The Input Topic
Apache Kafka stores data in so-called topics.  They serve to ensure elasticity, scalability, high performance, and fault tolerance [9]. At this point, it is not important to fully deep-dive into the concept of the topics but to know they are the main abstraction used for storing data within Kafka. All data that is fed into the "Extremely Fast Decision Tree" is send to the input topic via a provided rest API endpoint. To delete the input topic that is created in step 3 of "Build & Run Project", we provided the bash script "kafka-del-topics.sh". Running this skript from the "bin" folder of the kafka distribution deletes all existing topics.

### The Tree Application
Apache Kafka organizes data streams in so-called topologies [9]. The topology concept helps to denote the computational logic behind the transformation of an input data stream to any output (data stream). A topology is represented as a graph structure. The graph may contain source-, stream- or sink-processor nodes. While stream nodes receive their input from other nodes and send their output over to other nodes, source nodes receive their input from topics as well as sink nodes send their output to topics. We use the low-level processor API [10] of Apache Kafka to define our topology. Our graph consists only of one processor node, our tree application node, and is source- and sink-node at the same time. Each input record that is stored in the input topic is read, one record at a time, into the tree application processor node. All computational logic to operate the tree, described in "The Algorithm" is implemented in the tree application processor node. The corresponding java class is "TreeworkerProcessor". The topology with the embedded processor node is "Treeworker".
The implementation comes with an adapting live-visualization of the decision tree (implemented with graphstream library [11]). This enables manual tracking and exploration of the tree structure development within a running data stream.


### The Local State Store
By default, data streams are processed in a stateless way within a Kafka topology. This means that any input data is processed in a way that is independent of any former input. As we obviously learn from former input and maintain a decision tree structure, we must perform stateful operations and store the tree structure. This is done with the help of a local state store. A local state store is organized as a key-value store (think of a dictionary structure) and bound to our specific processor node. Anything that must be saved to represent the decision tree, that is learned from the data or saved within the tree must be stored in the local state store. Below, you find a short overview of how we organized the key-value store.
<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/kv-store.png" width="700"/>
</p>
The structure and contents (e.g. the statistics of the nodes described in "The Algorithm") of the state store are initialized within the init() method of the processor node class "TreeworkerProcessor" when the processor node is started before the first record arrives. Since we initialize the node statistics, as described in "The Algorithm", it is assumed that possible categoric record values must be known in advance. To reduce effort in this prototype, we included the dataset path parameter (see "Build & Run Project"). Our implementation automatically extracts distinct categoric variable values from a given dataset to initialize the root node. The dataset must conform to a specific format and properties. The requirements to the dataset/records are exemplified and described within the evaluation section.
Currently, we make use of a persistent local state store. The settings of the state store (e.g. using it in-memory) can be easily changed in the "Treeworker" class.

### The Query Application
The query application does not necessarily mean one specific implemented application by us. It specifies the possibility of any application to insert into, query from or receive status information of the tree (application). The specific query application that is used in this implementation is the Jupyter Notebook that was used for evaluation. Status information informs if the tree currently processes input records. This information is useful in the evaluation process to determine if the evaluation can already be started or if there are still records that must be processed by the processor node into the tree structure. The insertion, query, and status functionalities are implemented by the REST API Layer.

### REST API Layer
The REST API Layer serves as an interface to insert and query the decision tree, but also to receive status information for enabling a better evaluation process. The REST layer was realized with a Jetty [12] webserver and can be requested via http://localhost:7070/{endpoint}/{record}. All requests are GET-requests. Records are sent as URL parameters. They have to be in the format {attribute1_attributevalue_labelClass, attribute2_attributevalue_labelClass, ... }. You find specific examples for illustration purposes below. We also provided a dataset preprocessing pipeline within our jupyter notebook evaluation that you can use for the jar datapath parameter and to load and evaluate any dataset with this prototype (see "Evaluation" section). The implementation of the REST API Layer can be found in the "Query" class.

| Method         |  Endpoint                    | Return value
| ------------- | -------------------- | ----------------------- |
| `insert`      | messages/insert       | Successful: 1|
| `query`   | messages/query           |  Label value: 0/1; Error: -1|
| `status`   | messages/status        | Busy: 1; Not-Busy: 0|


| Method         |  Example                    | 
| ------------- | --------------------------------------------------------------------------- |
| `insert`      | http://localhost:7070/messages/insert/{wohnzeit_WD4_0, telef_nein_0, beruf_B2_0, moral_M1_0, dhoehe_DH2_0, dalter_A3_0, beszeit_BD4_0, rate_RH2_0, verm_V3_0, gastarb_ja_0, buerge_WS1_0, sparkont_SW1_0, weitkred_RK3_0, dlaufzeit_LZ02_0, bishkred_ARK1_0, pers_U2_0, verw_VZ9_0, wohn_W3_0, famges_FG2_0, laufkont_K1_0}  | 
| `query`   |http://localhost:7070/messages/query/{wohnzeit_WD3, beruf_B3, telef_nein, verw_VZ11, moral_M2, dhoehe_DH2, beszeit_BD5, verm_V3, rate_RH4, weitkred_RK3, label, laufkont_K4, gastarb_ja, buerge_WS1, pers_U2, dalter_A4, sparkont_SW5, bishkred_ARK2, dlaufzeit_LZ02, wohn_W1, famges_FG2}  |  
| `status`   |  http://localhost:7070/messages/status/| 


## Evaluation

The following section will show how we have evaluated our implementation. Two data sets were used for this purpose. 

The first dataset comes from a [major Southern German bank](https://www.wi.hs-wismar.de/~cleve/vorl/dmdaten/daten/kreditscoring.htm "Credit scoring data set")[13], which classified 1000 potential credit borrowers based on 20 categorical characteristics regarding their credit solvency. The first 70% of the persons in the data set with the classification 1, i.e. that the persons were able to repay the loan, are opposed to 30% of unworthy persons. 

The second data set, on the other hand, deals with [Skin Region Segmentation](https://archive.ics.uci.edu/ml/datasets/skin+segmentation)[14] with the aim of distinguishing [Skin and Nonskin](https://users.cecs.anu.edu.au/~adhall/INDICON.pdf)[15]. For this purpose, a total of 245057 uncategorized records are available, each of which contains 3 characteristics (values from the BGR color space). The classification ratio is 80% 0 and 20% 1.

The way to the evaluation as well as the results are accomplished by the Jupyter Notebook "EFDT_Handling" and the Python class `EFDT` and shown in the following chapters.

### Preprocessing

Since our algorithm works on the basis of categorical characteristic values, it is important to bring the data set into the required form in the first step. This is made possible by the `preprocess` function within the `EFDT` class. It transforms the data set into records, which has the already described REST API form (attribute_attributevalue_labelClass or attribute_attributevalue), under the assumption that the independent target variable is in the last column. The following parameters are provided for this.

| Parameter         |  Explanation |
| ------------- | -------------------- | 
| data              | data that needs to be preprocessed |
| categorized       | "True" if data is already categorized, "False" if not|
| bins              | number of equal-width categorizing bins |
| col_names_exist   | "True" if data has column names, "False" if not. In that case sample column names will inserted|
| shuffle           | "True" if data should be shuffled, "False" if not|
| save_categorized_data | "True" if categorized data set should be saved into the format name_categorized to pass it in java jar , "False" if not |

Three lists are returned. First, 90% of the instances become training records to train the model. On the other hand, 10% of the instances become unlabeled test records to predict labels as well as ground_truth values to test the predicted labels for accuracy.


### Core Functions

Once the 3 lists are available the tree structure can be built by the algorithm using `EFDT.train_model` and the train records. The resulting tree structure is illustrated in an extra window by GraphStream. You can also send queries directly in jupyter notebook via `EFDT.predict` to get labels for unlabeled records.

### Evaluation of the performance

Now, that the tree structure has been built, one can start to evaluate the accuracy. For these two functions are available. The total performance can be queried with the EFDT.evaluate_model. This function calculate the accuracy by looking in how many cases the predicted labels match the ground_truth values. The `EFDT.calc_error_curve` function is the second function. It uses the percentage defined by the parameter `percentage_split` to iteratively use this part of the train records to build the tree structure and to evaluate it afterwards. This will result in 1/percenage_split accuracy values at the end. These values can be visualized by the function `EFDT.plot_error_curves` for a clear representation. This is not limited to a list of accuracy values, it can also be added a second list of properties to compare the performance between shuffeling and unshuffeling.

We got the following graph for the bank data by the functions `EFDT.calc_error_curve` and `EFDT.plot_error_curves` with a `percentage_split` of 0.01 and a <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\delta=0.95" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\delta=0.95" title="\delta=0.95" /></a>.

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Bank_Error_Curve.png"/>
</p>

In the plot it can be observed that in the beginning both graphs (shuffled/unshuffled) remain around an accuracy of approximately 70% and an error of 30% respectively. The error rate of the graph of the unmixed data is even slightly lower which could be related to the initialization settings. Further on, the blue graph shows a lower error rate than the orange graph. The difference can be explained by the properties of the data set, because it is ordered as described above. This means that in the unshuffled case the first 70% of the instances carry the label 1 which means that (70% of the 90% of 1000) 630, training instances do not contain a 0 as a label. Thus, only towards the end when the concept drift starts the characteristics are "learned" which leads to the 0 classifications. Since the test data consists in unshuffled case only 0 labels, the algorithm performs worse and worse up to the sighting of the first training 0. In contrast, shuffled data leads to a more heterogeneous learning behavior, which is why short-term changes in the tree structure can be observed more frequently. In this case, the algorithm also learns the prediction of 0, which explains the adjustment in the last 15%. It is noticeable, however, that in neither case was the error threshold significantly below 30%. Two coherent reasons would be possible for this. Firstly, the safety threshold <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\delta" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\delta" title="\delta" /></a> of 0.95 could have been chosen too high because, for example, 865 records are needed to reduce <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\varepsilon" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\varepsilon" title="\varepsilon" /></a> to below 0.1. This in turn could lead to the algorithm not behaving sensitively enough to detect deeper details in the data. Secondly, the number of training records (in this case 900) could be too small. The conclusion remains the same in this case.

In order to estimate the influence of the number of learning records, the skin data is considered. They are about 250 times larger and are ordered with the first 50000 entries bearing the label 1. Using the same parameter values (`percentage_split=0.01`, <a href="https://www.codecogs.com/eqnedit.php?latex=\inline&space;\delta=0.95" target="_blank"><img src="https://latex.codecogs.com/svg.latex?\inline&space;\delta=0.95" title="\delta=0.95" /></a> ) and two different numbers of categories (5 and 25) we received the following plots.

<p align="center">
    <img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Skin_Error_Rate5.png" alt>
    <em>5 Categories</em>
</p>

<p align="center">
    <img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Skin_Error_Rate25.png" alt>
    <em>25 Categories</em>
</p>


It becomes apparent that the graph of the unshuffled data behaves similarly in both plots. Namely, after the first 50000 instances one can look at a concept drift after which the error rate drops to about 20%. However, the graphs that were created from the shuffled data differ. The error rate for 5 categories is significantly lower than for 25 categories. This supports the last two presented arguments derived from the bank data. That is, that a higher number of instances leads to a higher refinement within the data. For 25 categories, the amount of data seemed to be insufficient.

If one compares these graphs with the results of Manapragada et al. [1], it can be seen that their accuracy was not achieved. This could result from the automatic and not "natural" categorization of the data. What can be observed, however, is that at least the peak at 50000 instances, which marks the concept drift, can also be seen in both unshuffled data graphs. 

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/Skin_Reference.png" width="620"/>
</p>

In summary, it can be stated that a shuffling and a higher amount of data instances would have a positive effect on the accuracy and robustness of the algorithm.

## Scaling Considerations and Future Works
As mentioned in Motivation, data streams may grow to such sizes that storage is no longer economical or possible and therefore on-the-fly analysis is a sensible way to make use of them. Therefore, scaling is an important subject to consider.  In this implementation, we do not include any specific scaling mechanisms. Possible scaling approaches and their possible problems are discussed in this section. Those considerations on scaling can be taken up in future works.

If you think of scaling this EFDT implementation, you have to consider two different aspects. First, the possibility of scaling within the algorithm itself. Secondly, the scaling concepts of Apache Kafka must conform and support the concepts within the algorithm. Apache Kafka traditionally scales via replication of processor nodes (that is our tree application) and input topics. 

In our current architecture, we read one record at a time into our tree application component. The performance is therefore bound to the computational processing duration when each record is subsequently inserted into the tree. Possible requests that query/use the current tree and additionally add performance needs are not considered for simplicity reasons. Eventually, as soon as there are arriving more records in our input topic as we can process into our tree application, we face an overload. 

We want to discuss two approaches to avoid this bottleneck: aggregating incoming records into one summarized record and parallelizing within the tree structure.

### Aggregating Incoming Records 
To avoid this bottleneck, one can think of aggregating incoming data records into one record, from the view of the tree application. Topic scaling happens genuinely by Apache Kafka if the incoming record size grows. The timeframe of aggregating records corresponds to the duration the last aggregated record is processed/inserted into the tree structure. Each record contains all the necessary information for the update process of the tree. As the algorithm updates observed attributes and its node statistics from a given timeframe, we process fresh incoming records into one summarized record with all observations. The result is a batch insert operation into the tree.
 
<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/aggregation-app.png" width="550"/>
</p>

While this approach seems promising, we face one critical problem. 
It is exemplified below with two incoming records at their aggregated representation. 

<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/aggregation-app-conflict.png" width="700"/>
</p>

If we aggregate records as shown, we might lose information if a binary target variable (label) was observed with with two different categoric variables of the same attribute. In the example shown above, we cannot reconstruct if "Label_0" or "Label_1" was observed with the "Temperature_Hot" or "Temperature_Normal" attribute. Therefore, the approach of aggregating arbitrarily a given timeframe into one record does not work.  
As a consequence, one may argue that we could aggregate records in such a way that only unambiguous aggregation, without information loss, is created. This could be done by considering aggregating records for each possible tree path from the root to any leaf that exists in our decision tree. Those aggregated records must inevitably share the same attributes and could, therefore, be processed at the same time. Though, information about the paths has to be updated after each insertion and is dependent on the size of the decision tree, again depending on the formatting and statistical properties of the input data. Considering that trees do not grow with the input size of data, but with the complexity of its statistical properties and the number of categoric attributes, the insert operation might not be a necessary bottleneck. In the end, those performance factors have to be evaluated individually for each use case. 

### Parallelizing Tree Insertions

Inserting one (not-summarized) record at a time can lead to an overload of records that have to be inserted into the tree by our tree application.
Apache Kafka traditionally scales via replication of processor nodes that is our tree application. Is replicating our tree application with letting it insert in one global state store a possible solution?

Apache Kafka 
<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/parallelizing-node-processors.png" width="450"/>
</p>

It is not a solution due to a concurrency conflict. Apache Kafka ensures that the memory is consistent even when the memory is requested and updated. But it cannot ensure that multiple tree application nodes do not save back conflicting versions of the global tree store. 
<p align="center">
<img src="https://github.com/NicolasBenjamin/KAFKA-EFDT/blob/master/readme_images/parallelizing-node-processors-conflict.png" width="700"/>
</p>
If tree application A and tree application B both request the global state store at time t, they share a common version of the tree structure. Both update the tree according to the record A and B process, resulting in two inconsistent trees that have to be synchronized. An alternative might be that different tree workers maintain specific parts of the tree exclusively, which requires a new reworked architecture. 
Another alternative is the use of locking mechanisms such as semaphores or monitors that ensure mutual exclusion on nodes or tree parts, while the tree application could be simply replicated. Apache Kafka does not offer genuine techniques for using such locking mechanisms on one global state store. This results into the concurrency problem anew or multiple tree applications being bound into subsequently processing the entire global state store (and therefore do not parallelize). The only possible approach is, therefore, to rework the architecture in splitting the tree structure into multiple state stores that contain tree parts and limiting parallel access with locking mechanisms.

### Future Works Beyond Scaling

Extending the EFDT algorithm to a random forest approach is fairly easy with our implementation. The tree application in the "TreeworkerProcessor" class as well as the orchestration of the processor nodes in the "Treeworker" class has to be modified.

Another possibility to continue with the implementation could be an extension of the algorithm, so that it can process not only categorical but also metric attribute values. In addition, a parameter could be added to limit the tree depth in order to avoid the problem of overfitting.


## References
[1]  Manapragada, Chaitanya, Geoffrey I. Webb, and Mahsa Salehi. "Extremely fast decision tree." Proceedings of the 24th ACM SIGKDD International Conference on Knowledge Discovery & Data Mining. ACM, 2018.

[2]  https://github.com/chaitanya-m/kdd2018

[3]  https://kafka.apache.org/intro

[4]  https://www.confluent.io/wp-content/uploads/confluent-kafka-definitive-guide-complete.pdf

[5]  Domingos, Pedro, and Geoff Hulten. "Mining high-speed data streams." Kdd. Vol. 2. 2000.

[6]  Hoeffding, Wassily. "Probability inequalities for sums of bounded random variables." The Collected Works of Wassily Hoeffding. Springer, 1994.

[7]  Rosset, Corbin. "A Review of Online Decision Tree Learning Algorithms." Technical Report. Johns Hopkins University, Department of Computer Science, 2015.

[8]  https://moa.cms.waikato.ac.nz/

[9]  https://docs.confluent.io/current/streams/concepts.html

[10] https://docs.confluent.io/current/streams/developer-guide/processor-api.html

[11] http://graphstream-project.org/

[12] https://www.eclipse.org/jetty/
 
[13] https://www.wi.hs-wismar.de/~cleve/vorl/dmdaten/daten/kreditscoring.htm

[14] https://archive.ics.uci.edu/ml/datasets/skin+segmentation

[15] https://users.cecs.anu.edu.au/~adhall/INDICON.pdf
