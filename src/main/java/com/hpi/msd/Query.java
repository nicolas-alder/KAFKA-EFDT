package com.hpi.msd;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.ServerConnector;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import java.util.*;

@Path("messages")
public class Query {

    private final KafkaStreams streams;
    private final String storeName;
    private HostInfo hostInfo;
    private Server jettyServer;

    public Query(
            final KafkaStreams streams,
            final String storeName,
            final String hostName,
            final int port
    ) {
        this.streams = streams;
        this.storeName = storeName;
        this.hostInfo = new HostInfo(hostName, port);
    }

    public void start() throws Exception {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server(this.hostInfo.port());
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        ServletContainer sc = new ServletContainer(rc);
        ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(hostInfo.port());
        jettyServer.addConnector(connector);

        context.start();

        jettyServer.start();
    }

    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    private final Client client =
            ClientBuilder.newBuilder().register(JacksonFeature.class).build();


    @GET
    @Path("/query/{record}")
    @Produces(MediaType.APPLICATION_JSON)
    public String valueByKey(
            @PathParam("record") final String record,
            @Context UriInfo uriInfo
    ) {
        String attributes = StringUtils.substringBetween(record, "{", "}");
        ArrayList<String> attribute_list = new ArrayList<>();              //split the string to create key-value pairs
        attribute_list.addAll(Arrays.asList(attributes.replace(" ","").split(",")));

        // Get KeyValue Store --> Tree
        final ReadOnlyKeyValueStore<String, ListMultimap> store = streams.store(this.storeName, QueryableStoreTypes.keyValueStore());
        if (store == null) { throw new NotFoundException();}

        return Integer.toString(iterateTree(0,store,attribute_list));
    }

    private KeyValueBean fetchValueFromRemoteHost(
            final HostInfo host,
            final String path
    ) {
        return client
                .target(String.format("http://%s:%d/%s", host.host(), host.port(), path))
                .request(MediaType.APPLICATION_JSON_TYPE)
                .get(new GenericType<KeyValueBean>(){});
    }

    @GET
    @Path("/insert/{input}")
    @Produces(MediaType.APPLICATION_JSON)
    public int insertRecord(
            @PathParam("input") final String input,
            @Context UriInfo uriInfo
    ) {
        System.out.println(input);
        String attributes = StringUtils.substringBetween(input, "{", "}").replace(" ","");
        ArrayList<String> attribute_list = new ArrayList<>();              //split the string to create key-value pairs
        attribute_list.addAll(Arrays.asList(attributes.split(",")));

        HashMap<String, Double> insertion = new HashMap<>();
        for (String attribute:attribute_list) {System.out.println(attribute);insertion.put(attribute, 1.0);}
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.hpi.msd.RecordSerializer");
        Producer<String, HashMap> producer = new KafkaProducer<>(props);
        producer.send(new ProducerRecord<String, HashMap>("aggregatedinput", "record_seq", insertion));
        producer.close();

        return 1;
    }


    public int iterateTree(int node, ReadOnlyKeyValueStore tree, ArrayList<String> attributes){

        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));

        HashMap currentNodeChildList =  (HashMap) nodeMap.get("childList").iterator().next();
        boolean hasNoChild = currentNodeChildList.isEmpty();
        String splitattribute = (String) nodeMap.get("splitAttribute").iterator().next();

        // Check if node is leaf
        if(hasNoChild){
            return Integer.valueOf(splitattribute);
        }else{
                HashMap childList = (HashMap) nodeMap.get("childList").iterator().next();
                Iterator allChilds = childList.entrySet().iterator();

                while(allChilds.hasNext()){
                    Map.Entry pair = (Map.Entry)allChilds.next();
                    if(attributes.contains(splitattribute+"_"+pair.getKey())) {
                        return iterateTree((int) pair.getValue(), tree, attributes);
                    }
                 }
        }
        return -1;
    }
}
