package com.hpi.msd;


import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.HostInfo;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.time.Instant;
import java.util.*;
import java.util.function.Function;


/**
 * Created by nicolashoeck on 05.08.19.
 */


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
        ServletContextHandler context =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
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
        //jettyServer.join();

    }

    public void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    private final Client client =
            ClientBuilder.newBuilder().register(JacksonFeature.class).build();


    @GET
    @Path("/{record}")
    @Produces(MediaType.APPLICATION_JSON)
    public String valueByKey(
            @PathParam("record") final String record,
            @Context UriInfo uriInfo
    ) {
        String attributes = StringUtils.substringBetween(record, "{", "}");
        ArrayList<String> attribute_list = new ArrayList<>();              //split the string to creat key-value pairs
        attribute_list.addAll(Arrays.asList(attributes.replace(" ","").split(",")));


        // Get metadata for the instances of this Kafka Streams application hosting the store and
        // potentially the value for key
     //   final StreamsMetadata metadata =
       //         streams.metadataForKey(this.storeName, key, Serdes.String().serializer());
        //if (metadata == null) {
          //  throw new NotFoundException();
        //}

      //  if (metadata.hostInfo() != this.hostInfo) {
     //       return fetchValueFromRemoteHost(metadata.hostInfo(), uriInfo.getPath());
     //   }

        // Get KeyValue Store --> Tree
        final ReadOnlyKeyValueStore<String, ListMultimap> store =
                streams.store(this.storeName, QueryableStoreTypes.keyValueStore());
        if (store == null) {
            throw new NotFoundException();
        }

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
