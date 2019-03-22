package com.gs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.SpaceTypeDescriptor;
import com.gigaspaces.metadata.SpaceTypeDescriptorBuilder;
import com.gigaspaces.metadata.index.SpaceIndexType;
import com.j_spaces.core.client.SQLQuery;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import org.junit.Before;
import org.junit.Test;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.GigaSpaceConfigurer;
import org.openspaces.core.space.SpaceProxyConfigurer;


import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;


public class DateUtilTest {
    @Test
    public void testIt() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {


        GigaSpace gigaSpace = new GigaSpaceConfigurer(new SpaceProxyConfigurer("demo")).gigaSpace();


        SpaceTypeDescriptorBuilder typeDescriptorBuilder = new SpaceTypeDescriptorBuilder("test")
                .idProperty("id", true)
                .routingProperty("id");
        typeDescriptorBuilder.addFixedProperty("airport",
                String.class);
        typeDescriptorBuilder.addFixedProperty("pct",
                Integer.class);

        typeDescriptorBuilder.addFixedProperty("asof",
                LocalDateTime.class);
        // Register type:
        SpaceTypeDescriptor typeDescriptor = typeDescriptorBuilder.create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);
        System.out.println(gigaSpace.getTypeManager().getTypeDescriptor("test").getFixedProperty("asof").getType().getCanonicalName());


        final ObjectMapper mapper = new ObjectMapper();
        final AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        rootNodeRef.set(mapper.readTree("{\"pct\": 73, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26\"}"));

        final JsonNode rootNode = rootNodeRef.get();
        HashMap<String, Object> jsonProperties =
                new ObjectMapper().convertValue(rootNode, HashMap.class);
        SpaceDocument dataAsDocument = new SpaceDocument("test", jsonProperties);

        gigaSpace.write(dataAsDocument);


        Connection conn;
//        Class.forName("com.j_spaces.jdbc.driver.GDriver").newInstance();
//        String url = "jdbc:gigaspaces:url:jini://*/*/demo";



        Class.forName("com.gigaspaces.jdbc.Driver").newInstance();
        String url = "jdbc:insightedge:spaceName=demo";

        conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        String query = "select * from test ";
        st = conn.createStatement();
        ResultSet rs = st.executeQuery(query);
        while (rs.next()) {
            String s = rs.getString("asof");
            System.out.println(s);
        }

        query = "select ROUNDTOMINS(asof) from test";
        st = conn.createStatement();
        rs = st.executeQuery(query);
        while (rs.next()) {
            String s = rs.getString("asof");
            System.out.println(s);
        }


        com.gs.DateUtil dateUtil = new com.gs.DateUtil();
        Date now = new Date();
        LocalDateTime rightNow = now.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        List<Object> l = new ArrayList<>();
        l.add(rightNow);
        FunctionCallDescription sc = new FunctionCallDescription("asof", 0, l);
        LocalDateTime tt = (LocalDateTime) dateUtil.apply(sc);
        assert tt.toString().split(":").length == 2;
        System.out.println(dateUtil.apply(sc));
    }
}
