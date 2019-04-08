package com.gs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gigaspaces.document.SpaceDocument;
import com.gigaspaces.internal.io.IOUtils;
import com.gigaspaces.metadata.SpaceDocumentSupport;
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
import java.time.Month;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;


public class DateUtilRest {
    @Test
    public void testIt() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException, IOException {

        {
            System.setProperty("com.gs.jini_lus.groups", "xap-14.0.1");
            System.setProperty("com.gs.jini_lus.locators", "127.0.0.1:4174");
        }
        GigaSpace gigaSpace = new GigaSpaceConfigurer(new SpaceProxyConfigurer("mySpace")).gigaSpace();


        SpaceTypeDescriptorBuilder typeDescriptorBuilder = new SpaceTypeDescriptorBuilder("Park")
                .idProperty("id", true)
                .routingProperty("id");
        typeDescriptorBuilder.addFixedProperty("airport",
                String.class);
        typeDescriptorBuilder.addFixedProperty("pct",
                Integer.class);
        typeDescriptorBuilder.addFixedProperty("asof2", "java.time.LocalDateTime");
        typeDescriptorBuilder.addFixedProperty("asof",
                LocalDateTime.class);
        // Register type:
        SpaceTypeDescriptor typeDescriptor = typeDescriptorBuilder.create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);

        typeDescriptorBuilder = new SpaceTypeDescriptorBuilder("Parking")
                .idProperty("id")
                .routingProperty("id");
        typeDescriptorBuilder.addFixedProperty("asof",
                LocalDateTime.class);
        // Register type:
        typeDescriptor = typeDescriptorBuilder.create();
        gigaSpace.getTypeManager().registerTypeDescriptor(typeDescriptor);

        System.out.println(gigaSpace.getTypeManager().getTypeDescriptor("Park").getFixedProperty("asof").getType().getCanonicalName());
        System.out.println(gigaSpace.getTypeManager().getTypeDescriptor("Parking").getFixedProperty("asof").getType().getCanonicalName());

        ObjectMapper mapper = new ObjectMapper();
        AtomicReference<JsonNode> rootNodeRef = new AtomicReference<>(null);
        rootNodeRef.set(mapper.readTree("{ \"id\": 8,\"asof\": \"2019-03-06 15:59:12\"}"));
        JsonNode rootNode = rootNodeRef.get();
        HashMap<String, Object> jsonProperties =
                new ObjectMapper().convertValue(rootNode, HashMap.class);
        SpaceDocument dataAsDocument = new SpaceDocument("Parking", jsonProperties);
        gigaSpace.write(dataAsDocument);


        mapper = new ObjectMapper();
        rootNodeRef = new AtomicReference<>(null);
        rootNodeRef.set(mapper.readTree("{\"pct\": 73, \"airport\": \"EWR\", \"asof\": \"2019-03-06 15:59:26\", \"asof2\": \"2019-03-06 15:59:26\"}"));

        rootNode = rootNodeRef.get();
        jsonProperties =
                new ObjectMapper().convertValue(rootNode, HashMap.class);
        dataAsDocument = new SpaceDocument("Park", jsonProperties);

        gigaSpace.write(dataAsDocument);


        Connection conn;
//        Class.forName("com.j_spaces.jdbc.driver.GDriver").newInstance();
//        String url = "jdbc:gigaspaces:url:jini://*/*/demo";


        String sConnect = "jdbc:gigaspaces:url:jini://*/*/mySpace?locators=127.0.0.1:4174&groups=xap-14.0.1";

        final com.j_spaces.jdbc.driver.GConnection conno;


        LocalDateTime ldt = LocalDateTime.of(2019, Month.MARCH, 06, 15, 59);


        com.gs.DateUtil dateUtil = new com.gs.DateUtil();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime rightNow = LocalDateTime.parse("2019-03-06 15:59:12", formatter);
        List<Object> l = new ArrayList<>();
        l.add(rightNow);
        FunctionCallDescription sc = new FunctionCallDescription("asof", 0, l);
        LocalDateTime tt = (LocalDateTime) dateUtil.apply(sc);
        assert tt.toString().split(":").length == 2;
        System.out.println(dateUtil.apply(sc));

        Class.forName("com.j_spaces.jdbc.driver.GDriver");
        Properties props = new Properties();
        props.put("com.gs.embeddedQP.enabled", "true");
        conno = (com.j_spaces.jdbc.driver.GConnection) DriverManager
                .getConnection(sConnect, props);
        //conn.setUseSingleSpace(false);

        SQLQuery<SpaceDocument> squery =
                new SQLQuery<SpaceDocument>("Parking",  "rowNum < 2");

        SpaceDocument result = gigaSpace.read(squery);
        if (result != null ) {
            System.out.println("first result.asof: " + result.getProperty("asof") + " " + result.getProperty("asof").getClass());
        } else {
            System.out.println("No results returned for query using udf");
        }

        squery =
                new SQLQuery<SpaceDocument>("Parking",  "asof = ?");

        squery.setParameter(1, rightNow);

        System.out.println("rigthNow : " + rightNow);

        result = gigaSpace.read(squery);

        if (result != null ) {
            System.out.println("equality result.asof: " + result.getProperty("asof") + " " + result.getProperty("asof").getClass());
        } else {
            System.out.println("No results returned for query using udf");
        }

        String ldts = "2019-03-06 15:59:12";
        String ldtst = "2019-03-06 15:59:00";
        squery.setParameter(1, ldts);

        System.out.println("ldts : " + ldts);

        result = gigaSpace.read(squery);

        if (result != null ) {
            System.out.println("equality string result.asof: " + result.getProperty("asof") + " " + result.getProperty("asof").getClass());
        } else {
            System.out.println("No results returned for query using udf");
        }

        squery =
                new SQLQuery<SpaceDocument>("Parking",  "ROUNDTOMINS(asof) = ?");
        squery.setParameter(1, ldts);

        System.out.println("ldtst: " + ldtst);

        result = gigaSpace.read(squery);

        if (result != null ) {
            System.out.println("result.asof: " + result.getProperty("asof") + " " + result.getProperty("asof").getClass());
        } else {
            System.out.println("No results returned for query using udf");
        }

        PreparedStatement stmt = conno.prepareStatement("select * from Parking");
        ResultSet rs = stmt.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getObject("id"));
            System.out.println(rs.getObject("asof"));
        }
        System.out.println(ldtst+"=");
        stmt = conno.prepareStatement("select * from Parking where ROUNDTOMINS(asof) = ?");
        stmt.setObject(1, ldtst);
        rs = stmt.executeQuery();

        while (rs.next()) {
            System.out.println("filtered" + rs.getObject("id"));
            System.out.println(rs.getObject("id"));
            System.out.println(rs.getObject("asof"));
        }


        System.out.println(ldtst+"=");
        stmt = conno.prepareStatement("select * from Park where ROUNDTOMINS(asof) = ?");
        stmt.setObject(1, ldtst);
        rs = stmt.executeQuery();

        while (rs.next()) {
            System.out.println("filtered park" + rs.getObject("id"));
            System.out.println(rs.getObject("id"));
            System.out.println(rs.getObject("asof"));
        }

        stmt = conno.prepareStatement("select ROUNDTOMINS(asof) from Park ");
        stmt.setObject(1, ldt);
        rs = stmt.executeQuery();

        while (rs.next()) {
            System.out.println("select");

            System.out.println(rs.getObject("id"));
            System.out.println(rs.getObject("asof"));
        }

        Class.forName("com.gigaspaces.jdbc.Driver").newInstance();
        String url = "jdbc:insightedge:spaceName=mySpace";

        conn = DriverManager.getConnection(url);
        Statement st = conn.createStatement();
        String query = "select * from Parking ";
        st = conn.createStatement();
        rs = st.executeQuery(query);
        while (rs.next()) {
            String s = rs.getString("asof");
            System.out.println(s);
        }
// This fails wit a parsing error
        System.out.println(ldtst+"=");
        stmt = conno.prepareStatement("select * from Park where ROUNDTOMINS(asof) = ?");
        stmt.setObject(1, ldtst);
        st = conn.createStatement();
        rs = st.executeQuery(query);
        while (rs.next()) {
            String s = rs.getString("asof");
            System.out.println("new jdbc " + s);
        }

        query = "select * from Park ";
        st = conn.createStatement();
        rs = st.executeQuery(query);
        while (rs.next()) {
            String s = rs.getString("asof");
            System.out.println(s);
        }
// This fails wit a parsing error
//        query = "select ROUNDTOMINS(asof) from Parking";
//        st = conn.createStatement();
//        rs = st.executeQuery(query);
//        while (rs.next()) {
//            String s = rs.getString("asof");
//            System.out.println(s);
//        }

        l = new ArrayList<>();
        l.add(rightNow);
        sc = new FunctionCallDescription("asof", 0, l);
        tt = (LocalDateTime) dateUtil.apply(sc);
        assert tt.toString().split(":").length == 2;
        System.out.println(dateUtil.apply(sc));
    }
}
