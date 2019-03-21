package com.gs;

import com.gigaspaces.internal.io.IOUtils;
import com.j_spaces.jdbc.builder.range.FunctionCallDescription;
import org.junit.Test;

import java.io.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class DateUtilTest {
    @Test
    public void testIt() {

        com.gs.DateUtil dateUtil = new com.gs.DateUtil();
        Date now = new Date();
        LocalDateTime rightNow =now.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        List<Object> l = new ArrayList<>();
        l.add(rightNow);
        FunctionCallDescription sc = new FunctionCallDescription("asof",0, l);
        LocalDateTime tt = (LocalDateTime) dateUtil.apply(sc);
        assert tt.toString().split(":").length==2;
        System.out.println(dateUtil.apply(sc));
    }
}
