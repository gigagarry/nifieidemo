package com.gs;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.time.DateUtils;

public class DateUtil extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        System.out.println("Input" +context.getArgument(0) + " " + context.getArgument(0).getClass());
        LocalDateTime in;
        if (context.getArgument(0) instanceof String) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            in = LocalDateTime.parse((String) context.getArgument(0), formatter);
            Date nearestMinute = DateUtils.round(java.sql.Timestamp.valueOf(in), Calendar.MINUTE);
            LocalDateTime out = nearestMinute.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();
//                    .toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            System.out.println("Output" + out);
            return out;
        } else {
            in=(LocalDateTime) context.getArgument(0);
            Date nearestMinute = DateUtils.round(java.sql.Timestamp.valueOf(in), Calendar.MINUTE);
            System.out.println("Output" + nearestMinute.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime());
            return nearestMinute.toInstant()
                    .atZone(ZoneId.systemDefault())
                    .toLocalDateTime();
        }

    }
}