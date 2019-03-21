package com.gs;
import com.gigaspaces.query.sql.functions.SqlFunction;
import com.gigaspaces.query.sql.functions.SqlFunctionExecutionContext;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import org.apache.commons.lang.time.DateUtils;

public class DateUtil extends SqlFunction {
    @Override
    public Object apply(SqlFunctionExecutionContext context) {
        System.out.println(context.getArgument(0));
        Date nearestMinute = DateUtils.round(java.sql.Timestamp.valueOf((LocalDateTime) context.getArgument(0)), Calendar.MINUTE);
        System.out.println(nearestMinute);
        return nearestMinute.toInstant()
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
    }
}