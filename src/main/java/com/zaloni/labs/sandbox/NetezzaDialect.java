package com.zaloni.labs.sandbox;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;

import scala.Option;

import java.sql.Types;

public class NetezzaDialect extends JdbcDialect{

    @Override
    public boolean canHandle(String arg0) {
        if(arg0.startsWith("jdbc:netezza")){
            return true;
        }
        return false;
    }

    public Option<JdbcType> getJDBCType(DataType dt){
        Option<JdbcType> apply = null;
        String dataType = dt.toString().toUpperCase();
        switch (dataType) {
            case "LONGTYPE":
                apply = Option.apply(new JdbcType("BIGINT", Types.BIGINT));
                System.out.println("LONGTYPE => "+apply);
                break;
            case "STRINGTYPE":
                apply = Option.apply(new JdbcType("VARCHAR(255)", java.sql.Types.VARCHAR));
                System.out.println("STRINGTYPE => "+apply);
                break;
            default:
                System.out.println("Default");
        }
        return apply;
    }
}
