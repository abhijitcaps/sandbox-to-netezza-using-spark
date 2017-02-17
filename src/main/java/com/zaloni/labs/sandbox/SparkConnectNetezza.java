package com.zaloni.labs.sandbox;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.jdbc.JdbcDialects;

import java.util.List;
import java.util.Properties;

public class SparkConnectNetezza{
    private static final SparkContext sc =
            new SparkContext(new SparkConf().setAppName("SparkSaveToDb").setMaster("local[*]"));

    private static final SQLContext sqlContext = new HiveContext(sc);

    public static void main(String[] args) throws Exception{

        final String jdbcUrl = args[0];
        System.out.println("destinationLocation :: "+jdbcUrl);

        /*
        Source table, from where metadata and data will be fetched. This is from Hive in our scenario
         */
        String hiveSchemaNameDotTableName = "schemaName.TableName"; //Hive SchemaName.TableName
        
        sqlContext.setConf("hive.metastore.filter.hook", "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl");

        String hql = "select * from "+hiveSchemaNameDotTableName;
        DataFrame usersDf = sqlContext.sql(hql);
        System.out.print("Data Frame :: "+usersDf); // Output is => Data Frame :: [id: int, name: string]
        final SQLContext sqlc = new HiveContext(sc);
        Properties prop = new Properties();
        prop.setProperty("url", jdbcUrl);
        prop.setProperty("driver", "org.netezza.Driver");
        sqlc.setConf(prop);
        DataFrame usersDf1 = sqlc.read().json(usersDf.toJSON());

        JdbcDialects.registerDialect(new NetezzaDialect());

        usersDf1.write().mode("overwrite").jdbc(jdbcUrl, "destinationTableName", prop);
    }
}
