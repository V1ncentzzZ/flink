package org.apache.flink.connector.redis.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xiaozilong
 * @date 2021/05/28 10:05
 */
public class RedisLookupTableTest {

    static String dataGenSql =
            "CREATE TABLE datagen (`id` int, `name` string, `proctime` as PROCTIME()) "
                    + "WITH ("
                    + "'connector'='datagen', "
                    + "'rows-per-second'='1', "
                    + "'fields.id.min'='1', "
                    + "'fields.id.max'='10' "
                    + ")";

    @Test
    public void testSyncLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>("zl_1", "1"), new Tuple2<>("zl_2", "2"))),
                        $("id"),
                        $("name"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String sql2 =
                "CREATE TABLE redis_test (`orderId` string, `orderName` string) "
                        + "WITH ("
                        + "'connector'='redis', "
                        + "'host'='codis.devops-k8s.sg2.i.sz.shopee.io', "
                        + "'port'='31925', "
                        + "'mode'='single', "
                        + "'lookup.async'='false',"
                        + "'format'='csv', "
                        + "'csv.field-delimiter'='_' "
                        + ")";
        tEnv.executeSql(sql2);

        tEnv.executeSql(dataGenSql);

        String sqlQuery =
                "SELECT source.id, L.`orderId`, L.orderName FROM T AS source "
                        + "JOIN redis_test for system_time as of source.proctime AS L "
                        + "ON source.id = L.orderId";

        CloseableIterator<Row> collected = tEnv.executeSql(sqlQuery).collect();
        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("size: " + result.size() + " result: " + result);
    }

    @Test
    public void testCodisAsyncLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>("zl_1", "1"),
                                        new Tuple2<>("zl_2", "2"),
                                        new Tuple2<>("zl_3", "2"),
                                        new Tuple2<>("zl_4", "2"),
                                        new Tuple2<>("zl_5", "2"))),
                        $("id"),
                        $("name"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String sql2 =
                "CREATE TABLE redis_test (`orderId` string, `orderName` string) "
                        + "WITH ("
                        + "'connector'='shopee-redis', "
                        + "'redis.nodes'='localhost:6379', "
                        + "'redis.mode'='single', "
                        + "'lookup.batch.size'='5', "
                        + "'lookup.async'='true', "
                        + "'format'='csv', "
                        + "'csv.field-delimiter'='_' "
                        + ")";
        tEnv.executeSql(sql2);

        String sqlQuery =
                "SELECT source.id, L.`orderId`, L.orderName FROM T AS source "
                        + "JOIN redis_test for system_time as of source.proctime AS L "
                        + "ON source.id = L.orderId";

        CloseableIterator<Row> collected = tEnv.executeSql(sqlQuery).collect();
        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("size: " + result.size() + " result: " + result);
    }

    @Test
    public void testSentinelAsyncLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>("zl_1", "1"),
                                        new Tuple2<>("zl_2", "2"),
                                        new Tuple2<>("zl_3", "2"),
                                        new Tuple2<>("zl_4", "2"),
                                        new Tuple2<>("zl_5", "2"))),
                        $("id"),
                        $("name"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String sql2 =
                "CREATE TABLE redis_test (`orderId` string, `orderName` string) "
                        + "WITH ("
                        + "'connector'='shopee-redis', "
                        + "'redis.nodes'='localhost:6379', "
                        + "'redis.mode'='single', "
                        + "'lookup.batch.size'='-1', "
                        + "'lookup.async'='true', "
                        + "'format'='csv', "
                        + "'csv.field-delimiter'='_' "
                        + ")";
        tEnv.executeSql(sql2);

        String sqlQuery =
                "SELECT source.id, L.`orderId`, L.orderName FROM T AS source "
                        + "JOIN redis_test for system_time as of source.proctime AS L "
                        + "ON source.id = L.orderId";

        CloseableIterator<Row> collected = tEnv.executeSql(sqlQuery).collect();
        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("size: " + result.size() + " result: " + result);
    }
}
