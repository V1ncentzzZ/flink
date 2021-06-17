package org.apache.flink.connector.http.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xiaozilong
 * @date 2021/05/28 10:05
 */
public class HttpLookupTableTest {

    static String dataGenSql =
            "CREATE TABLE datagen (`id` int, `name` string, `proctime` as PROCTIME()) "
                    + "WITH ("
                    + "'connector'='datagen', "
                    + "'rows-per-second'='1', "
                    + "'fields.id.min'='1', "
                    + "'fields.id.max'='10' "
                    + ")";

    public void testSyncLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "8"),
                                        new Tuple2<>(4, "9"),
                                        new Tuple2<>(5, "9"))),
                        $("id"),
                        $("name"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String sql2 =
                "CREATE TABLE http_test (`orderId` int, `orderName` string, `orderStatus` int, `desc` string) "
                        + "WITH ("
                        + "'connector'='http', "
                        + "'lookup.async'='false',"
                        + "'request.url'='http://localhost:8080/order_post', "
                        + "'request.method'='POST', "
                        + "'request.headers'='Content-Type:application/json', "
                        + "'lookup.cache.max-rows'='-1', "
                        + "'lookup.cache.ttl'='1 s', "
                        + "'format'='http-json', "
                        + "'http-json.response.data.fields'='data' "
                        + ")";
        tEnv.executeSql(sql2);

        tEnv.executeSql(dataGenSql);

        String sqlQuery =
                "SELECT source.id, L.`orderId`, L.orderName, L.orderStatus, L.desc FROM T AS source "
                        + "JOIN http_test for system_time as of source.proctime AS L "
                        + "ON source.id = L.orderId";

        CloseableIterator<Row> collected = tEnv.executeSql(sqlQuery).collect();
        List<String> result =
                Lists.newArrayList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());

        System.out.println("size: " + result.size() + " result: " + result);
    }

    public void testAsyncLookup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings envSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

        Table t =
                tEnv.fromDataStream(
                        env.fromCollection(
                                Arrays.asList(
                                        new Tuple2<>(1, "1"),
                                        new Tuple2<>(2, "5"),
                                        new Tuple2<>(3, "8"),
                                        new Tuple2<>(4, "9"),
                                        new Tuple2<>(3, "9"))),
                        $("id"),
                        $("name"),
                        $("proctime").proctime());

        tEnv.createTemporaryView("T", t);

        String sql2 =
                "CREATE TABLE http_test (`orderId` int, `orderName` string, `orderStatus` int, `desc` string) "
                        + "WITH ("
                        + "'connector'='http', "
                        + "'lookup.async'='true',"
                        + "'request.url'='http://localhost:8080/order_post_batch_result', "
                        + "'request.method'='POST', "
                        + "'request.parameters'='orderIds', "
                        + "'request.headers'='Content-Type:application/json', "
                        + "'request.batch.size'='2', "
                        + "'lookup.cache.max-rows'='100', "
                        + "'lookup.cache.ttl'='1 s', "
                        + "'format'='http-json', "
                        + "'http-json.ignore-parse-errors'='true', "
                        + "'http-json.response.data.fields'='data' "
                        + ")";
        tEnv.executeSql(sql2);

        String sqlQuery =
                "SELECT source.id, L.`orderId`, L.orderName, L.orderStatus, L.desc FROM T AS source "
                        + "JOIN http_test for system_time as of source.proctime AS L "
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
