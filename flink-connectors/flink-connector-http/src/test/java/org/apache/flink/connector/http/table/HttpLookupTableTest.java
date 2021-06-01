package org.apache.flink.connector.http.table;


import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author xiaozilong
 * @date 2021/05/28 10:05
 */
public class HttpLookupTableTest {

    @Test
    public void testLookup() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		String sql1 = "CREATE TABLE datagen (`id` int, `name` string, `proctime` as PROCTIME()) "
			+ "WITH ('connector'='datagen')";
		tEnv.executeSql(sql1);

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, "1"),
			new Tuple2<>(2, "5"),
			new Tuple2<>(3, "8")
		)), $("id"), $("id2"), $("proctime").proctime());

		tEnv.createTemporaryView("T", t);

        String sql2 = "CREATE TABLE http_test (`orderId` int, `orderName` string, `orderStatus` int, `desc` string) "
                + "WITH ("
			+ "'connector'='http', "
			+ "'lookup.async'='true',"
			+ "'table-name'='test_http', "
			+ "'request.url'='http://localhost:8080/order', "
			+ "'request.method'='POST', "
			+ "'request.headers'='Content-Type:application/json')";
        tEnv.executeSql(sql2);

		String sqlQuery = "SELECT source.id, L.`orderId`, L.orderName, L.orderStatus, L.desc FROM T AS source " +
			"JOIN http_test for system_time as of source.proctime AS L " +
			"ON source.id = L.orderId";

		CloseableIterator<Row> collected = tEnv.executeSql(sqlQuery).collect();
		List<String> result = Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		System.out.println("result: " + result);
    }
}
