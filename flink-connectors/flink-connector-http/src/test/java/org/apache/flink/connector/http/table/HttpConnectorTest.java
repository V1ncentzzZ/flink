package org.apache.flink.connector.http.table;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;


import org.junit.Test;


/**
 * @author xiaozilong
 * @date 2021/05/28 10:05
 */
public class HttpConnectorTest {

    @Test
    public void testHttpConnector() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings envSettings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.inStreamingMode()
			.build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, envSettings);

		String sql2 = "CREATE TABLE http_test (`orderId` int, `orderName` string, `orderStatus` int, `desc` string) "
			+ "WITH ("
			+ "'connector'='http', "
			+ "'lookup.async'='true',"
			+ "'table-name'='test_http', "
			+ "'request.url'='http://localhost:8080/order', "
			+ "'request.method'='POST', "
			+ "'request.headers'='Content-Type:application/json')";
		tEnv.executeSql(sql2);

		String sqlQuery = "SELECT * FROM http_test";

		Table result = tEnv.sqlQuery(sqlQuery);

		tEnv.toAppendStream(result, RowData.class).print("===== ");
    }
}
