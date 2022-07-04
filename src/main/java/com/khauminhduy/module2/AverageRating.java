package com.khauminhduy.module2;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class AverageRating {

	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// @formatter:off
		DataSource<Tuple3<Long, String, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
				.ignoreFirstLine()
				.parseQuotedStrings('"')
				.ignoreInvalidLines()
				.includeFields(true, true, true)
				.types(Long.class, String.class, String.class);
		
		DataSource<Tuple2<Long, Double>> ratings = env.readCsvFile("ml-latest-small/ratings.csv")
				.ignoreFirstLine()
				.includeFields(false, true, true, false)
				.types(Long.class, Double.class);
		
		// @formatter:on

	}

}
