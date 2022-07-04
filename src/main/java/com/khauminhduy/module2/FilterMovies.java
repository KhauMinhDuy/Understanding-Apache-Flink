package com.khauminhduy.module2;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class FilterMovies {

	public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// @formatter:off
		DataSource<Tuple3<Long, String, String>> movies = env.readCsvFile("ml-latest-small/movies.csv")
					.ignoreFirstLine()
					.parseQuotedStrings('"')
					.ignoreInvalidLines()
					.types(Long.class, String.class, String.class);
		
		// @formatter:on

	}

}
