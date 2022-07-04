package com.khauminhduy.module2;

import java.util.Arrays;
import java.util.HashSet;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;

public class FilterMovies {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// @formatter:off
		DataSource<Tuple3<Long, String, String>> lines = env.readCsvFile("ml-latest-small/movies.csv")
					.ignoreFirstLine()
					.parseQuotedStrings('"')
					.ignoreInvalidLines()
					.types(Long.class, String.class, String.class);
		
		DataSet<Movie> movies = lines.map(new MapFunction<Tuple3<Long,String,String>, Movie>() {
			@Override
			public Movie map(Tuple3<Long, String, String> line) throws Exception {
				String movieName = line.f1;
				String[] genres = line.f2.split("\\|");
				return new Movie(movieName, new HashSet<>(Arrays.asList(genres)));
			}
		}).filter(new FilterFunction<Movie>() {
			@Override
			public boolean filter(Movie movie) throws Exception {
				return movie.getGenres().contains("Drama");
			}
		});
		
//		filterMovies.print();
		
		movies.writeAsText("filter-output");
		env.execute();
		
		// @formatter:on

	}

}
