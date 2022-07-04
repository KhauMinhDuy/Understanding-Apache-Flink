package com.khauminhduy.module2;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

public class AverageRating {

	public static void main(String[] args) throws Exception {
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
		
		List<Tuple2<String, Double>> results = movies.join(ratings)
				.where(0)
				.equalTo(0)
				.with(new JoinFunction<Tuple3<Long, String, String>, Tuple2<Long, Double>, 
						Tuple3<StringValue, StringValue, DoubleValue>>() {
					private StringValue name = new StringValue();
					private StringValue genres = new StringValue();
					private DoubleValue score = new DoubleValue();
					private Tuple3<StringValue, StringValue, DoubleValue> result = new Tuple3<>(name, genres, score);
					
					@Override
					public Tuple3<StringValue, StringValue, DoubleValue> join(Tuple3<Long, String, String> movie, 
							Tuple2<Long, Double> rating) throws Exception {
						name.setValue(movie.f1);
						genres.setValue(movie.f2.split("\\|")[0]);
						score.setValue(rating.f1);
						return result;
					}
				})
//				.print();
				.groupBy(1)
				.reduceGroup(new GroupReduceFunction<Tuple3<StringValue,StringValue,DoubleValue>, Tuple2<String, Double>>() {
					@Override
					public void reduce(Iterable<Tuple3<StringValue,StringValue,DoubleValue>> iterable, 
							Collector<Tuple2<String, Double>> collector) throws Exception {
						String genres = null;
						int count = 0;
						double totalScore = 0;
						for (Tuple3<StringValue,StringValue,DoubleValue> movie : iterable) {
							genres = movie.f1.getValue();
							totalScore += movie.f2.getValue();
							count++;
						}
						collector.collect(new Tuple2<>(genres, totalScore / count));
					}
				})
				.collect();
		
		String rs = results.stream()
			.sorted((s1, s2) -> Double.compare(s1.f1, s2.f1))
			.map(Object::toString)
			.collect(Collectors.joining("\n"));
		
		System.out.println(rs);
		
		// @formatter:on

	}

}
