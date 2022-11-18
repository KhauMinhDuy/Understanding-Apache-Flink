package com.khauminhduy.module3;

import java.util.Date;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AllWindowFunctionTweets
		implements AllWindowFunction<Tuple2<String, Integer>, Tuple3<Date, String, Integer>, TimeWindow> {

	@Override
	public void apply(TimeWindow window, Iterable<Tuple2<String, Integer>> iterable,
			Collector<Tuple3<Date, String, Integer>> collector) throws Exception {
		String topHashTag = null;
		int count = 0;
		for (Tuple2<String, Integer> hashTag : iterable) {
			if (hashTag.f1 > count) {
				topHashTag = hashTag.f0;
				count = hashTag.f1;
			}
		}

		collector.collect(new Tuple3<>(new Date(window.getEnd()), topHashTag, count));
	}

}
