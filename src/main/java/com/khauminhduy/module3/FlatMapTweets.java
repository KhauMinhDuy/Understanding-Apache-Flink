package com.khauminhduy.module3;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlatMapTweets implements FlatMapFunction<Tweet, Tuple2<String, Integer>>{

	@Override
	public void flatMap(Tweet value, Collector<Tuple2<String, Integer>> out) throws Exception {
				
	}
	
}
