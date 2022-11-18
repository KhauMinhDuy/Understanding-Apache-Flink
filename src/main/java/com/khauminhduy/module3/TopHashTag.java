package com.khauminhduy.module3;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class TopHashTag {

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "...");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
		props.setProperty(TwitterSource.TOKEN, "...");
		props.setProperty(TwitterSource.TOKEN_SECRET, "...");

		env.addSource(new TwitterSource(props))
			.map(new MapToTweets())
			.flatMap(new FlatMapTweets())
			.keyBy(0)
			.timeWindow(Time.minutes(1))
			.sum(1)
			.timeWindowAll(Time.minutes(1))
			.apply(new AllWindowFunctionTweets())
			.print();

	}

}
