package com.khauminhduy.module3;

import java.util.Properties;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class NumberOfTweetsPerLanguage {
	
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "...");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
		props.setProperty(TwitterSource.TOKEN, "...");
		props.setProperty(TwitterSource.TOKEN_SECRET, "...");

		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		env.addSource(new TwitterSource(props))
			.map(new MapToTweets())
			.keyBy(new KeySelectorTweets())
			.timeWindow(Time.minutes(1))
			.apply(new WindowFunctionTweets())
			.print();

		env.execute();

	}

}
