package com.khauminhduy.module3;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

public class TopTagPerLanguage {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "...");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
		props.setProperty(TwitterSource.TOKEN, "...");
		props.setProperty(TwitterSource.TOKEN_SECRET, "...");

		env.addSource(new TwitterSource(props))
				// parse tweets
				.map(new MapToTweets()) // DataStream<Tweet>
				// extract hashtags from tweets
				.flatMap(new FlatMapFunction<Tweet, Tuple3<String, String, Long>>() {
					@Override
					public void flatMap(Tweet tweet, Collector<Tuple3<String, String, Long>> collector) throws Exception {
						for (String tag : tweet.getTags()) {
							collector.collect(new Tuple3<>(tweet.getLanguage(), tag, 1L));
						}
					}
				}) // DataStream<Tuple3<String, String, Long>>
				// Key by language AND hashtag value
				.keyBy(0, 1)
				.timeWindow(Time.minutes(15))
				// Count how many times each tag was used in tweets by different languages
				.reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
					@Override
					public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> tag1,
							Tuple3<String, String, Long> tag2) throws Exception {
						return new Tuple3<>(tag1.f0, tag1.f1, tag1.f2 + tag2.f2);
					}
				}) // DataStream<Tuple3<String, String, Long>
				// Group by language and count what is the most popular hashtag per language
				.keyBy(0)
				.timeWindow(Time.minutes(15))
				.reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
					@Override
					public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> tag1,
							Tuple3<String, String, Long> tag2) throws Exception {
						if (tag1.f2 >= tag2.f2) {
							return tag1;
						} else {
							return tag2;
						}
					}
				}) // DataStream<Tuple3<String, String, Long>>
				.print();

		env.execute();
	}

}
