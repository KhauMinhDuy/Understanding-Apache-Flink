package com.khauminhduy.module3;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

public class FilterEnglishTweets {

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty(TwitterSource.CONSUMER_KEY, "...");
		props.setProperty(TwitterSource.CONSUMER_SECRET, "...");
		props.setProperty(TwitterSource.TOKEN, "...");
		props.setProperty(TwitterSource.TOKEN_SECRET, "...");

		env.addSource(new TwitterSource(props))
			.map(new MapToTweets())
			.filter(new FilterFunction<Tweet>() {

				@Override
				public boolean filter(Tweet tweet) throws Exception {
					return tweet.getLanguage().equals("en");
				}
				
			})
			.print();

	}

}
