package com.khauminhduy.module3;

import org.apache.flink.api.java.functions.KeySelector;

public class KeySelectorTweets implements KeySelector<Tweet, String>{

	@Override
	public String getKey(Tweet tweet) throws Exception {
		return tweet.getLanguage();
	}
	
}
