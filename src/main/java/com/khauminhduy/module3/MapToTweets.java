package com.khauminhduy.module3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

public class MapToTweets implements MapFunction<String, Tweet> {

	private static final ObjectMapper mapper = new ObjectMapper();

	@Override
	public Tweet map(String tweetJsonStr) throws Exception {
		JsonNode tweetJson = mapper.readTree(tweetJsonStr);
		JsonNode textNode = tweetJson.get("text");
		JsonNode langNode = tweetJson.get("lang");

		String text = textNode == null ? "" : textNode.getTextValue();
		String lang = langNode == null ? "" : langNode.getTextValue();

		List<String> tags = new ArrayList<>();

		JsonNode entities = tweetJson.get("entities");

		if (entities != null) {
			JsonNode hashtags = entities.get("hashtags");

			for (Iterator<JsonNode> iter = hashtags.getElements(); iter.hasNext();) {
				JsonNode node = iter.next();
				String hashtag = node.get("text").getTextValue();
				tags.add(hashtag);
			}
		}

		return new Tweet(lang, text, tags);
	}

}
