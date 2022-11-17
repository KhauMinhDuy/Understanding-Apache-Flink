package com.khauminhduy.module3;

import java.util.List;

public class Tweet {
	private String language;
	private String text;
	private List<String> tags;

	public Tweet(String language, String text, List<String> tags) {
		this.language = language;
		this.text = text;
		this.tags = tags;
	}

	public String getLanguage() {
		return language;
	}

	public String getText() {
		return text;
	}

	public List<String> getTags() {
		return tags;
	}

	@Override
	public String toString() {
		return "Tweet{" +
				"language='" + language + '\'' +
				", text='" + text + '\'' +
				", tags=" + tags +
				'}';
	}
}
