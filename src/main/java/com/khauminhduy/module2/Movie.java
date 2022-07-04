package com.khauminhduy.module2;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@ToString
public class Movie {

	private String name;
	private Set<String> genres;
	
}
