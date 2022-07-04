package com.khauminhduy.module2;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class Demo {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSource<Integer> datas = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9);
		DataSet<Integer> result = datas.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return value % 2 == 0;
			}
		});
		
		result.print();
		
	}

}
