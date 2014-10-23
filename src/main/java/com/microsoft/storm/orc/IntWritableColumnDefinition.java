package com.microsoft.storm.orc;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

import com.microsoft.storm.orc.OrcTupleSchema.ColumnDefinition;


public final class IntWritableColumnDefinition extends WritableColumnDefinition {
	public IntWritableColumnDefinition(String name) {
		super(new ColumnDefinition(name, TypeInfoFactory.intTypeInfo));
	}

	@Override
	public Object getWritableValue(Object rawValue) {
		return new IntWritable((Integer)rawValue);
	}
}