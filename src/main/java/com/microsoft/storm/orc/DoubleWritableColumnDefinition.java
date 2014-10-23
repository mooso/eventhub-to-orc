package com.microsoft.storm.orc;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.microsoft.storm.orc.OrcTupleSchema.ColumnDefinition;


public final class DoubleWritableColumnDefinition extends WritableColumnDefinition {
	public DoubleWritableColumnDefinition(String name) {
		super(new ColumnDefinition(name, TypeInfoFactory.doubleTypeInfo));
	}

	@Override
	public Object getWritableValue(Object rawValue) {
		if (Integer.class.isInstance(rawValue)) {
			return new org.apache.hadoop.hive.serde2.io.DoubleWritable((Integer)rawValue);
		} else {
			return new org.apache.hadoop.hive.serde2.io.DoubleWritable((Double)rawValue);
		}
	}
}