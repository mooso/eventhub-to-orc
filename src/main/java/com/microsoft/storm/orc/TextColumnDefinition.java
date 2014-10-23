package com.microsoft.storm.orc;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

import com.microsoft.storm.orc.OrcTupleSchema.ColumnDefinition;


public final class TextColumnDefinition extends WritableColumnDefinition {
	public TextColumnDefinition(String name) {
		super(new ColumnDefinition(name, TypeInfoFactory.stringTypeInfo));
	}

	@Override
	public Object getWritableValue(Object rawValue) {
		return new Text((String)rawValue);
	}
}