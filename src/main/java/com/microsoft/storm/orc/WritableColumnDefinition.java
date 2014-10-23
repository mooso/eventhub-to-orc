package com.microsoft.storm.orc;

import com.microsoft.storm.orc.OrcTupleSchema.ColumnDefinition;


public abstract class WritableColumnDefinition {
	private final ColumnDefinition _columnDefinition;

	protected WritableColumnDefinition(ColumnDefinition columnDefinition) {
		_columnDefinition = columnDefinition;
	}

	public ColumnDefinition getDefinition() {
		return _columnDefinition;
	}

	public abstract Object getWritableValue(Object rawValue);
}