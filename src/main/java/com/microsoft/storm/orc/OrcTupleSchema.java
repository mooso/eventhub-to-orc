package com.microsoft.storm.orc;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import backtype.storm.tuple.Tuple;

public abstract class OrcTupleSchema implements Serializable {
	private static final long serialVersionUID = 3589035965719368825L;

	public static class ColumnDefinition implements Serializable {
		private static final long serialVersionUID = -4112628680199279133L;

		private final String _columnName;
		private final TypeInfo _columnType;

		public ColumnDefinition(String columnName, TypeInfo columnType) {
			_columnName = columnName;
			_columnType = columnType;
		}

		public String getColumnName() {
			return _columnName;
		}

		public TypeInfo getColumnType() {
			return _columnType;
		}
	}

	public abstract List<ColumnDefinition> getColumnDefinitions();

	public abstract List<ProcessedTuple> processTuple(Tuple tuple, ErrorReporter errorReporter);
	
	public abstract static class ProcessedTuple {
		public abstract Object getColumnValue(String columnName);
	}
}
