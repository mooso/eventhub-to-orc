package com.microsoft.storm.orc;

import java.util.*;

import com.fasterxml.jackson.databind.*;
import com.google.common.base.*;
import com.google.common.collect.*;

import backtype.storm.tuple.*;

public class JsonOrcTupleSchema extends OrcTupleSchema {
	private static final long serialVersionUID = -524531838672712694L;
	private final ObjectMapper _mapper = new ObjectMapper();
	private final Map<String, WritableColumnDefinition> _columnsByName;
	private final List<ColumnDefinition> _columns;

	public JsonOrcTupleSchema(List<WritableColumnDefinition> columnDefinitions) {
		_columnsByName = new HashMap<String, WritableColumnDefinition>();
		for (WritableColumnDefinition column: columnDefinitions) {
			_columnsByName.put(column.getDefinition().getColumnName(), column);
		}
		_columns = Lists.newArrayList(Iterables.transform(columnDefinitions,
				new Function<WritableColumnDefinition, ColumnDefinition>() {
					public ColumnDefinition apply(WritableColumnDefinition column) {
						return column.getDefinition();
					}
				}));
	}

	@Override
	public List<ColumnDefinition> getColumnDefinitions() {
		return _columns;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<ProcessedTuple> processTuple(Tuple tuple,
			ErrorReporter errorReporter) {
		String message = tuple.getString(0);
		List<ProcessedTuple> tuples = new ArrayList<ProcessedTuple>();
		// TODO: Also accept single JSON values instead of just arrays.
		List<Map<String, Object>> valueList;
		try {
			valueList = _mapper.readValue(message, List.class);
		} catch (Exception e) {
			errorReporter.reportError(e);
			return tuples;
		}
		for (int i = 0; i < valueList.size(); i++) {
			tuples.add(new JsonTuple(valueList.get(i)));
		}
		return tuples;
	}

	private class JsonTuple extends ProcessedTuple {
		private Map<String, Object> _values;

		public JsonTuple(Map<String, Object> values) {
			_values = values;
		}

		@Override
		public Object getColumnValue(String columnName) {
			Object rawValue = _values.get(columnName);
			if (rawValue == null) {
				return null;
			}
			return _columnsByName.get(columnName).getWritableValue(rawValue);
		}
	}

}
