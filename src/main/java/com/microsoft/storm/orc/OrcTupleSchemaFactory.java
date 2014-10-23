package com.microsoft.storm.orc;

import java.util.*;

public class OrcTupleSchemaFactory {
	public static JsonOrcTupleSchema createJsonSchemaFromColumnDefinition(String columnDefinitionLine) {
		String[] columns = columnDefinitionLine.split("\\s*,\\s*");
		List<WritableColumnDefinition> columnDefinitions = new ArrayList<WritableColumnDefinition>(columns.length);
		for (String column: columns) {
			String[] nameAndType = column.split("\\s");
			if (nameAndType.length != 2) {
				throw new IllegalArgumentException("Invalid column definition line: " + columnDefinitionLine);
			}
			WritableColumnDefinition currentDefinition;
			String name = nameAndType[0];
			String type = nameAndType[1].toLowerCase();
			switch (type) {
			case "int": currentDefinition = new IntWritableColumnDefinition(name); break;
			case "string": currentDefinition = new TextColumnDefinition(name); break;
			case "double": currentDefinition = new DoubleWritableColumnDefinition(name); break;
			default: throw new IllegalArgumentException("Unknown type: " + type);
			}
			columnDefinitions.add(currentDefinition);
		}
		return new JsonOrcTupleSchema(columnDefinitions);
	}
}
