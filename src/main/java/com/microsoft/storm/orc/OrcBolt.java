package com.microsoft.storm.orc;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hive.ql.io.orc.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.slf4j.*;

import backtype.storm.task.*;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.*;

public abstract class OrcBolt extends BaseRichBolt {
	private static final long serialVersionUID = 2311793202424841136L;
	private static final Logger LOG = LoggerFactory.getLogger(OrcBolt.class);

	private OutputCollector _collector;
	private SettableStructObjectInspector _inspector;
	private Writer _orcWriter;
	private String _outputDirectory;
	private String _tempDirectory;
	private OrcTupleSchema _schema;
	private List<OrcTupleSchema.ColumnDefinition> _columnDefinitions;
	private ErrorReporter _errorReporter;
	private int _tuplesPerCycle;
	private int _tuplesInCurrentCycle = 0;
	private int _currentCycleIndex = 0;
	private UUID _uniqueId;
	private OrcFile.WriterOptions _writerOptions;
	private Path _currentTempPath;
	private Path _currentOutputPath;
	private FileSystem _outputFileSystem;

	public OrcBolt(String outputDirectory, String tempDirectory, int tuplesPerCycle) {
		_outputDirectory = outputDirectory;
		_tempDirectory = tempDirectory;
		_tuplesPerCycle = tuplesPerCycle;
	}

	@Override
	public void execute(Tuple tuple) {
		cycleWriterIfNeeded();
		if (_orcWriter == null) {
			_collector.fail(tuple);
			return;
		}
		List<Object> rows = createRows(tuple);
		for (Object row : rows) {
			try {
				_orcWriter.addRow(row);
				_collector.ack(tuple);
				_tuplesInCurrentCycle++;
			} catch (IOException e) {
				_collector.reportError(e);
				_collector.fail(tuple);
			}
		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
		_errorReporter = new ErrorReporter() {
			@Override
			public void reportError(Throwable error) {
				_collector.reportError(error);
			}
		};
		Configuration conf = new Configuration();
		try {
			_outputFileSystem = FileSystem.get(new URI(_outputDirectory), conf);
			Path outputDirectoryPath = new Path(_outputDirectory);
			if (!_outputFileSystem.exists(outputDirectoryPath)) {
				if (!_outputFileSystem.mkdirs(new Path(_outputDirectory))) {
					throw new IOException("Failed to create output directory: " + _outputDirectory);
				}
			}
		} catch (Exception ex) {
			_collector.reportError(ex);
		}
		_uniqueId = UUID.randomUUID();
		_writerOptions = OrcFile.writerOptions(conf);
		_schema = createSchema(stormConf);
		_columnDefinitions = _schema.getColumnDefinitions();
		_inspector = configureObjectInspector(_columnDefinitions);
		_writerOptions.inspector(_inspector);
	}

	private void cycleWriterIfNeeded() {
		if (_orcWriter == null || _tuplesInCurrentCycle > _tuplesPerCycle) {
			_currentCycleIndex++;
			_tuplesInCurrentCycle = 0;
			if (_orcWriter != null) {
				try {
					_orcWriter.close();
					LOG.info("Renaming " + _currentTempPath + " to " + _currentOutputPath);
					if (!_outputFileSystem.rename(_currentTempPath, _currentOutputPath)) {
						throw new IOException(
								"Failed to rename " + _currentTempPath + " to " + _currentOutputPath);
					}
				} catch (IOException e) {
					_collector.reportError(e);
				}
				_orcWriter = null;
			}
			String fileName = "Task_" +
					_uniqueId + "_" + _currentCycleIndex;
			_currentOutputPath = new Path(_outputDirectory, fileName);
			_currentTempPath = new Path(_tempDirectory, fileName);
			try {
				_orcWriter = OrcFile.createWriter(_currentTempPath, _writerOptions);
			} catch (IOException e) {
				_collector.reportError(e);
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@SuppressWarnings("rawtypes")
	protected abstract OrcTupleSchema createSchema(Map stormConf);

	@Override
	public void cleanup() {
		super.cleanup();
		try {
			if (_orcWriter != null) {
				_orcWriter.close();
				_outputFileSystem.rename(_currentTempPath, _currentOutputPath);
			}
		} catch (IOException e) {
			_collector.reportError(e);
		}
	}

	private List<Object> createRows(Tuple tuple) {
		List<OrcTupleSchema.ProcessedTuple> processedTuples =
				_schema.processTuple(tuple, _errorReporter);
		List<Object> rows = new ArrayList<Object>(processedTuples.size());
		for (OrcTupleSchema.ProcessedTuple processedTuple : processedTuples) {
			Object row = _inspector.create();
			for (OrcTupleSchema.ColumnDefinition column : _columnDefinitions) {
				StructField field = _inspector.getStructFieldRef(column.getColumnName());
				Object value = processedTuple.getColumnValue(column.getColumnName());
				_inspector.setStructFieldData(row, field, value);
			}
			rows.add(row);
		}
		return rows;
	}

	private SettableStructObjectInspector configureObjectInspector(
			List<OrcTupleSchema.ColumnDefinition> columnDefinitions) {
		ArrayList<String> columnNames = new ArrayList<String>();
		ArrayList<TypeInfo> columnTypes = new ArrayList<TypeInfo>();
		for (OrcTupleSchema.ColumnDefinition column : columnDefinitions) {
			columnNames.add(column.getColumnName());
			columnTypes.add(column.getColumnType());
		}
		TypeInfo rootType = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
		SettableStructObjectInspector inspector =
				(SettableStructObjectInspector)OrcStruct.createObjectInspector(rootType);
		return inspector;
	}
}