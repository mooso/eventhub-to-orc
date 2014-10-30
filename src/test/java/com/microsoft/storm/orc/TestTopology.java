package com.microsoft.storm.orc;

import java.util.*;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.*;

import backtype.storm.*;
import backtype.storm.generated.*;
import backtype.storm.spout.*;
import backtype.storm.task.*;
import backtype.storm.topology.*;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.*;
import backtype.storm.utils.Utils;

class TestTopology {

	public static class TestSchema extends OrcTupleSchema {
		private static final long serialVersionUID = -3978395779741259772L;

		@Override
		public List<ColumnDefinition> getColumnDefinitions() {
			ArrayList<ColumnDefinition> columns = new ArrayList<ColumnDefinition>();
			columns.add(new ColumnDefinition("a", TypeInfoFactory.stringTypeInfo));
			columns.add(new ColumnDefinition("b", TypeInfoFactory.intTypeInfo));
			return columns;
		}

		@SuppressWarnings("serial")
		@Override
		public List<ProcessedTuple> processTuple(final Tuple tuple, ErrorReporter errorReporter) {
			return new ArrayList<ProcessedTuple>() {{
				add(new TestTuple(tuple));
			}};
		}

		private static class TestTuple extends ProcessedTuple {
			private final Tuple _tuple;

			public TestTuple(Tuple tuple) {
				_tuple = tuple;
			}

			@Override
			public Object getColumnValue(String columnName) {
				switch (columnName) {
				case "a":
					return new Text(_tuple.getString(0));
				case "b":
					return new IntWritable(_tuple.getInteger(1));
				default:
					return null;
				}
			}
		}
	}

	public static class TestBolt extends OrcBolt {
		private static final long serialVersionUID = -2496970569143421945L;

		public TestBolt(String outputDirectory, String tempDirectory) {
			super(outputDirectory, tempDirectory, 2);
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected OrcTupleSchema createSchema(Map stormConf) {
			return new TestSchema();
		}
	}

	public static void main(String[] args) throws Exception {
		StormTopology topology = createTestTopology();
		Config conf = configure();
		if (Arrays.asList(args).contains("-local")) {
			runLocally(topology, conf);
		} else {
			StormSubmitter.submitTopology("test", conf, topology);
		}
	}

	private static StormTopology createTestTopology() {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("source", new TestSpout(), 1);
		builder.setBolt("orc", new TestBolt("/fromStorm", "/temp"), 1)
				.shuffleGrouping("source");
		StormTopology toplogy = builder.createTopology();
		return toplogy;
	}

	private static void runLocally(StormTopology topology, Config conf) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, topology);
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();
	}

	private static Config configure() {
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		return conf;
	}

	static class TestSpout extends BaseRichSpout {
		private static final long serialVersionUID = 1L;
		private SpoutOutputCollector _collector;

		@Override
		public void nextTuple() {
			Utils.sleep(100);
			final String[] words = new String[] { "my", "test", "words", "are", "so",
					"original" };
			final Random rand = new Random();
			final String word = words[rand.nextInt(words.length)];
			_collector.emit(new Values(word, rand.nextInt(1000)));
		}

		@SuppressWarnings("rawtypes")
		@Override
		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			_collector = collector;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("a", "b"));
		}
	}
}
