package com.microsoft.storm.orc;

import java.io.FileReader;
import java.util.*;

import org.slf4j.*;

import backtype.storm.*;
import backtype.storm.generated.*;
import backtype.storm.topology.*;
import backtype.storm.utils.*;

import com.microsoft.eventhubs.spout.*;

public class StandardJsonEventHubToOrcTopology {
	@SuppressWarnings("unused")
	private static final Logger LOG = LoggerFactory.getLogger(StandardJsonEventHubToOrcTopology.class);

	private final static class JsonOrcBolt extends OrcBolt {
		private static final long serialVersionUID = -5551938938243538834L;
		private final String _columnDefinitionLine;

		public JsonOrcBolt(String outputDirectory, String tempDirectory,
				int tuplesPerCycle, String columnDefinitionLine) {
			super(outputDirectory, tempDirectory, tuplesPerCycle);
			_columnDefinitionLine = columnDefinitionLine;
		}

		@SuppressWarnings("rawtypes")
		@Override
		protected OrcTupleSchema createSchema(Map stormConf) {
			return OrcTupleSchemaFactory.createJsonSchemaFromColumnDefinition(
					_columnDefinitionLine);
		}
	}

	public static void main(String[] args) throws Exception {
		boolean runLocal = Arrays.asList(args).contains("-local");
		Properties properties = new Properties();
		if (args.length > 0) {
			properties.load(new FileReader(args[0]));
		} else {
			properties.load(StandardJsonEventHubToOrcTopology.class.getClassLoader().getResourceAsStream(
					"Config.properties"));
		}
		EventHubSpoutConfig spoutConfig = readEHConfig(properties);
		StormTopology topology = createEventHubToOrcTopology(properties, spoutConfig);
		String topologyname = properties.getProperty("storm.orc.topology.name", "JsonOrc");
		Config conf = configure(properties, spoutConfig);
		if (runLocal) {
			runLocally(conf, topology, topologyname);
		} else {
			runRemote(conf, topology, topologyname);
		}
	}

	private static StormTopology createEventHubToOrcTopology(Properties properties,
			EventHubSpoutConfig spoutConfig) throws Exception {
		// Read configuration
		String columnDefinitionLine = properties.getProperty("storm.orc.columns.definition");
		if (columnDefinitionLine == null) {
			throw new IllegalArgumentException("Undefined storm.orc.columns.definition");
		}
		String outputDirectory = properties.getProperty("storm.orc.output.directory",
				"/fromStormToOrc");
		String tempDirectory = properties.getProperty("storm.orc.temp.directory", "/temp");
		int partitionsPerSpoutExecutor = Integer.parseInt(
				properties.getProperty("storm.orc.partitions.per.spout.executor", "1"));
		int partitionsPerOrcExecutor = Integer.parseInt(
				properties.getProperty("storm.orc.partitions.per.orc.executor", "1"));
		int tuplesPerCycle = Integer.parseInt(
				properties.getProperty("storm.orc.tuples.per.cycle", "300000"));

		// Build the topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("source", new EventHubSpout(spoutConfig),
				spoutConfig.getPartitionCount() / partitionsPerSpoutExecutor);
		builder.setBolt("toorc",
				new JsonOrcBolt(outputDirectory, tempDirectory, tuplesPerCycle, columnDefinitionLine),
				spoutConfig.getPartitionCount() / partitionsPerOrcExecutor)
			.shuffleGrouping("source");
		return builder.createTopology();
	}

	private static Config configure(Properties properties, EventHubSpoutConfig spoutConfig) {
		Config conf = new Config();
		conf.setDebug(Boolean.parseBoolean(
				properties.getProperty("storm.orc.debug", "false")));
		conf.setNumWorkers(Integer.parseInt(
				properties.getProperty("storm.orc.num.workers",
						"" + (2 * spoutConfig.getPartitionCount()))));
		return conf;
	}

	private static void runRemote(Config conf, StormTopology topology, String topologyName)
			throws Exception {
		deleteTopologyIfExists(conf, topologyName);
		StormSubmitter.submitTopology(topologyName, conf, topology);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static void deleteTopologyIfExists(HashMap stormConf, String name) throws Exception {
		stormConf = new HashMap(stormConf);
		stormConf.putAll(Utils.readCommandLineOpts());
		Map conf = Utils.readStormConfig();
		conf.putAll(stormConf);
		NimbusClient client = NimbusClient.getConfiguredClient(conf);
		try {
			ClusterSummary summary = client.getClient().getClusterInfo();
			for(TopologySummary s : summary.get_topologies()) {
				if(s.get_name().equals(name)) {
					client.getClient().killTopology(name);
					return;
				}
			}
		} finally {
			client.close();
		}
	}

	private static void runLocally(Config conf, StormTopology topology, String topologyName) {
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology(topologyName, conf, topology);
		Utils.sleep(100000);
		cluster.killTopology(topologyName);
		System.out.println("Shutting down...");
		cluster.shutdown();
	}

	private static EventHubSpoutConfig readEHConfig(Properties properties)
			throws Exception {
		String username = properties.getProperty("eventhubspout.username");
		String password = properties.getProperty("eventhubspout.password");
		String namespaceName = properties.getProperty("eventhubspout.namespace");
		String entityPath = properties.getProperty("eventhubspout.entitypath");
		String zkEndpointAddress = properties
				.getProperty("zookeeper.connectionstring");
		int partitionCount = Integer.parseInt(properties
				.getProperty("eventhubspout.partitions.count"));
		int checkpointIntervalInSeconds = Integer.parseInt(properties
				.getProperty("eventhubspout.checkpoint.interval"));
		return new EventHubSpoutConfig(username, password, namespaceName,
				entityPath, partitionCount, zkEndpointAddress,
				checkpointIntervalInSeconds, 1000);
	}
}
