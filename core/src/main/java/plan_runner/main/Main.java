package plan_runner.main;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import plan_runner.query_plans.QueryPlan;
import plan_runner.query_plans.QueryBuilder;

import plan_runner.components.Component;
import plan_runner.components.theta.ThetaJoinDynamicComponentAdvisedEpochs;
import plan_runner.ewh.components.DummyComponent;

import java.lang.reflect.InvocationTargetException;

import plan_runner.storm_components.StormComponent;
import plan_runner.storm_components.StormJoin;
import plan_runner.storm_components.synchronization.TopologyKiller;
import plan_runner.utilities.MyUtilities;
import plan_runner.utilities.StormWrapper;
import plan_runner.utilities.SystemParameters;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class Main {
	private static Logger LOG = Logger.getLogger(Main.class);

	public static void main(String[] args) {
		new Main(args);
	}

	public Main(String[] args){
		String confPath = args[0];
		Config conf = SystemParameters.fileToStormConfig(confPath);
		QueryBuilder queryPlan = chooseQueryPlan(conf);

		//            conf.put(conf.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 262144);
		//            conf.put(conf.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 262144);
		//            conf.put(conf.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8);
		//            conf.put(conf.TOPOLOGY_TRANSFER_BUFFER_SIZE, 262144);

		addVariablesToMap(conf, confPath);
		putBatchSizes(queryPlan, conf);
		TopologyBuilder builder = createTopology(queryPlan, conf);
		StormWrapper.submitTopology(conf, builder);
	}

	public Main(QueryBuilder queryPlan, Map map, String confPath){
		Config conf = SystemParameters.mapToStormConfig(map);

		addVariablesToMap(conf, confPath);
		putBatchSizes(queryPlan, conf);
		TopologyBuilder builder = createTopology(queryPlan, conf);
		StormWrapper.submitTopology(conf, builder);
	}

	private static void addVariablesToMap(Map map, String confPath){
		//setting topologyName: DIP_TOPOLOGY_NAME_PREFIX + CONFIG_FILE_NAME
		String confFilename = MyUtilities.getPartFromEnd(confPath, 0);
		String prefix = SystemParameters.getString(map, "DIP_TOPOLOGY_NAME_PREFIX");
		String topologyName = prefix + "_" + confFilename;
		SystemParameters.putInMap(map, "DIP_TOPOLOGY_NAME", topologyName);
	}

	//this method is a skeleton for more complex ones
	//  an optimizer should do this in a smarter way
	private static void putBatchSizes(QueryBuilder plan, Map map) {
		if(SystemParameters.isExisting(map, "BATCH_SIZE")){

			//if the batch mode is specified, but nothing is put in map yet (because other than MANUAL_BATCH optimizer is used)
			String firstBatch = plan.getComponentNames().get(0) + "_BS";
			if(!SystemParameters.isExisting(map, firstBatch)){
				String batchSize = SystemParameters.getString(map, "BATCH_SIZE");
				for(String compName: plan.getComponentNames()){
					String batchStr = compName + "_BS";
					SystemParameters.putInMap(map, batchStr, batchSize);
				}
			}

			//no matter where this is set, we print out batch sizes of components
			for(String compName: plan.getComponentNames()){
				String batchStr = compName + "_BS";
				String batchSize = SystemParameters.getString(map, batchStr);
				LOG.info("Batch size for " + compName + " is " + batchSize);
			}
		}
		if(!MyUtilities.checkSendMode(map)){
			throw new RuntimeException("BATCH_SEND_MODE value is not recognized.");
		}
	}

	private static TopologyBuilder createTopology(QueryBuilder qp, Config conf) {
		TopologyBuilder builder = new TopologyBuilder();
		TopologyKiller killer = new TopologyKiller(builder);

		//DST_ORDERING is the optimized version, so it's used by default
		int partitioningType = StormJoin.DST_ORDERING;

		List<Component> queryPlan = qp.getPlan();
		List<String> allCompNames = qp.getComponentNames();
		Collections.sort(allCompNames);
		int planSize = queryPlan.size();
		for(int i=0;i<planSize;i++){
			Component component = queryPlan.get(i);
			Component child = component.getChild();
			if(child == null){
				//a last component (it might be multiple of them)
				component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.FINAL_COMPONENT);
			}else if (child instanceof DummyComponent){
				component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.NEXT_TO_DUMMY);
			}else if(child.getChild() == null && !(child instanceof ThetaJoinDynamicComponentAdvisedEpochs)){
				// if the child is dynamic, then reshuffler is NEXT_TO_LAST
				component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.NEXT_TO_LAST_COMPONENT);
			}else{
				component.makeBolts(builder, killer, allCompNames, conf, partitioningType, StormComponent.INTERMEDIATE);
			}
		}

		// printing infoID information and returning the result
		//printInfoID(killer, queryPlan); commented out because IDs are now desriptive names
		return builder;
	}

	private static void printInfoID(TopologyKiller killer,
			List<Component> queryPlan) {

		StringBuilder infoID = new StringBuilder("\n");
		if(killer!=null){
			infoID.append(killer.getInfoID());
			infoID.append("\n");
		}
		infoID.append("\n");

		// after creating bolt, ID of a component is known
		int planSize = queryPlan.size();
		for(int i=0;i<planSize;i++){
			Component component = queryPlan.get(i);
			infoID.append(component.getInfoID());
			infoID.append("\n\n");
		}

		LOG.info(infoID.toString());
	}


	public static QueryBuilder chooseQueryPlan(Map conf){
		String queryName = SystemParameters.getString(conf, "DIP_QUERY_NAME");
		//if "/" is the last character, adding one more is not a problem
		String dataPath = SystemParameters.getString(conf, "DIP_DATA_PATH") + "/";
		String extension = SystemParameters.getString(conf, "DIP_EXTENSION");
		boolean isMaterialized = SystemParameters.isExisting(conf, "DIP_MATERIALIZED") && SystemParameters.getBoolean(conf, "DIP_MATERIALIZED");
		boolean isOkcanSampling = SystemParameters.isExisting(conf, "DIP_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_SAMPLING");
		boolean isEWHSampling = SystemParameters.isExisting(conf, "DIP_EWH_SAMPLING") && SystemParameters.getBoolean(conf, "DIP_EWH_SAMPLING");

		QueryBuilder queryPlan = null;
		/*
            if(isSampling && isMaterialized){
            	// still can be used when the query plan code is not adjusted, and the query is materialized
            	queryPlan = new OkcanSampleMatrixPlan(dataPath, extension, conf).getQueryPlan();
            }else if (isEWHSampling && isMaterialized){
            	// still can be used when the query plan code is not adjusted, and the query is materialized
            	queryPlan = new EWHSampleMatrixPlan(dataPath, extension, conf).getQueryPlan();
            } else{
		 */
		// change between this and ...
                String className = SystemParameters.getString(conf, "DIP_QUERY_PLAN");

                if (className == null) {
                  throw new RuntimeException("QueryPlan " + queryName + " failed to load: DIP_QUERY_PLAN was not defined.");
                }
                try {
                  Class planClass = Class.forName(className);
                  queryPlan = ((QueryPlan)(planClass.getConstructor(String.class,
                                                                    String.class,
                                                                    Map.class)).newInstance(dataPath, extension, conf)).getQueryPlan();
                } catch (InstantiationException e) {
                  LOG.info("Could not instantiate class" + className);
                } catch (IllegalAccessException e) {
                  LOG.info("Could not access class" + className);
                } catch (ClassNotFoundException e) {
                  LOG.info("Could not find class " + className);
                } catch (NoSuchMethodException e) {
                  LOG.info("Class " + className + " doesn't have an appropriate constructor");
                } catch (InvocationTargetException e) {
                  LOG.info("The constructor for " + className + " threw an exception");
                }

		// ... this line
		if (queryPlan == null){
			throw new RuntimeException("QueryPlan " + queryName + " failed to load.");
		}
		return queryPlan;
	}
}
