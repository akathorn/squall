/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import ch.epfl.data.squall.storm_components.hyper_cube.stream_grouping.HyperCubeGrouping;
import ch.epfl.data.squall.thetajoin.matrix_assignment.HyperCubeAssignment;
import org.apache.log4j.Logger;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.Grouping;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.epfl.data.squall.components.Component;
import ch.epfl.data.squall.components.DataSourceComponent;
import ch.epfl.data.squall.components.theta.ThetaJoinComponent;
import ch.epfl.data.squall.expressions.ValueExpression;
import ch.epfl.data.squall.operators.AggregateOperator;
import ch.epfl.data.squall.operators.ProjectOperator;
import ch.epfl.data.squall.operators.SampleOperator;
import ch.epfl.data.squall.operators.Operator;
import ch.epfl.data.squall.predicates.ComparisonPredicate;
import ch.epfl.data.squall.query_plans.QueryBuilder;
import ch.epfl.data.squall.storm_components.StormComponent;
import ch.epfl.data.squall.storm_components.StormEmitter;
import ch.epfl.data.squall.storm_components.StormSrcHarmonizer;
import ch.epfl.data.squall.storm_components.stream_grouping.BatchStreamGrouping;
import ch.epfl.data.squall.storm_components.stream_grouping.HashStreamGrouping;
import ch.epfl.data.squall.storm_components.stream_grouping.ShuffleStreamGrouping;
import ch.epfl.data.squall.storm_components.theta.stream_grouping.ContentInsensitiveThetaJoinGrouping;
import ch.epfl.data.squall.storm_components.theta.stream_grouping.ContentSensitiveThetaJoinGrouping;
import ch.epfl.data.squall.thetajoin.matrix_assignment.ContentSensitiveMatrixAssignment;
import ch.epfl.data.squall.thetajoin.matrix_assignment.MatrixAssignment;
import ch.epfl.data.squall.types.DateIntegerType;
import ch.epfl.data.squall.types.DoubleType;
import ch.epfl.data.squall.types.NumericType;
import ch.epfl.data.squall.types.Type;
import ch.epfl.data.squall.utilities.SystemParameters.HistogramType;
import ch.epfl.data.squall.window_semantics.WindowSemanticsManager;

public class MyUtilities {
    private static void addSampleOp(Component parent, ProjectOperator project,
	    int relSize, int numLastJoiners, Map conf) {
	// hash is always 0 as the key is there
	List<Integer> hash = new ArrayList<Integer>(Arrays.asList(0));

	SampleOperator sample = new SampleOperator(relSize, numLastJoiners);
	parent.add(sample).add(project).setOutputPartKey(hash);
    }

    /*
     * Necessary to avoid the last bucket to be very large in
     * EWHSampleMatrixBolt.createBoundaries For example, if relSize = 12000 and
     * numOfBuckets = 6001, EWHSampleMatrixBolt.createBoundaries creates 6000
     * buckets of size 1 and 1 bucket of size 6000 The last bucket is 6000X
     * bigger than any previous bucket Here, we ensure that the last bucket is
     * at most 2X larger than any previous bucket
     * 
     * numOfBuckets = n_s
     * 
     * This method can also be invoked for (n_s, p)
     */
    public static int adjustPartitioning(int relSize, int numOfBuckets,
	    String parameterName) {
	if (relSize % numOfBuckets != 0) {
	    // roof of bucketSize: increasing bucketSize reduces numOfBuckets
	    int bucketSize = (relSize / numOfBuckets) + 1;
	    if (numOfBuckets >= bucketSize) {
		// Meant for the case numOfBuckets >= bucketSize.
		// If that's not the case, no need for any adjustment.

		/*
		 * relSize and bucketSize are integers; newBucketSize is double
		 * of form x.y We have x buckets of size bucketSize; the last
		 * bucket has 0.y * bucketSize, and this is at most bucketSize
		 * Hence, the last bucket is at most two times larger than the
		 * previous buckets
		 */
		int newNumOfBuckets = relSize / bucketSize;

		LOG.info("Reducing the size of " + parameterName + " from "
			+ numOfBuckets + " to " + newNumOfBuckets);
		/*
		 * if(numOfBuckets > 1.15 * newNumOfBuckets){ throw new
		 * RuntimeException(
		 * "No need to fix the code: just keep in mind that you reduced the number of buckets for more than 15%"
		 * ); }
		 */

		numOfBuckets = newNumOfBuckets;
	    }
	}
	return numOfBuckets;
    }

    public static InputDeclarer attachEmitterBatch(Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID,
			new BatchStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterBroadcast(String streamId,
	    InputDeclarer currentBolt, String emitterId1,
	    String... emitterIdArray) {
	final List<String> emittersIdsList = new ArrayList<String>();
	emittersIdsList.add(emitterId1);
	emittersIdsList.addAll(Arrays.asList(emitterIdArray));

	for (final String emitterId : emittersIdsList) {
	    currentBolt = currentBolt.allGrouping(emitterId, streamId);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterBroadcast(InputDeclarer currentBolt, List<StormEmitter> emitters) {
        for (StormEmitter e : emitters) {
            for (String emitterId : e.getEmitterIDs()) {
                currentBolt = currentBolt.allGrouping(emitterId);
            }
        }
        return currentBolt;
    }

    public static InputDeclarer attachEmitterHash(Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID,
			new HashStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterHash(String streamId, Map map,
	    List<String> fullHashList, InputDeclarer currentBolt,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, streamId,
			new HashStreamGrouping(map, fullHashList));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterShuffle(Map map,
	    InputDeclarer currentBolt, String emitterId1,
	    String... emitterIdArray) {
	final List<String> emittersIdsList = new ArrayList<String>();
	emittersIdsList.add(emitterId1);
	emittersIdsList.addAll(Arrays.asList(emitterIdArray));

	for (final String emitterId : emittersIdsList) {
	    currentBolt = currentBolt.customGrouping(emitterId,
		    new ShuffleStreamGrouping(map));
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterToSingle(
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.globalGrouping(emitterID);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterToSingle(String streamId,
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.globalGrouping(emitterID, streamId);
	}
	return currentBolt;
    }

    public static InputDeclarer attachEmitterStarSchema(Map map,
        InputDeclarer currentBolt, List<StormEmitter> emitters,
        long[] cardinality) {
        String starEmitterName = emitters.get(0).getName();
        long largestCardinality = cardinality[0];
        // find the starEmitter as the one with largest cardinality
        for (int i = 1; i < emitters.size(); i++) {
            if (cardinality[i] > largestCardinality) {
                largestCardinality = cardinality[i];
                starEmitterName = emitters.get(i).getName();
            }
        }

        for (final StormEmitter emitter : emitters) {
            final String[] emitterIDs = emitter.getEmitterIDs();
            if (emitter.getName().equals(starEmitterName)) {
                for (final String emitterID : emitterIDs)
                    currentBolt = currentBolt.customGrouping(emitterID, new ShuffleStreamGrouping(map));
            } else {
                for (final String emitterID : emitterIDs) {
                    currentBolt = currentBolt.allGrouping(emitterID);
                }
            }
        }

        return currentBolt;
    }


    public static InputDeclarer attachEmitterHyperCube(
            InputDeclarer currentBolt, List<StormEmitter> emitters, List<String> allCompNames,
            HyperCubeAssignment assignment, Map map) {


        String[] emitterIndexes = new String[emitters.size()];
        for (int i = 0; i < emitterIndexes.length; i++)
            emitterIndexes[i] = String.valueOf(allCompNames
                    .indexOf(emitters.get(i).getName()));

        CustomStreamGrouping mapping = new HyperCubeGrouping(emitterIndexes, assignment, map);

        for (final StormEmitter emitter : emitters) {
            final String[] emitterIDs = emitter.getEmitterIDs();
            for (final String emitterID : emitterIDs)
                currentBolt = currentBolt.customGrouping(emitterID, mapping);
        }
        return currentBolt;
    }

    public static void checkBatchOutput(long batchOutputMillis,
	    AggregateOperator aggregation, Map conf) {
	if (aggregation == null && batchOutputMillis != 0)
	    throw new RuntimeException(
		    "A component must have aggregation operator in order to support batching.");
	if (isAckEveryTuple(conf) && batchOutputMillis != 0)
	    throw new RuntimeException(
		    "With batching, only AckAtEnd mode is allowed!");
	// we don't keep Storm Tuple instances for batched tuples
	// we also ack them immediately, which doesn't fir in AckEveryTime
	// logic
    }

    public static void checkIIPrecValid(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	List<String> values = new ArrayList<String>(Arrays.asList("DENSE",
		"PWEIGHT", "BOTH"));
	if (SystemParameters.isExisting(conf, key)) {
	    String value = SystemParameters.getString(conf, key);
	    if (!values.contains(value)) {
		throw new RuntimeException(
			"Unsupported value for SECOND_PRECOMPUTATION = "
				+ value);
	    }
	}
    }

    public static boolean checkSendMode(Map map) {
	if (SystemParameters.isExisting(map, "BATCH_SEND_MODE")) {
	    final String mode = SystemParameters.getString(map,
		    "BATCH_SEND_MODE");
	    if (!mode.equalsIgnoreCase("THROTTLING")
		    && !mode.equalsIgnoreCase("SEND_AND_WAIT")
		    && !mode.equalsIgnoreCase("MANUAL_BATCH"))
		return false;
	}
	return true;
    }

    public static int chooseBalancedTargetIndex(String hash,
	    List<String> allHashes, int targetParallelism) {
	return allHashes.indexOf(hash) % targetParallelism;
    }

    public static int chooseHashTargetIndex(String hash, int targetParallelism) {
	return Math.abs(hash.hashCode()) % targetParallelism;
    }

    private static double computeDiffPercent(double first, double second) {
	double bigger = first > second ? first : second;
	double smaller = first < second ? first : second;
	return (bigger / smaller - 1) * 100;
    }

    public static double computePercentage(int smaller, int bigger) {
	int diff = bigger - smaller; // always positive
	return ((double) diff) / bigger;
    }

    public static String createHashString(List<String> tuple,
	    List<Integer> hashIndexes, List<ValueExpression> hashExpressions,
	    Map map) {
	if (hashIndexes == null && hashExpressions == null)
	    return SINGLE_HASH_KEY;

	final String columnDelimiter = getColumnDelimiter(map);

	// NOTE THAT THE HASHCOLUMN depend upon the output of the projection!!
	final StringBuilder hashStrBuf = new StringBuilder();
	if (hashIndexes != null)
	    for (final int hashIndex : hashIndexes)
		hashStrBuf.append(tuple.get(hashIndex)).append(columnDelimiter);
	if (hashExpressions != null)
	    for (final ValueExpression hashExpression : hashExpressions)
		hashStrBuf.append(hashExpression.eval(tuple)).append(
			columnDelimiter);

	// remove one extra HASH_DELIMITER at the end

	final int hdLength = columnDelimiter.length();
	final int fullLength = hashStrBuf.length();
	return hashStrBuf.substring(0, fullLength - hdLength);

    }

    public static String createHashString(List<String> tuple,
	    List<Integer> hashIndexes, Map map) {
	if (hashIndexes == null || hashIndexes.isEmpty())
	    return SINGLE_HASH_KEY;
	String hashString = "";
	final int tupleLength = hashIndexes.size();
	for (int i = 0; i < tupleLength; i++)
	    // depend upon the output of the
	    // projection!!
	    if (i == tupleLength - 1)
		hashString += tuple.get(hashIndexes.get(i));
	    else
		hashString += tuple.get(hashIndexes.get(i))
			+ getColumnDelimiter(map);
	return hashString;
    }

    /*
     * For each emitter component (there are two input emitters for each join),
     * appropriately connect with all of its inner Components that emits tuples
     * to StormDestinationJoin. For destinationJoiner, there is only one bolt
     * that emits tuples, but for sourceJoiner, there are two SourceStorage (one
     * for storing each emitter tuples), which emits tuples.
     */
    /*
     * public static InputDeclarer attachEmitterComponents(InputDeclarer
     * currentBolt, StormEmitter emitter1, StormEmitter... emittersArray){
     * List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
     * emittersList.add(emitter1);
     * emittersList.addAll(Arrays.asList(emittersArray)); for(StormEmitter
     * emitter: emittersList){ String[] emitterIDs = emitter.getEmitterIDs();
     * for(String emitterID: emitterIDs){ currentBolt =
     * currentBolt.fieldsGrouping(emitterID, new Fields("Hash")); } } return
     * currentBolt; }
     */

    public static List<String> createOutputTuple(List<String> firstTuple,
	    List<String> secondTuple) {
	final List<String> outputTuple = new ArrayList<String>();

	for (int j = 0; j < firstTuple.size(); j++)
	    // first relation (R)
	    outputTuple.add(firstTuple.get(j));
	for (int j = 0; j < secondTuple.size(); j++)
	    outputTuple.add(secondTuple.get(j));
	return outputTuple;
    }

    public static List<String> createOutputTuple(List<String> firstTuple,
	    List<String> secondTuple, List<Integer> joinParams) {
	final List<String> outputTuple = new ArrayList<String>();

	for (int j = 0; j < firstTuple.size(); j++)
	    // first relation (R)
	    outputTuple.add(firstTuple.get(j));
	for (int j = 0; j < secondTuple.size(); j++)
	    if ((joinParams == null) || (!joinParams.contains(j)))
		// not
		// exits
		// add
		// the
		// column!!
		// (S)
		outputTuple.add(secondTuple.get(j));
	return outputTuple;
    }

    public static List<String> createOutputTuple(List<List<String>> tuples) {
        final List<String> outputTuple = new ArrayList<String>();

        for (List<String> tpI : tuples) {
            for (String coulumnJ : tpI) {
                outputTuple.add(coulumnJ);
            }
        }

        return outputTuple;
    }

    public static Values createRelSizeTuple(String componentIndex, int relSize) {
	Values relSizeTuple = new Values();
	relSizeTuple.add(componentIndex);
	List<String> tuple = new ArrayList<String>(Arrays.asList(
		SystemParameters.REL_SIZE, String.valueOf(relSize)));
	relSizeTuple.add(tuple);
	relSizeTuple.add("O"); // does not matter as we send to a single
			       // Partitioner node
	return relSizeTuple;
    }

    public static Values createTotalOutputSizeTuple(String componentIndex,
	    long totalOutputSize) {
	Values totalOutputSizeTuple = new Values();
	totalOutputSizeTuple.add(componentIndex);
	List<String> tuple = new ArrayList<String>(Arrays.asList(
		SystemParameters.TOTAL_OUTPUT_SIZE,
		String.valueOf(totalOutputSize)));
	totalOutputSizeTuple.add(tuple);
	totalOutputSizeTuple.add("O"); // does not matter as we send to a single
				       // Partitioner node
	return totalOutputSizeTuple;
    }

    public static Values createTupleValues(List<String> tuple, long timestamp,
	    String componentIndex, List<Integer> hashIndexes,
	    List<ValueExpression> hashExpressions, Map conf) {

	final String outputTupleHash = MyUtilities.createHashString(tuple,
		hashIndexes, hashExpressions, conf);
	if (MyUtilities.isCustomTimestampMode(conf)
		|| MyUtilities.isWindowTimestampMode(conf))
	    return new Values(componentIndex, tuple, outputTupleHash, timestamp);
	else
	    return new Values(componentIndex, tuple, outputTupleHash);
    }

    public static Values createUniversalFinalAckTuple(Map map) {
	final Values values = new Values();
	values.add("N/A");
	if (!MyUtilities.isManualBatchingMode(map)) {
	    final List<String> lastTuple = new ArrayList<String>(
		    Arrays.asList(SystemParameters.LAST_ACK));
	    values.add(lastTuple);
	    values.add("N/A");
	} else
	    values.add(SystemParameters.LAST_ACK);
	if (MyUtilities.isCustomTimestampMode(map)
		|| MyUtilities.isWindowTimestampMode(map))
	    values.add(0);
	return values;
    }

    public static void dumpSignal(StormComponent comp, Tuple stormTupleRcv,
	    OutputCollector collector) {
	comp.printContent();
	collector.ack(stormTupleRcv);
    }

    private static String extractSizeSkew(String dataPath) {
	String parts[] = dataPath.split("/");
	int size = parts.length;
	if (size == 1) {
	    return parts[0];
	} else {
	    return parts[size - 2] + "_" + parts[size - 1];
	}
    }

    /*
     * Different tuple<->(String, Hash) conversions
     */
    public static List<String> fileLineToTuple(String line, Map conf) {
	final String[] columnValues = line.split(SystemParameters.getString(
		conf, "DIP_READ_SPLIT_DELIMITER"));
	return new ArrayList<String>(Arrays.asList(columnValues));
    }

    // collects all the task ids for "default" stream id
    public static List<Integer> findTargetTaskIds(TopologyContext tc) {
	final List<Integer> result = new ArrayList<Integer>();
	final Map<String, Map<String, Grouping>> streamComponentGroup = tc
		.getThisTargets();
	final Iterator<Entry<String, Map<String, Grouping>>> it = streamComponentGroup
		.entrySet().iterator();
	while (it.hasNext()) {
	    final Map.Entry<String, Map<String, Grouping>> pair = it.next();
	    final String streamId = pair.getKey();
	    final Map<String, Grouping> componentGroup = pair.getValue();
	    if (streamId.equalsIgnoreCase("default")) {
		final Iterator<Entry<String, Grouping>> innerIt = componentGroup
			.entrySet().iterator();
		while (innerIt.hasNext()) {
		    final Map.Entry<String, Grouping> innerPair = innerIt
			    .next();
		    final String componentId = innerPair.getKey();
		    // Grouping group = innerPair.getValue();
		    // if (group.is_set_direct()){
		    result.addAll(tc.getComponentTasks(componentId));
		    // }
		}
	    }
	}
	return result;
    }

    // Previously HASH_DELIMITER = "-" in SystemParameters, but now is the same
    // as DIP_GLOBAL_ADD_DELIMITER
    // we need it for preaggregation
    public static String getColumnDelimiter(Map map) {
	return SystemParameters.getString(map, "DIP_GLOBAL_ADD_DELIMITER");
    }

    public static int getCompBatchSize(String compName, Map map) {
	return SystemParameters.getInt(map, compName + "_BS");
    }

    public static Type getDominantNumericType(List<ValueExpression> veList) {
	Type wrapper = veList.get(0).getType();
	for (int i = 1; i < veList.size(); i++) {
	    final Type currentType = veList.get(1).getType();
	    if (isDominant(currentType, wrapper))
		wrapper = currentType;
	}
	return wrapper;
    }

    public static String getHistogramFilename(Map conf, int numJoiners,
	    String filePrefix) {
	return getHistogramFilename(conf, filePrefix) + "_" + numJoiners + "j";
    }

    public static String getHistogramFilename(Map conf, String filePrefix) {
	String queryId = MyUtilities.getQueryID(conf);
	return SystemParameters.getString(conf, HistogramType.ROOT_DIR) + "/"
		+ filePrefix + "_" + queryId;
    }

    public static String getKeyRegionFilename(Map conf) {
	String queryId = MyUtilities.getQueryID(conf);
	return SystemParameters.getString(conf, "DIP_KEY_REGION_ROOT") + "/"
		+ queryId;
    }

    public static String getKeyRegionFilename(Map conf, String shortName) {
	return getKeyRegionFilename(conf) + "_" + shortName;
    }

    public static String getKeyRegionFilename(Map conf, String shortName,
	    int numJoiners) {
	return getKeyRegionFilename(conf, shortName) + "_" + numJoiners + "j";
    }

    public static String getKeyRegionFilename(Map conf, String shortName,
	    int numJoiners, int numBuckets) {
	return getKeyRegionFilename(conf, shortName, numJoiners) + "_"
		+ numBuckets + "b";
    }

    public static long getM(int firstRelSize, int secondRelSize, Map conf) {
	if (!SystemParameters.isExisting(conf, "M_DESCRIPTOR")) {
	    throw new RuntimeException(
		    "M_DESCRIPTOR does not exist in the config file!");
	}

	String mDescriptor = SystemParameters.getString(conf, "M_DESCRIPTOR");
	if (mDescriptor.equalsIgnoreCase("M")) {
	    return SystemParameters.getLong(conf, "TOTAL_OUTPUT_SIZE");
	} else if (mDescriptor.equalsIgnoreCase("MAX_INPUT")) {
	    return firstRelSize > secondRelSize ? firstRelSize : secondRelSize;
	} else if (mDescriptor.equalsIgnoreCase("SUM_INPUT")) {
	    return firstRelSize + secondRelSize;
	} else {
	    throw new RuntimeException("Unsupported getM " + mDescriptor);
	}
    }

    public static int getMax(int first, int second) {
	return first > second ? first : second;
    }

    public static long getMax(long first, long second) {
	return first > second ? first : second;
    }

    public static int getMin(int first, int second) {
	return first < second ? first : second;
    }

    public static long getMin(long first, long second) {
	return first < second ? first : second;
    }

    public static int getNumParentTasks(TopologyContext tc,
	    List<StormEmitter> emittersList) {
	int result = 0;
	for (final StormEmitter emitter : emittersList) {
	    // We have multiple emitterIDs only for StormSrcJoin
	    final String[] ids = emitter.getEmitterIDs();
	    for (final String id : ids)
		result += tc.getComponentTasks(id).size();
	}
	return result;
    }

    // used for NoACK optimization
    public static int getNumParentTasks(TopologyContext tc,
	    StormEmitter emitter1, StormEmitter... emittersArray) {
	final List<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.addAll(Arrays.asList(emittersArray));

	return getNumParentTasks(tc, emittersList);
    }

    // used for NoACK optimization for StormSrcJoin
    public static int getNumParentTasks(TopologyContext tc,
	    StormSrcHarmonizer harmonizer) {
	final String id = String.valueOf(harmonizer.getID());
	return tc.getComponentTasks(String.valueOf(id)).size();
    }

    /*
     * Method invoked with arguments "a/b//c/e//f", 0 return "f" Method invoked
     * with arguments "a/b//c/e//f", 1 return "e"
     */
    public static String getPartFromEnd(String path, int fromEnd) {
	final String parts[] = path.split("\\/+");
	final int length = parts.length;
	return new String(parts[length - (fromEnd + 1)]);
    }

    public static String getQueryID(Map map) {
	String queryName = SystemParameters.getString(map, "DIP_QUERY_NAME");
	String dataPath = SystemParameters.getString(map, "DIP_DATA_PATH");
	String sizeSkew = extractSizeSkew(dataPath);
	return queryName + "_" + sizeSkew;
    }

    public static String getStackTrace(Throwable aThrowable) {
	final Writer result = new StringWriter();
	final PrintWriter printWriter = new PrintWriter(result);
	aThrowable.printStackTrace(printWriter);
	return result.toString();
    }

    public static double getUsedMemoryMBs() {
	Runtime runtime = Runtime.getRuntime();
	long memory = runtime.totalMemory() - runtime.freeMemory();
	return memory / 1024.0 / 1024.0;
    }

    // assumes TreeMap where non-mentioned elements have value of their closest
    // left neighbor
    public static <K, V> V getValue(TreeMap<K, V> treeMap, K key) {
	Entry<K, V> repositionedEntry = treeMap.floorEntry(key);
	if (repositionedEntry == null) {
	    return null;
	} else {
	    return repositionedEntry.getValue();
	}
    }

    public static long getWindowClockTicker(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_GC_CLOCK_TICK_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_GC_CLOCK_TICK_SECS");
	else
	    return -1;
    }

    public static long getWindowSize(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_SIZE_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_SIZE_SECS") * 1000;
	else
	    return -1;
    }

    public static long getWindowTumblingSize(Map conf) {
	if (SystemParameters.doesExist(conf, "WINDOW_TUMBLING_SIZE_SECS"))
	    return SystemParameters.getLong(conf, "WINDOW_TUMBLING_SIZE_SECS") * 1000;
	else
	    return -1;
    }

    // if this is false, we have a specific mechanism to ensure all the tuples
    // are fully processed
    // it is based on CustomStreamGrouping
    public static boolean isAckEveryTuple(Map map) {
	int ackers;
	if (!SystemParameters.isExisting(map, "DIP_NUM_ACKERS"))
	    // number of ackers is defined in storm.yaml
	    ackers = SystemParameters.DEFAULT_NUM_ACKERS;
	else
	    ackers = SystemParameters.getInt(map, "DIP_NUM_ACKERS");
	return (ackers > 0);
    }

    public static boolean isAggBatchOutputMode(long batchOutputMillis) {
	return batchOutputMillis != 0L;
    }

    public static boolean isAutoOutputSampleSize(Map conf) {
	String mode = SystemParameters.getString(conf,
		"OUTPUT_SAMPLE_SIZE_MODE");
	return mode.equalsIgnoreCase("AUTO");
    }

    public static boolean isBDB(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_IS_BDB")
		&& SystemParameters.getBoolean(conf, "DIP_IS_BDB");
    }

    public static boolean isBDBSkewed(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
		&& SystemParameters.getString(conf, "DIP_BDB_TYPE")
			.equalsIgnoreCase("SKEWED");
    }

    public static boolean isBDBUniform(Map conf) {
	return SystemParameters.isExisting(conf, "DIP_BDB_TYPE")
		&& SystemParameters.getString(conf, "DIP_BDB_TYPE")
			.equalsIgnoreCase("UNIFORM");
    }

    public static boolean isCustomTimestampMode(Map map) {
	return SystemParameters.isExisting(map, "CUSTOM_TIMESTAMP")
		&& SystemParameters.getBoolean(map, "CUSTOM_TIMESTAMP");
    }

    /*
     * Does bigger dominates over smaller? For (bigger, smaller) = (double,
     * long) answer is yes.
     */
    private static boolean isDominant(Type bigger, Type smaller) {
	// for now we only have two numeric types: double and long
	if (bigger instanceof DoubleType)
	    return true;
	else
	    return false;
    }

    public static boolean isFinalAck(List<String> tuple, Map map) {
	return (!isAckEveryTuple(map)) && isFinalAck(tuple.get(0));
    }

    private static boolean isFinalAck(String tupleString) {
	return tupleString.equals(SystemParameters.LAST_ACK);
    }

    public static boolean isFinalAckManualBatching(String tupleString, Map map) {
	return (!isAckEveryTuple(map)) && isFinalAck(tupleString);
    }

    public static boolean isIIPrecBoth(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	return (SystemParameters.isExisting(conf, key))
		&& SystemParameters.getString(conf, key).equalsIgnoreCase(
			"BOTH");
    }

    public static boolean isIIPrecDense(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	if (SystemParameters.isExisting(conf, key)) {
	    return (SystemParameters.getString(conf, key).equalsIgnoreCase(
		    "DENSE") || SystemParameters.getString(conf, key)
		    .equalsIgnoreCase("BOTH"));
	} else {
	    // by default it is turned false, because it's expensive
	    return false;
	}
    }

    public static boolean isIIPrecPWeight(Map conf) {
	String key = "SECOND_PRECOMPUTATION";
	// if nothing is in there, we take the best case
	return (!(SystemParameters.isExisting(conf, key))
		|| SystemParameters.getString(conf, key).equalsIgnoreCase(
			"PWEIGHT") || SystemParameters.getString(conf, key)
		.equalsIgnoreCase("BOTH"));
    }

    public static boolean isManualBatchingMode(Map map) {
	return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
		&& SystemParameters.getString(map, "BATCH_SEND_MODE")
			.equalsIgnoreCase("MANUAL_BATCH");
    }

    public static <K, V> boolean isMapsEqual(HashMap<K, V> map1,
	    Map<K, V> map2, StringBuilder sb) {
	int size1 = map1.size();
	int size2 = map2.size();
	if (size1 != size2) {
	    sb.append("Different sizes: Size1 = " + size1 + ", size2 = "
		    + size2);
	    return false;
	}
	for (Map.Entry<K, V> entry : map1.entrySet()) {
	    K key1 = entry.getKey();
	    V value1 = entry.getValue();
	    if (!map2.containsKey(key1)) {
		sb.append("Map2 does not contain key " + key1);
		return false;
	    } else {
		V value2 = map2.get(key1);
		if (!value1.equals(value2)) {
		    sb.append("Different values for key = " + key1
			    + ": value1 = " + value1 + ", value2 = " + value2);
		    return false;
		}
	    }
	}
	return true;
    }

    // If Opt2 is not used (S1ReservoirGenerator), let's not invoke this method
    // map1 is based on prefix sum: the value of a key is the value of its
    // closest neighbor to the left
    public static <K, V> boolean isMapsEqual(TreeMap<K, V> map1,
	    Map<K, V> map2, StringBuilder sb) {
	// their sizes differ because of different map implementation
	for (Map.Entry<K, V> entry : map2.entrySet()) {
	    K key2 = entry.getKey();
	    V value2 = entry.getValue();
	    V value1 = getValue(map1, key2);
	    if (!value2.equals(value1)) {
		sb.append("Different values for key = " + key2 + ": value1 = "
			+ value1 + ", value2 = " + value2);
		return false;
	    }
	}
	return true;
    }

    public static boolean isOutputSampleSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.OUTPUT_SAMPLE_SIZE);
    }

    // printout the next_to_last component output just after the selection
    public static boolean isPrintFilteredLast(Map map) {
	return SystemParameters.isExisting(map, "PRINT_FILTERED_LAST")
		&& SystemParameters.getBoolean(map, "PRINT_FILTERED_LAST");
    }

    public static boolean isPrintLatency(int hierarchyPosition, Map conf) {
	return MyUtilities.isCustomTimestampMode(conf)
		&& hierarchyPosition == StormComponent.FINAL_COMPONENT;
    }

    public static boolean isRelSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.REL_SIZE);
    }

    public static boolean isSending(int hierarchyPosition,
	    long batchOutputMillis) {
	return (hierarchyPosition != StormComponent.FINAL_COMPONENT)
		&& !isAggBatchOutputMode(batchOutputMillis);
    }

    public static boolean isStatisticsCollector(Map map, int hierarchyPosition) {
	return hierarchyPosition == StormComponent.FINAL_COMPONENT
		&& SystemParameters.isExisting(map, "DIP_STATISTIC_COLLECTOR")
		&& SystemParameters.getBoolean(map, "DIP_STATISTIC_COLLECTOR");
    }

    public static boolean isStoreTimestamp(Map map, int hierarchyPosition) {
	return (isWindowTimestampMode(map))
		|| (isCustomTimestampMode(map)
			&& hierarchyPosition == StormComponent.FINAL_COMPONENT
			&& SystemParameters.isExisting(map, "STORE_TIMESTAMP") && SystemParameters
			    .getBoolean(map, "STORE_TIMESTAMP"));
    }

    public static boolean isThrottlingMode(Map map) {
	return SystemParameters.isExisting(map, "BATCH_SEND_MODE")
		&& SystemParameters.getString(map, "BATCH_SEND_MODE")
			.equalsIgnoreCase("THROTTLING");
    }

    public static boolean isTickTuple(Tuple tuple) {
	return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
		&& tuple.getSourceStreamId().equals(
			Constants.SYSTEM_TICK_STREAM_ID);
    }

    public static boolean isTotalOutputSize(List<String> tuple) {
	return tuple.get(0).equals(SystemParameters.TOTAL_OUTPUT_SIZE);
    }

    public static boolean isWindowTimestampMode(Map map) {
	return (WindowSemanticsManager._IS_WINDOW_SEMANTICS);
    }

    public static List<String> listFilesForPath(String dir) {
	List<String> filePaths = new ArrayList<String>();

	File folder = new File(dir);
	for (File fileEntry : folder.listFiles()) {
	    if (fileEntry.isDirectory()) {
		if (!fileEntry.getName().startsWith(".")) {
		    // avoid hidden folder
		    filePaths.addAll(listFilesForPath(fileEntry
			    .getAbsolutePath()));
		}
	    } else {
		filePaths.add(fileEntry.getAbsolutePath());
	    }
	}

	return filePaths;
    }

    public static <T extends Comparable<T>> List<ValueExpression> listTypeErasure(
	    List<ValueExpression<T>> input) {
	final List<ValueExpression> result = new ArrayList<ValueExpression>();
	for (final ValueExpression ve : input)
	    result.add(ve);
	return result;
    }

    public static void main(String[] args) {
	System.out
		.println("For relSize = 12 000 and ns = 5821, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 5821, "n_s"));
	System.out
		.println("For relSize = 12 000 and ns = 6000, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 6000, "n_s"));
	System.out
		.println("For relSize = 12 000 and ns = 6001, newNumOfBuckets = "
			+ MyUtilities.adjustPartitioning(12000, 6001, "n_s"));
	System.out
		.println("For relSize = 12M and ns = 12405, newNumOfBuckets = "
			+ MyUtilities
				.adjustPartitioning(12000000, 12405, "n_s"));
	System.out
		.println("For relSize = 25123111 and ns = 17778, newNumOfBuckets = "
			+ MyUtilities
				.adjustPartitioning(25123111, 17778, "n_s"));
    }

    public static void printBlockingResult(String componentName,
	    Operator op, int hierarchyPosition, Map map, Logger log) {
	// just print it, necessary for both modes (in Local mode we might print
	// other than final components)
	printPartialResult(componentName, op.getNumTuplesProcessed(),
		op.printContent(), map, log);

	LocalMergeResults.localCollectFinalResult(op, hierarchyPosition, map,
		log);
    }

    private static void printPartialResult(String componentName,
	    int numProcessedTuples, String compContent, Map map, Logger log) {
	final StringBuilder sb = new StringBuilder();
	sb.append("\nThe result for topology ");
	sb.append(SystemParameters.getString(map, "DIP_TOPOLOGY_NAME"));
	sb.append("\nComponent ").append(componentName).append(":\n");
	sb.append("\nThis task received ").append(numProcessedTuples);
	sb.append("\n").append(compContent);
	log.info(sb.toString());
    }

    // in ProcessFinalAck and dumpSignal we have acking at the end, because we
    // return after that
    public static void processFinalAck(int numRemainingParents,
	    int hierarchyPosition, Map conf, Tuple stormTupleRcv,
	    OutputCollector collector) {
	if (numRemainingParents == 0)
	    // this task received from all the parent tasks
	    // SystemParameters.LAST_ACK
	    if (hierarchyPosition != StormComponent.FINAL_COMPONENT) {
		// if this component is not the last one
		final Values values = createUniversalFinalAckTuple(conf);
		collector.emit(values);
	    } else
		collector.emit(SystemParameters.EOF_STREAM, new Values(
			SystemParameters.EOF));
	collector.ack(stormTupleRcv);
    }

    public static void processFinalAck(int numRemainingParents,
	    int hierarchyPosition, Map conf, Tuple stormTupleRcv,
	    OutputCollector collector, PeriodicAggBatchSend periodicBatch) {
	if (numRemainingParents == 0)
	    if (periodicBatch != null) {
		periodicBatch.cancel();
		periodicBatch.getComponent().aggBatchSend();
	    }
	processFinalAck(numRemainingParents, hierarchyPosition, conf,
		stormTupleRcv, collector);
    }

    public static void processFinalAckCustomStream(String streamId,
	    int numRemainingParents, int hierarchyPosition, Map conf,
	    Tuple stormTupleRcv, OutputCollector collector) {
	if (numRemainingParents == 0)
	    // this task received from all the parent tasks
	    // SystemParameters.LAST_ACK
	    if (hierarchyPosition != StormComponent.FINAL_COMPONENT) {
		// if this component is not the last one
		final Values values = createUniversalFinalAckTuple(conf);
		// collector.emit(values); this caused a bug that
		// S1ReservoirGenerator sent to S1ReservoirMerge 2x more acks
		// than required
		collector.emit(streamId, values);
	    } else
		collector.emit(SystemParameters.EOF_STREAM, new Values(
			SystemParameters.EOF));
	collector.ack(stormTupleRcv);
    }

    public static void processFinalAckCustomStream(String streamId,
	    int numRemainingParents, int hierarchyPosition, Map conf,
	    Tuple stormTupleRcv, OutputCollector collector,
	    PeriodicAggBatchSend periodicBatch) {
	if (numRemainingParents == 0)
	    if (periodicBatch != null) {
		periodicBatch.cancel();
		periodicBatch.getComponent().aggBatchSend();
	    }
	processFinalAckCustomStream(streamId, numRemainingParents,
		hierarchyPosition, conf, stormTupleRcv, collector);
    }

    /*
     * Read query plans - read as verbatim
     */
    public static String readFile(String path) {
	try {
	    final StringBuilder sb = new StringBuilder();
	    final BufferedReader reader = new BufferedReader(new FileReader(
		    new File(path)));

	    String line;
	    while ((line = reader.readLine()) != null)
		sb.append(line).append("\n");
	    if (sb.length() > 0)
		sb.deleteCharAt(sb.length() - 1); // last \n is unnecessary
	    reader.close();

	    return sb.toString();
	} catch (final IOException ex) {
	    final String err = MyUtilities.getStackTrace(ex);
	    throw new RuntimeException("Error while reading a file:\n " + err);
	}
    }

    /*
     * Used for reading a result file, # should be treated as possible data, not
     * comment
     */
    public static List<String> readFileLinesSkipEmpty(String path)
	    throws IOException {
	final BufferedReader reader = new BufferedReader(new FileReader(
		new File(path)));

	final List<String> lines = new ArrayList<String>();
	String strLine;
	while ((strLine = reader.readLine()) != null)
	    if (!strLine.isEmpty())
		lines.add(strLine);
	reader.close();
	return lines;
    }

    /*
     * Used for reading an SQL file
     */
    public static String readFileSkipEmptyAndComments(String path) {
	try {
	    final StringBuilder sb = new StringBuilder();

	    final List<String> lines = readFileLinesSkipEmpty(path);
	    for (String line : lines) {
		line = line.trim();
		if (!line.startsWith("#"))
		    sb.append(line).append(" ");
	    }
	    if (sb.length() > 0)
		sb.deleteCharAt(sb.length() - 1); // last space is unnecessary

	    return sb.toString();
	} catch (final IOException ex) {
	    final String err = MyUtilities.getStackTrace(ex);
	    throw new RuntimeException("Error while reading a file:\n " + err);
	}
    }

    public static void sendTuple(String streamId, Values stormTupleSnd,
	    Tuple stormTupleRcv, OutputCollector collector, Map conf) {
	// stormTupleRcv is equals to null when we send tuples in batch fashion
	if (isAckEveryTuple(conf) && stormTupleRcv != null)
	    collector.emit(streamId, stormTupleRcv, stormTupleSnd);
	else
	    collector.emit(streamId, stormTupleSnd);
    }

    // this is for Spout
    public static void sendTuple(Values stormTupleSnd,
	    SpoutOutputCollector collector, Map conf) {
	String msgId = null;
	if (MyUtilities.isAckEveryTuple(conf))
	    msgId = "T"; // as short as possible
	if (msgId != null)
	    collector.emit(stormTupleSnd, msgId);
	else
	    collector.emit(stormTupleSnd);
    }
    
 // this is for Spout
    public static void sendTuple(String streamID, Values stormTupleSnd,
	    SpoutOutputCollector collector, Map conf) {
	String msgId = null;
	if (MyUtilities.isAckEveryTuple(conf))
	    msgId = "T"; // as short as possible
	if (msgId != null)
	    collector.emit(streamID, stormTupleSnd, msgId);
	else
	    collector.emit(streamID, stormTupleSnd);
    }

    /*
     * no acking at the end, because for one tuple arrived in JoinComponent, we
     * might have multiple tuples to be sent.
     */
    public static void sendTuple(Values stormTupleSnd, Tuple stormTupleRcv,
	    OutputCollector collector, Map conf) {
	// stormTupleRcv is equals to null when we send tuples in batch fashion
	if (isAckEveryTuple(conf) && stormTupleRcv != null)
	    collector.emit(stormTupleRcv, stormTupleSnd);
	else
	    collector.emit(stormTupleSnd);
    }

    public static List<String> stringToTuple(String tupleString, Map conf) { // arraylist
	// 2
	// values
	final String[] columnValues = tupleString.split(SystemParameters
		.getString(conf, "DIP_GLOBAL_SPLIT_DELIMITER"));
        return new ArrayList<String>(Arrays.asList(columnValues));
    }

    public static InputDeclarer thetaAttachEmitterComponents(
	    InputDeclarer currentBolt, StormEmitter emitter1,
	    StormEmitter emitter2, List<String> allCompNames,
	    MatrixAssignment assignment, Map map, Type wrapper) {

	// MatrixAssignment assignment = new MatrixAssignment(firstRelationSize,
	// secondRelationSize, parallelism,-1);

	final String firstEmitterIndex = String.valueOf(allCompNames
		.indexOf(emitter1.getName()));
	final String secondEmitterIndex = String.valueOf(allCompNames
		.indexOf(emitter2.getName()));

	CustomStreamGrouping mapping = null;

	if (assignment instanceof ContentSensitiveMatrixAssignment) {
	    mapping = new ContentSensitiveThetaJoinGrouping(firstEmitterIndex,
		    secondEmitterIndex, assignment, map, wrapper);
	} else
	    mapping = new ContentInsensitiveThetaJoinGrouping(
		    firstEmitterIndex, secondEmitterIndex, assignment, map);

	final ArrayList<StormEmitter> emittersList = new ArrayList<StormEmitter>();
	emittersList.add(emitter1);
	emittersList.add(emitter2);

	for (final StormEmitter emitter : emittersList) {
	    final String[] emitterIDs = emitter.getEmitterIDs();
	    for (final String emitterID : emitterIDs)
		currentBolt = currentBolt.customGrouping(emitterID, mapping);
	}
	return currentBolt;
    }

    // FIXME
    // For DateIntegerConversion, we want 1992-02-01 instead of 19920201
    // and there the default toString method returns 19920201
    public static <JAT extends Comparable<JAT>> String toSpecialString(JAT key,
	    NumericType wrapper) {
	String strKey;
	if (wrapper instanceof DateIntegerType) {
	    DateIntegerType dateIntConv = (DateIntegerType) wrapper;
	    strKey = dateIntConv.toStringWithDashes((Integer) key);
	} else {
	    strKey = wrapper.toString(key);
	}
	return strKey;
    }

    public static String tupleToString(List<String> tuple, Map conf) {
	String tupleString = "";
	for (int i = 0; i < tuple.size(); i++)
	    if (i == tuple.size() - 1)
		tupleString += tuple.get(i);
	    else
		tupleString += tuple.get(i)
			+ SystemParameters.getString(conf,
				"DIP_GLOBAL_ADD_DELIMITER");
	// this cause a bug when space (" ") is the last character:
	// tupleString=tupleString.trim();
	return tupleString;
    }

    private static Logger LOG = Logger.getLogger(MyUtilities.class);

    public static final String SINGLE_HASH_KEY = "SingleHashEntry";

}
