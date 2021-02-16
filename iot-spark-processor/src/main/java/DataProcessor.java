

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.google.common.base.Optional;

import scala.Tuple2;

public class DataProcessor {
	private static final Logger logger = Logger.getLogger(DataProcessor.class);

	public void processTotalUser(JavaDStream<Data> filteredDataStream) {
		
		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredDataStream
				.mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getSports()), 1L))
				.reduceByKey((a, b) -> a + b);
		
		JavaMapWithStateDStream<AggregateKey, Long, Long, Tuple2<AggregateKey, Long>> countDStreamState = countDStreamPair
				.mapWithState(StateSpec.function(totalSumFunc).timeout(Durations.seconds(3600)));//maintain state for one hour

		JavaDStream<Tuple2<AggregateKey, Long>> countDStream = countDStreamState.map(tuple2 -> tuple2);
		JavaDStream<TotalUserData> userDStream = countDStream.map(totalUserFunction);

		Map<String, String> columnMappings = new HashMap<String, String>();
		columnMappings.put("sports", "sports");
		columnMappings.put("count", "count");
		columnMappings.put("timestamp", "timestamp");
		columnMappings.put("date", "date");

		javaFunctions(userDStream).writerBuilder("iotkeyspace", "totalusersbysports",
				CassandraJavaUtil.mapToRow(TotalUserData.class, columnMappings)).saveToCassandra();
	}

	public void processWindowUser(JavaDStream<Data> filteredDataStream) {

		JavaPairDStream<AggregateKey, Long> countDStreamPair = filteredDataStream
				.mapToPair(iot -> new Tuple2<>(new AggregateKey(iot.getSports()), 1L))
				.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(6));

		JavaDStream<WindowUserData> userDStream = countDStreamPair.map(windowUserFunc);

		Map<String, String> columnMappings = new HashMap<String, String>();
		columnMappings.put("sports", "sports");
		columnMappings.put("count", "count");
		columnMappings.put("timestamp", "timestamp");
		columnMappings.put("date", "date");

		javaFunctions(userDStream).writerBuilder("iotkeyspace", "usersbysports_last10seconds",
				CassandraJavaUtil.mapToRow(WindowUserData.class, columnMappings)).saveToCassandra();
	}


	
	private static final Function3<AggregateKey, Optional<Long>, State<Long>,Tuple2<AggregateKey,Long>> totalSumFunc = (key,currentSum,state) -> {
        long totalSum = currentSum.or(0L) + (state.exists() ? state.get() : 0);
        Tuple2<AggregateKey, Long> total = new Tuple2<>(key, totalSum);
        state.update(totalSum);
        return total;
    };
    
    private static final Function<Tuple2<AggregateKey, Long>, TotalUserData> totalUserFunction = (tuple -> {
		logger.debug("Total Count : " + "key " + tuple._1().getSports() + " value "+ tuple._2());
		TotalUserData useData = new TotalUserData();
		useData.setSports(tuple._1().getSports());
		useData.setCount(tuple._2());
		useData.setTimestamp(new Date());
		useData.setDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return useData;
	});
    
    private static final Function<Tuple2<AggregateKey, Long>, WindowUserData> windowUserFunc = (tuple -> {
		logger.debug("Window Count : " + "key " + tuple._1().getSports() + " value " + tuple._2());
		WindowUserData windowData = new WindowUserData();
		windowData.setSports(tuple._1().getSports());
		windowData.setCount(tuple._2());
		windowData.setTimestamp(new Date());
		windowData.setDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
		return windowData;
	});

}
