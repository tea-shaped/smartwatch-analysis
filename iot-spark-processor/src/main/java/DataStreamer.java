


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import kafka.serializer.StringDecoder;
import scala.Tuple2;


public class DataStreamer {
	
	 public static void main(String[] args) throws Exception {
		 SparkConf conf = new SparkConf()
				 .setAppName("DataProcessor")
				 .setMaster("local[4]")
				 .set("spark.cassandra.connection.host", "127.0.0.1")
				 .set("spark.cassandra.connection.port", "9042")
				 .set("spark.cassandra.connection.keep_alive_ms","6000");		  
		 JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(6));	
		 jssc.checkpoint("/tmp/checkpoints");
		 Map<String, String> kafkaParams = new HashMap<String, String>();
		 kafkaParams.put("zookeeper.connect", "localhost:2181");
		 kafkaParams.put("metadata.broker.list", "localhost:9092");
		 String topic = "iotdata";
		 Set<String> topics = new HashSet<String>();
		 topics.add(topic);
		 JavaPairInputDStream<String, Data> directKafkaStream = KafkaUtils.createDirectStream(
			        jssc,
			        String.class,
			        Data.class,
			        StringDecoder.class,
			        DataDecoder.class,
			        kafkaParams,
			        topics
			    );
		 
		
		 JavaPairDStream<String,Data> dataStream = directKafkaStream.map(tuple -> tuple._2()).mapToPair(iot -> new Tuple2<String,Data>(iot.getUserId(),iot)).reduceByKey((a, b) -> a );
		
		 JavaMapWithStateDStream<String, Data, Boolean, Tuple2<Data,Boolean>> dataStreamState = dataStream
							.mapWithState(StateSpec.function(uniqueUserFunction).timeout(Durations.seconds(1800)));//maintain state for 30min

		 JavaDStream<Tuple2<Data,Boolean>> filteredDStreams = dataStreamState.map(tuple2 -> tuple2)
							.filter(tuple -> tuple._2.equals(Boolean.FALSE));

		 JavaDStream<Data> filteredDataStream = filteredDStreams.map(tuple -> tuple._1);
		 
		 filteredDataStream.cache();
		 	 
		 DataProcessor dataProcessor = new DataProcessor();
		 dataProcessor.processTotalUser(filteredDataStream);
		 dataProcessor.processWindowUser(filteredDataStream);

		 
		
		 jssc.start();            
		 jssc.awaitTermination();  
  }

	private static final Function3<String, Optional<Data>, State<Boolean>, Tuple2<Data,Boolean>> uniqueUserFunction = (String, iot, state) -> {
			Tuple2<Data,Boolean> user = new Tuple2<>(iot.get(),false);
			if(state.exists()){
				user = new Tuple2<>(iot.get(),true);
			}else{
				state.update(Boolean.TRUE);
			}			
			return user;
		};
          
}
