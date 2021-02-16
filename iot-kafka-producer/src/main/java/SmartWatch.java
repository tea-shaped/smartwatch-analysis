
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SmartWatch {
	

	public static void main (String[] args) throws Exception {
		String topic = "iotdata";
		Properties props = new Properties();
		props.put("zookeeper.connect", "localhost:2181");
		props.put("metadata.broker.list", "localhost:9092");
		props.put("request.required.acks", "1");
		props.put("serializer.class", "DataEncoder");

		Producer<String, Data> producer = new Producer<String, Data>(new ProducerConfig(props));
		SmartWatch watch = new SmartWatch();
		watch.sendEvent(producer,topic);		
	}



	private void sendEvent(Producer<String, Data> producer, String topic) throws InterruptedException {
		long offset = Timestamp.valueOf("2017-12-12 08:00:00").getTime();
		long end = Timestamp.valueOf("2017-12-12 22:00:00").getTime();
		long diff = end - offset + 1;
		List<String> sportsList = Arrays.asList(new String[]{"running", "swimming", "biking", "hiking", "skiing"});
		Random rand = new Random();
		List<Data> useList = new ArrayList<Data>();
		while (true) {
			String userId = "u" + String.valueOf(rand.nextInt(40));
			String sports = sportsList.get(rand.nextInt(5));
			Timestamp time = new Timestamp(offset + (long)(Math.random() * diff));
			int heartrate = ThreadLocalRandom.current().nextInt(50,150+1);
			int stresslevel = ThreadLocalRandom.current().nextInt(0,10+1);
			Data use = new Data(userId, sports, time, heartrate, stresslevel);
			useList.add(use);
		
			
			for (Data event : useList) {
				KeyedMessage<String, Data> message = new KeyedMessage<String, Data>(topic, event);
				producer.send(message);
				Thread.sleep(1000);
			}
		}
	}
}
