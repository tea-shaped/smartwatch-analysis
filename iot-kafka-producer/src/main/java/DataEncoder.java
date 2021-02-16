
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class DataEncoder implements Encoder<Data> {
	
	private static final Logger logger = Logger.getLogger(DataEncoder.class);	
	private static ObjectMapper objectMapper = new ObjectMapper();		
	public DataEncoder(VerifiableProperties verifiableProperties) {

    }
	public byte[] toBytes(Data use) {
		try {
			String msg = objectMapper.writeValueAsString(use);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}
}
