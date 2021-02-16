

import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;


public class DataDecoder implements Decoder<Data> {
	
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	public DataDecoder(VerifiableProperties verifiableProperties) {

    }
	public Data fromBytes(byte[] bytes) {
		try {
			return objectMapper.readValue(bytes, Data.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
