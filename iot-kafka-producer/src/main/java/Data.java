


import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;


public class Data implements Serializable{
	

	private static final long serialVersionUID = 1L;
	private String userId;
	private String sports;
	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="EST")
	private Date timestamp;
	private int heartrate;
	private int stresslevel;

	@JsonCreator
	public Data(@JsonProperty("userId") String userId, @JsonProperty("sports") String sports, @JsonProperty("timestamp") Date timestamp, @JsonProperty("heartrate") int heartrate, @JsonProperty("stresslevel") int stresslevel) {
		super();
		this.userId = userId;
		this.sports = sports;
		this.timestamp = timestamp;
		this.heartrate = heartrate;
		this.stresslevel = stresslevel;
	}

	public String getUserId() {
		return userId;
	}

	public String getSports() {
		return sports;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}

	public int getHeartrate() {
		return heartrate;
	}

	public int getStresslevel() {
		return stresslevel;
	}



}
