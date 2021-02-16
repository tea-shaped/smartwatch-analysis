import java.io.Serializable;

public class AggregateKey implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private String sports;
	
	public AggregateKey(String sports) {
		super();
		this.sports = sports;
	}

	public String getSports() {
		return sports;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
result = prime * result + ((sports == null) ? 0 : sports.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if(obj !=null && obj instanceof AggregateKey){
			AggregateKey other = (AggregateKey)obj;
			if(other.getSports() != null){
				if((other.getSports().equals(this.sports))){
					return true;
				} 
			}
		}
		return false;
	}
	

}

