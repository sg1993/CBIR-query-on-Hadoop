package mapreduce;
import java.util.Comparator;

public class DistanceObject {
	
	private double value = 9999999999999.999999;
	private String key = "";

	DistanceObject(String k, double v) {
		this.key = k;
		this.value = v;
	}

	void put(String k, double v) {
		this.key = k;
		this.value = v;
	}

	double getValue() {
		return this.value;
	}

	String getKey() {
		return this.key;
	}
}

class DistanceObjectComparator implements Comparator<DistanceObject> {

	@Override
	public int compare(DistanceObject o1, DistanceObject o2) {
		// TODO Auto-generated method stub
		double v1 = o1.getValue();
		double v2 = o2.getValue();
		return (v1 < v2 ? -1 : (v1 == v2 ? 0 : 1));
	}	
}