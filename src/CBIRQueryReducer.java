import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 	For now, the top 'N' values are 
 * 	forwarded from the mappers to the 
 * 	lone reducer, where thay are sorted
 * 	and then the final top 'N' values
 *  are forwarded. This is to be later
 *  reviewed as it might be a 
 *  performance-damper.
 */

public class CBIRQueryReducer extends Reducer<Text, Text, Text, Text> {

	private static Log logger = LogFactory.getLog(CBIRQueryReducer.class);

	private static DistanceObject[] dObj;
	private static int objCount = 0;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		logger.info("Setup called");
		dObj = new DistanceObject[10000000];
		objCount = 0;
	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String k = "";
		double v = 9999999999.9999999;
		
		for(Text value : values){
			String a[] = value.toString().split("_r_");
			k = a[0];
			v = Double.parseDouble(a[1]);//
			logger.info(k + "\t" + v + "\t" + value.toString());
			dObj[objCount] = new DistanceObject(k, v);
			objCount++;
		}
		
		// sort the figures
		dObj = sortDistanceObjects(dObj, objCount);
	}
	
	private DistanceObject[] sortDistanceObjects(DistanceObject[] dObj, int objCount) {
		// TODO Auto-generated method stub
		Arrays.sort(dObj, 0, objCount-1, new DistanceObjectComparator());
		logger.info("\n\nSorted\n\n");
		for(int i=0;i<objCount;i++){
			logger.info(dObj[i].getKey() + "\t" + dObj[i].getValue());
		}
		return dObj;
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}

class CBIRQueryPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		return 0;
	}
}