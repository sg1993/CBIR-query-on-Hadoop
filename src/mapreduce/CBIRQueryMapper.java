package mapreduce;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CBIRQueryMapper extends Mapper<Text, Text, Text, Text> {

	private static Log logger = LogFactory.getLog(CBIRQueryMapper.class);
	private static DistanceObject[] dObj;
	private static int objCount = 0;
	private static double[] queryFeatureVector;

	protected void setup(Context context) {
		// can handle a maximum of
		// million key-value pairs
		dObj = new DistanceObject[1000000];
		objCount = 0;

		// Get the query-image
		// feature-vector
		String query = context.getConfiguration().get(
				"Query_image_feature_vector");
		logger.info("Query image fv:\n" + query);
		String[] q = query.split("_");
		queryFeatureVector = new double[q.length + 2];
		for (int i = 0; i < q.length; i++) {
			queryFeatureVector[i] = Double.parseDouble(q[i]);
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		logger.info("Cleanup: Total count received: " + objCount);

		// sort the figures
		dObj = sortDistanceObjects(dObj, objCount);

		Text t1 = new Text();
		Text t2 = new Text();

		// finally, output only the top 'N' figures.
		int r = Integer.parseInt(context.getConfiguration().get("Num_results"));
		int o = Math.min(objCount, r);
		for (int i = 0; i < o; i++) {
			// Let all the keys be the same
			// This is supposed to be an optimization.
			t1.set("0");
			t2.set(dObj[i].getKey().toString() + "_val_"
					+ Double.toString(dObj[i].getValue()));
			logger.info("sending\t" + t1.toString() + "\t" + t2.toString());
			context.write(t1, t2);
		}
	}

	public void map(Text k, Text v, Context contex) throws IOException,
			InterruptedException {
		logger.info("map method called.. " + k.toString() + "\n");

		/*
		 * This is where we compute the distance between image passed to the map
		 * process and the query image.
		 */
		String query = v.toString();
		String[] fvs = query.split("_");
		double dist = 0.0, fvi;
		// double []featureVector = new double[fvs.length];
		for (int i = 0; i < fvs.length; i++) {
			fvi = Double.parseDouble(fvs[i]);
			dist += (double) (Math.abs(fvi - queryFeatureVector[i]))
					/ (1 + fvi + queryFeatureVector[i]);
		}
		dObj[objCount] = new DistanceObject(k.toString().split("_r_")[0], dist);
		objCount++;
		// contex.write(t1, t2);
	}

	private DistanceObject[] sortDistanceObjects(DistanceObject[] dObj,
			int objCount) {
		// TODO Auto-generated method stub
		Arrays.sort(dObj, 0, objCount, new DistanceObjectComparator());
		for (int i = 0; i < objCount; i++) {
			logger.info(dObj[i].getKey() + "\t" + dObj[i].getValue());
		}
		return dObj;
	}
}