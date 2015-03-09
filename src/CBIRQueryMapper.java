import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ReflectionUtils;


public class CBIRQueryMapper extends Mapper<Object, Text, Text, Text> {
	
	private static Log logger = LogFactory
			.getLog(CBIRQueryMapper.class);

	@SuppressWarnings("deprecation")
	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {
		logger.info("map method called.. " + value.toString() + "\n");

		DistanceObject[] dObj = new DistanceObject[1000000];
		int objCount = 0;

		String uri = value.toString();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		String query = contex.getConfiguration().get(
				"Query_image_feature_vector");
		logger.info("Query image fv:\n" + query);
		String[] q = query.split("_");
		double[] queryFeatureVector = new double[q.length];
		for (int i = 0; i < q.length; i++) {
			queryFeatureVector[i] = Double.parseDouble(q[i]);
		}
		Text t1 = new Text();
		Text t2 = new Text();
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable k = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable v = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();

			IntWritable c = new IntWritable();

			while (reader.next(k, v)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				c.set((int) position);
				t1.set(k.toString().split("_r_")[0]);

				position = reader.getPosition(); // beginning of next record

				t2.set(v.toString());
				// logger.info(position + " " + syncSeen + "\t" + t1.toString()
				// + "\t" + t2.toString() + "\t"
				// + reader.getValueClassName());

				/*
				 * This is where we compute the distance between images in
				 * database and the query image.
				 */
				query = v.toString();
				String[] fvs = query.split("_");
				double dist = 0.0, fvi;
				// double []featureVector = new double[fvs.length];
				for (int i = 0; i < fvs.length; i++) {
					fvi = Double.parseDouble(fvs[i]);
					dist += (double) (Math.abs(fvi - queryFeatureVector[i]))
							/ (1 + fvi + queryFeatureVector[i]);
				}
				dObj[objCount] = new DistanceObject(t1.toString(), dist);
				objCount++;
				// contex.write(t1, t2);
			}
		} catch (Exception e) {
			logger.info("Probably, some file-related issue has occurred. Exiting.");
		} finally {
			if (reader != null)
				reader.close();

			// sort the figures
			dObj = sortDistanceObjects(dObj, objCount);

			// finally, output only the top 'N' figures.
			int r = Integer.parseInt(contex.getConfiguration().get("Num_results"));
			int o = Math.min(objCount, r);
			for (int i = 0; i < o; i++) {
				//	Let all the keys be the same
				//	This is supposed to be an optimization.
				t1.set("0");
				t2.set(dObj[i].getKey().toString() + "_r_" + Double.toString(dObj[i].getValue()));
				logger.info("sending\t" + t1.toString() + "\t" + t2.toString());
				contex.write(t1, t2);
			}
		}
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