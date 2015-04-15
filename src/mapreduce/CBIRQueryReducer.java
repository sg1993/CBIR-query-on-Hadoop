package mapreduce;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
	private static String queryResult = "";

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

		for (Text value : values) {
			String a[] = value.toString().split("_val_");
			k = a[0];
			v = Double.parseDouble(a[1]);//
			logger.info(k + "\t" + v + "\t" + value.toString());
			dObj[objCount] = new DistanceObject(k, v);
			objCount++;
		}

		// sort the figures
		dObj = sortDistanceObjects(dObj, objCount);
	}

	private DistanceObject[] sortDistanceObjects(DistanceObject[] dObj,
			int objCount) {
		// TODO Auto-generated method stub
		Arrays.sort(dObj, 0, objCount, new DistanceObjectComparator());
		logger.info("\n\nSorted\n\n");
		for (int i = 0; i < objCount; i++) {
			logger.info(dObj[i].getKey() + "\t" + dObj[i].getValue());
		}
		return dObj;
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// A single reducer ensures
		// that cleanup() will be called
		// only once.
		Configuration conf = context.getConfiguration();
		String uri = conf.get("Query_file_name");
		Path f = new Path(uri + ".queresult");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(f)) {
			// File already exists.
			// Delete the file before proceeding.
			logger.info("Deleting existing file");
			fs.delete(f, true);
		}

		// proceed to write the
		// query results to the
		// created file.

		int r = Integer.parseInt(conf.get("Num_results"));
		int o = Math.min(objCount, r);
		for (int i = 0; i < o; i++) {
			queryResult += dObj[i].getKey() + "\n";
		}
		queryResult += dObj[o].getKey();
		FSDataOutputStream os = fs.create(f);
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os,
				"UTF-8"));
		br.write(queryResult);
		br.close();
	}
}