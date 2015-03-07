import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;
import javax.imageio.ImageIO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CBIRQueryDriver {
	// private static Log logger = LogFactory
	// .getLog(BinaryFilesToHadoopSequenceFile.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 2) {
			System.out.println("Usage: <jar file> <Query filename>");
			System.exit(0);
		}

		Configuration conf = new Configuration();
		String queryFilename = args[args.length - 2], s = "";
		/**
		 * Compute feature-vector for the given query image once.
		 **/
		FileSystem fs = FileSystem.get(URI.create(queryFilename), conf);
		FSDataInputStream in = null;
		BufferedImage b;
		try {
			in = fs.open(new Path(queryFilename));
			b = ImageIO.read(in);
			GrayScaleFilter gsf = new GrayScaleFilter(b);

			// get the grayscale image using the filter
			BufferedImage grayImage = gsf.convertToGrayScale();

			// extract the feature from the grayscale image
			// using the LTrPFeatureExtractor
			LTrPFeatureExtractor lfe = new LTrPFeatureExtractor(grayImage);
			lfe.extractFeature();
			int[] featureVector = lfe.getFeatureVector();
			for (int i = 0; i < featureVector.length; i++) {
				s += featureVector[i] + "_";
			}
			s += "42";
		} catch(Exception e){
			System.out.println("Caught an exception, exiting.");
			System.exit(0);
		} finally {
			IOUtils.closeStream(in);
		}

		conf.set("Query_image_feature_vector", s);
		// conf.set("NUM_REDUCERS", Integer.toString(numOutputFiles));
		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CBIRQueryMapper.class);
		job.setReducerClass(CBIRQueryReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		// LazyOutputFormat.setOutputFormatClass(job,
		// SequenceFileOutputFormat.class);
		job.setPartitionerClass(CBIRQueryPartitioner.class);

		for (int i = 0; i < args.length - 2; i++) {
			// FileInputFormat.setInputPaths(job, new Path(args[i]));
			MultipleInputs.addInputPath(job, new Path(args[i]),
					TextInputFormat.class);
		}
		job.setJarByClass(CBIRQueryDriver.class);
		FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
		job.submit();
	}
}