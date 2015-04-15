package driver;

import imagefeature.GrayScaleFilter;
import imagefeature.LTrPFeatureExtractor;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URI;

import javax.imageio.ImageIO;

import mapreduce.CBIRQueryMapper;
import mapreduce.CBIRQueryPartitioner;
import mapreduce.CBIRQueryReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CBIRQueryDriver {
	// private static Log logger = LogFactory
	// .getLog(BinaryFilesToHadoopSequenceFile.class);

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		if (args.length < 4) {
			System.out
					.println("Usage: <jar file> <sequence filename(s)> "
							+ "<ABSOLUTE path of each text file containing paths "
							+ "to feature vector sequence file>"
							+ "<ABSOLUTE path of query filename> "
							+ "<desired number of results to fetch>"
							+ "<ABSOLUTE path to hdfs location where the output folder "
							+ "will automatically be created>");
			System.exit(0);
		}

		String absPath = args[args.length - 1];
		if (absPath.charAt(absPath.length() - 1) != '/') {
			absPath += "/";
		}

		Configuration conf = new Configuration();
		String a[] = args[args.length - 3].split("/");
		String queryFilename = args[args.length - 3];
		String qFilename = a[a.length - 1];
		String s = "";
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
			double[] featureVector = lfe.getFeatureVector();
			for (int i = 0; i < featureVector.length; i++) {
				s += featureVector[i] + "_";
			}
			s += "42";
		} catch (Exception e) {
			System.out.println("Caught an exception, exiting.");
			System.out
					.println("Remember to give the absolute path of the query file i.e. /test/path/qwerty.jpg");
			System.exit(0);
		} finally {
			IOUtils.closeStream(in);
		}

		conf.set("Query_file_name", absPath + qFilename);
		conf.set("Query_image_feature_vector", s);
		conf.set("Num_results", args[args.length - 2]);

		Job job = Job.getInstance(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CBIRQueryMapper.class);
		job.setReducerClass(CBIRQueryReducer.class);
		job.setPartitionerClass(CBIRQueryPartitioner.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);

		job.setNumReduceTasks(1);

		for (int i = 0; i < args.length - 3; i++) {
			MultipleInputs.addInputPath(job, new Path(args[i]),
					SequenceFileInputFormat.class);
		}
		FileOutputFormat.setOutputPath(job, new Path(absPath + "queryOutput"));

		job.setJarByClass(CBIRQueryDriver.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}