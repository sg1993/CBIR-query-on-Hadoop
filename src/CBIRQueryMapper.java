import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import javax.imageio.ImageIO;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ReflectionUtils;


public class CBIRQueryMapper extends Mapper<Object, Text, Text, Text> {
	
	private static Log logger = LogFactory
			.getLog(CBIRQueryMapper.class);

	public void map(Object key, Text value, Context contex) throws IOException,
			InterruptedException {
		logger.info("map method called.. " + value.toString() + "\n");

		String uri = value.toString();
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path path = new Path(uri);

		SequenceFile.Reader reader = null;
		
		try {
			reader = new SequenceFile.Reader(fs, path, conf);
			Writable k = (Writable) ReflectionUtils.newInstance(
					reader.getKeyClass(), conf);
			Writable v = (Writable) ReflectionUtils.newInstance(
					reader.getValueClass(), conf);
			long position = reader.getPosition();
			Text t1 = new Text();
			Text t2 = new Text();
			IntWritable c = new IntWritable();
			while (reader.next(k, v)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				c.set((int) position);
				t1.set(k.toString().split("_r_")[0]);
				
				position = reader.getPosition(); // beginning of next record
								
				t2.set(v.toString());
				logger.info(position + " " + syncSeen + "\t" + t1.toString() + "\t"
						+ t2.toString() + "\t"
						+ reader.getValueClassName());
				contex.write(t1, t2);
			}
		} finally {

		}
	}
}
