// NikitaKunchanwar
//800962459
//nkunchan@uncc.edu




package org.myorg1;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.nio.charset.CharacterCodingException;
import java.io.IOException;

public class PageRank extends Configured implements Tool {
	private static int N;
	private static NumberFormat nf = new DecimalFormat("0");

	// private static final Logger LOG = Logger.getLogger(PageRank.class);
	public static void main(String[] args) throws Exception {

		
		System.exit(ToolRunner.run(new Configuration(), new PageRank(), args));

	}
// Method to control flow of all jobs 
	public int run(String[] args) throws Exception {
		// boolean to handle the calls to 3 different jobs if any of them 
		boolean isCompleted = runforLength(args);
		if (!isCompleted)
			return 1;
 PageRank pageRanking = new PageRank();
   isCompleted =pageRanking.runParsing(args);	
			if (!isCompleted)
				return 1;
			String lastResultPath = null;
			for (int runs = 0; runs < 10; runs++) {
				String inPath = "PageRank/output" + nf.format(runs);
				lastResultPath = "PageRank/output" + nf.format(runs + 1);

				isCompleted = runRankCalculation(inPath, lastResultPath);

				 if (!isCompleted) return 1;
			}

			 isCompleted = runRankOrdering(lastResultPath,
			 "PageRank/result");

			 if (!isCompleted) return 1;
			return 0;
		

	}

	// Method to set configuratons to rank calculation job
	private boolean runRankCalculation(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job rankCalculator = Job.getInstance(conf, "rankCalculator");
		rankCalculator.setJarByClass(PageRank.class);

		rankCalculator.setOutputKeyClass(Text.class);
		rankCalculator.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(rankCalculator, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankCalculator, new Path(outputPath));

		rankCalculator.setMapperClass(RankCalculateMapper.class);
		rankCalculator.setReducerClass(RankCalculateReduce.class);

		return rankCalculator.waitForCompletion(true);
	}
	// Method to set configuratons to N calculation(no of lines) job
	private boolean runforLength(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job runforlength = Job.getInstance(conf, " runforlength");
		runforlength.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(runforlength, args[0]);
		FileOutputFormat.setOutputPath(runforlength, new Path("PageRank/nvalue"));
		runforlength.setMapperClass(MapperLengthJob.class);
		runforlength.setReducerClass(ReducerLengthJob.class);
		// set key and value class for reducer
		runforlength.setOutputKeyClass(Text.class);
		runforlength.setOutputValueClass(IntWritable.class);
		return runforlength.waitForCompletion(true);

	}
	// Method to set configuratons to parsing(link graph creation) job
	private boolean runParsing(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setInt("lines", N);
		Job runparsing = Job.getInstance(conf, " pagerank");
		runparsing.setJarByClass(this.getClass());

		FileInputFormat.addInputPaths(runparsing, args[0]);
		FileOutputFormat.setOutputPath(runparsing, new Path(args[1]));
		runparsing.setMapperClass(MapperJob1.class);
		runparsing.setReducerClass(ReducerJob1.class);
		// set key and value class for reducer
		runparsing.setOutputKeyClass(Text.class);
		runparsing.setOutputValueClass(Text.class);
		return runparsing.waitForCompletion(true);
	}
	// Method to set configuratons to sorter(which ranks the pages in decreasing order) job
	private boolean runRankOrdering(String inputPath, String outputPath)
			throws IOException, ClassNotFoundException, InterruptedException {
		// Configuration conf = new Configuration();

		Job rankOrdering = Job.getInstance(getConf(), " runrankordering ");
		rankOrdering.setJarByClass(PageRank.class);
		rankOrdering.setMapperClass(RankingMapper.class);
		rankOrdering.setReducerClass(RankingReducer.class);
		rankOrdering.setMapOutputKeyClass(DoubleWritable.class);
		rankOrdering.setMapOutputValueClass(Text.class);
		rankOrdering.setSortComparatorClass(MyKeyComparator.class);
		rankOrdering.setOutputKeyClass(Text.class);
		rankOrdering.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputPaths(rankOrdering, new Path(inputPath));
		FileOutputFormat.setOutputPath(rankOrdering, new Path(outputPath));

		rankOrdering.setInputFormatClass(TextInputFormat.class);
		rankOrdering.setOutputFormatClass(TextOutputFormat.class);

		return rankOrdering.waitForCompletion(true);
	}
	// custom key comparator to sort the pages in decreasing order instead of increasing one(which is default one)
	public static class MyKeyComparator extends WritableComparator {
		protected MyKeyComparator() {
			super(DoubleWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleWritable key1 = (DoubleWritable) w1;
			DoubleWritable key2 = (DoubleWritable) w2;
			return -1 * key1.compareTo(key2);
		}
	}
	//Mapper job to count the no of lines in input file 
	public static class MapperLengthJob extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);
		private static final Pattern WORD_BOUNDARY = Pattern
				.compile("\\s*\\b\\s*");

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			if (!line.isEmpty())
				context.write(new Text("count"), one);  // each time emit 1 with "count" word

		}

	}
	// Reducer job to count the no of lines in input file
	public static class ReducerLengthJob extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text word, Iterable<IntWritable> counts,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			// get the count for count word(as it is the only key)  and sum it up to get the total no of
			// occurances (which will give the total no of lines
			for (IntWritable count : counts) {
				sum += count.get();
			}
			N = sum;
			// print the word along with no of occurances
			context.write(word, new IntWritable(N));

		}
	}
	// Mapper job to parse input file and create link graph
	public static class MapperJob1 extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern wikiLinksPattern = Pattern .compile("\\[\\[.*?]\\]");

		static public enum MyCounters {
			Counter
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			
			// context.getCounter(MapperJob1.MyCounters.Counter).increment(1);
			// Configuration conf=context.getConfiguration();
			// String counter=conf.get("counter");
			// context.getConfiguration().set("counter",String.valueOf(Integer.parseInt(counter)+1));

			String[] titleAndText = parseTitleAndText(value);

			String pageString = titleAndText[0];

			Text page = new Text(pageString);

			Matcher matcher = wikiLinksPattern.matcher(titleAndText[1]);

			// Loop through the matched links in [CONTENT]
			while (matcher.find()) {
				String otherPage = matcher.group();
				
				otherPage = getWikiPageFromLink(otherPage);
				if (otherPage == null || otherPage.isEmpty())
					continue;

				// add valid otherPages to the map.
				context.write(page, new Text(otherPage));
			}
		}

		private String getWikiPageFromLink(String aLink) {
			
			int start = aLink.startsWith("[[") ? 2 : 1;
			int endLink = aLink.indexOf("]");
			aLink = aLink.substring(start, endLink);
			return aLink;
		}

		private String[] parseTitleAndText(Text value)
				throws CharacterCodingException {
			String[] titleAndText = new String[2];

			int start = value.find("<title>");
			int end = value.find("</title>");
			start += 7; // add <title> length.
			if (start == -1 || end == -1) {
				return new String[] { "","" };
			}

			titleAndText[0] = Text.decode(value.getBytes(), start, end - start);

			start = value.find("<text>");
			end = value.find("</text>");
			start += 6;

			if (start == -1 || end == -1) {
				return new String[] { "","" };
			}

			titleAndText[1] = Text.decode(value.getBytes(), start, end - start);

			return titleAndText;
		}
	}
// Reducer job to parse input file and create link graph
	public static class ReducerJob1 extends Reducer<Text, Text, Text, Text> {

		private long mapperCounter;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		
			Configuration conf = context.getConfiguration();
			int counter = conf.getInt("lines",1);
			double initial = 1.0 / (double)counter;
			String pagerank = String.valueOf(initial) + "\t";
			boolean first = true;

			for (Text value : values) {
				if (!first)
					pagerank += "#####";

				pagerank += value.toString();
				first = false;
			}

			context.write(key, new Text(pagerank));
		}

	}
	//Mapper job to calculate page rank of pages
	public static class RankCalculateMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			int pageTabIndex = value.find("\t");
			int rankTabIndex = value.find("\t", pageTabIndex + 1);

			String page = Text.decode(value.getBytes(), 0, pageTabIndex);
			String pageWithRank = Text.decode(value.getBytes(), 0,
					rankTabIndex + 1);

			// Mark page as an Existing page to calculate page rank of these pages alone
			context.write(new Text(page), new Text("!"));

			// Skip pages with no links.
			if (rankTabIndex == -1)
				return;

			String links = Text.decode(value.getBytes(), rankTabIndex + 1,
					value.getLength() - (rankTabIndex + 1));
			String[] allOtherPages = links.split("#####");
			int totalLinks = allOtherPages.length;

			for (String otherPage : allOtherPages) {
				Text pageRankTotalLinks = new Text(pageWithRank + totalLinks);
				context.write(new Text(otherPage), pageRankTotalLinks);
			}

			// Put the original links of the page along with current for the reduce output to use in further iterations
			context.write(new Text(page), new Text("|" + links));
		}
	}
       //Reducer job to calculate page rank of pages

	public static class RankCalculateReduce extends
			Reducer<Text, Text, Text, Text> {

		private static final float damping = 0.85F;

		@Override
		public void reduce(Text page, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			boolean isExistingWikiPage = false;
			String[] split;
			float sumShareOtherPageRanks = 0;
			String links = "";
			String pageWithRank;

			// For each otherPage(pages appended with left hand side page)
			// - calculate pageRank share <rank> / count(<links>)
			// - add the share to sumShareOtherPageRanks
			for (Text value : values) {
				pageWithRank = value.toString();

				if (pageWithRank.equals("!")) {
					isExistingWikiPage = true;
					continue;
				}

				if (pageWithRank.startsWith("|")) {
					links = "\t" + pageWithRank.substring(1);
					continue;
				}

				split = pageWithRank.split("\\t");

				float pageRank = Float.valueOf(split[1]);
				int countOutLinks = Integer.valueOf(split[2]);

				sumShareOtherPageRanks += (pageRank / countOutLinks);
			}

			if (!isExistingWikiPage)
				return;
			float newRank = damping * sumShareOtherPageRanks + (1 - damping);

			context.write(page, new Text(newRank + links));
		}
	}
	// Mapper job to assign final page ranks got from privious job 
	public static class RankingMapper extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] pageAndRank = getPageAndRank(key, value);

			float parseFloat = Float.parseFloat(pageAndRank[1]);

			Text page = new Text(pageAndRank[0]);
			DoubleWritable rank = new DoubleWritable(parseFloat);

			context.write(rank, page);
		}

		private String[] getPageAndRank(LongWritable key, Text value)
				throws CharacterCodingException {
			String[] pageAndRank = new String[2];
			int tabPageIndex = value.find("\t");
			int tabRankIndex = value.find("\t", tabPageIndex + 1);

			// no tab after rank (when there are no links)
			int end;
			if (tabRankIndex == -1) {
				end = value.getLength() - (tabPageIndex + 1);
			} else {
				end = tabRankIndex - (tabPageIndex + 1);
			}

			pageAndRank[0] = Text.decode(value.getBytes(), 0, tabPageIndex);
			pageAndRank[1] = Text.decode(value.getBytes(), tabPageIndex + 1,
					end);

			return pageAndRank;
		}

	}
	// Mapper job to assign final page ranks got from privious job 

	public static class RankingReducer extends
			Reducer<DoubleWritable, Text, Text, DoubleWritable> {
		@Override
		public void reduce(DoubleWritable rank, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			Text count = new Text();
			for (Text page : values) {
				context.write(page, rank);
			}

		}

	}
}

