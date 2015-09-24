package bigdata.project.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MaxVote {
	@SuppressWarnings("serial")
	private static final FlatMapFunction<String, Candidate> WORDS_EXTRACTOR = new FlatMapFunction<String, Candidate>() {
		public Iterable<Candidate> call(String s) throws Exception {
			String[] lineText = s.split("\n");
			List<Candidate> list = new ArrayList<Candidate>();
			if (lineText != null) {
				for (int i = 0; i < lineText.length; i++) {
					String line = lineText[i];
					String[] canData = line.split(" ");
					String name = canData[0];
					int vote = Integer.parseInt(canData[1]);
					Candidate candidate = new Candidate(name, vote);
					list.add(candidate);

				}
			}
			return list;
		}
	};

	@SuppressWarnings("serial")
	private static final PairFunction<Candidate, String, Integer> WORDS_MAPPER = new PairFunction<Candidate, String, Integer>() {
		public Tuple2<String, Integer> call(Candidate can) throws Exception {
			return new Tuple2<String, Integer>(can.name, can.vote);
		}
	};

	@SuppressWarnings("serial")
	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {

		public Integer call(Integer v1, Integer v2) throws Exception {
			// TODO Auto-generated method stub
			if (v1 < v2)
				return v2;
			else
				return v1;
		}

	};

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("usage [input] [output]");
			System.exit(0);
		}

		SparkConf conf = new SparkConf().setAppName("bigdata.project.spark.MaxVote").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> file = context.textFile(args[0]);
		JavaRDD<Candidate> words = file.flatMap(WORDS_EXTRACTOR);
		JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
		JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

		counter.saveAsTextFile(args[1]);
	}
}