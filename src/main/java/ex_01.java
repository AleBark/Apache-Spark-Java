import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.*;

public class ex_01 {

    public static void main(String[] args) {

        // Country with the largest number of commercial transactions

        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> transactionInputFile = sc.textFile("in/transacoes.csv");

        JavaPairRDD<String, Integer> countriesCount = transactionInputFile
                .mapToPair(getCountry())
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> results = countriesCount.collect();

        Integer transactions = 0;
        String country = null;

        for (Tuple2<String, Integer> tuple : results) {
            if (tuple._2() > transactions) {
                country = tuple._1();
                transactions = tuple._2();
            }
        }
        System.out.println("Country with the largest number (" + transactions + ") of transactions is " + country);

    }

    public static PairFunction<String, String, Integer> getCountry() {
        PairFunction<String, String, Integer> result;
        result = transaction -> new Tuple2<>(transaction.split(";")[0], 1);
        return result;

    }
}
