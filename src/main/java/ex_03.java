import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_03 {

    public static void main(String[] args) {

        // Number of financial transactions/year

        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> transactionInputFile = sc.textFile("in/transacoes.csv");

        JavaPairRDD<String, Integer> countriesCount = transactionInputFile
                .mapToPair(getCountry())
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> results = countriesCount.collect();


        for (Tuple2<String, Integer> tuple : results) {
            System.out.println("Year: " + tuple._1() + " qty: " + tuple._2());
        }
    }

    public static PairFunction<String, String, Integer> getCountry() {
        PairFunction<String, String, Integer> result;
        result = transaction -> new Tuple2<>(transaction.split(";")[1], 1);
        return result;

    }
}
