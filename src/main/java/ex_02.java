import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_02 {

    public static void main(String[] args) {

        // Product with the highest amount of commercial transactions in Brazil

        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> transactionInputFile = sc.textFile("in/transacoes.csv");

        JavaPairRDD<String, Integer> countriesCount = transactionInputFile
                .filter(line -> line.contains("Brazil"))
                .mapToPair(getProduct())
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Integer>> results = countriesCount.collect();

        Integer transactions = 0;
        String product = null;

        for (Tuple2<String, Integer> tuple : results) {
            if (tuple._2() > transactions) {
                product = tuple._1();
                transactions = tuple._2();
            }
        }
        System.out.println("Product with the largest number (" + transactions + ") of transactions in Brazil is: " + product);

    }

    public static PairFunction<String, String, Integer> getProduct() {
        PairFunction<String, String, Integer> result;
        result = transaction -> new Tuple2<>(transaction.split(";")[3], 1);
        return result;

    }
}
