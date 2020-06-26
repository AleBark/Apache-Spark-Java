import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_07 {

    public static void main(String[] args) {

        // Highest weighted product in all transactions

        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> transactionInputFile = sc.textFile("in/transacoes.csv");

        JavaPairRDD<String, Double> countriesCount = transactionInputFile
                .filter(line -> !line.contains("weight_kg") && !line.isEmpty())
                .mapToPair(getWeight())
                .reduceByKey((x, y) -> x + y);

        List<Tuple2<String, Double>> results = countriesCount.collect();


        Double weight = 0.0;
        String product = null;

        for (Tuple2<String, Double> tuple : results) {
            if (tuple._2() > weight) {
                product = tuple._1();
                weight = tuple._2();
            }
        }

        System.out.println("Product with the highest weight (" + weight + ") of all transactions is: " + product);

    }

    public static PairFunction<String, String, Double> getWeight() {
        PairFunction<String, String, Double> result;

        result = transaction -> {
            if (!transaction.split(";")[6].equals("")) {
                return new Tuple2<>(transaction.split(";")[3], Double.parseDouble(transaction.split(";")[6].trim()));
            }
            return new Tuple2<>("null", 0.0);
        };
        return result;
    }
}
