import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_15 {

    public static void main(String[] args) {

        // Number of commercial transactions according to the "flux", separated by year
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //<<Flux + Year>, Occurrences>>
        JavaPairRDD<String, Integer> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.isEmpty() && !line.split(";")[4].isEmpty())
                .mapToPair(getFlux())
                .reduceByKey((x, y) -> x + y);

        System.out.println(transactionInputFile.collect());

    }

    public static PairFunction<String, String, Integer> getFlux() {
        PairFunction<String, String, Integer> result;
        result = transaction -> new Tuple2<>(transaction.split(";")[4] + "-" + transaction.split(";")[1], 1);
        return result;

    }
}
