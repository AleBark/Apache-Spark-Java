import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_14 {

    public static void main(String[] args) {

        // Highest priced product, per weight type
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //<<Product + Weight type>, Price>>
        JavaPairRDD<String, Double> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("trade_usd") && !line.isEmpty() && !line.split(";")[3].isEmpty())
                .mapToPair(linha -> {
                    return new Tuple2<String, Double>(
                           (linha.split(";")[3] + "-" +linha.split(";")[7]) ,
                            Double.parseDouble(linha.split(";")[5]));
                }).reduceByKey(Math::max);

        System.out.println(transactionInputFile.collect());

    }

    public static PairFunction<String, String, Integer> getFlux() {
        PairFunction<String, String, Integer> result;
        result = transaction -> new Tuple2<>(transaction.split(";")[4] + "-" + transaction.split(";")[1], 1);
        return result;

    }
}
