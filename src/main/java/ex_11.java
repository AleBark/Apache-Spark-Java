import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_11 {

    public static void main(String[] args) {

        // Average weight per product in Brazil, separated according to year and flux type
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //<<Flux , Year + Product>,<Weight, Occurrences>>
        JavaPairRDD<Tuple2<String, String>,Double> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("weight_kg") && !line.isEmpty() && !line.split(";")[6].isEmpty())
                .filter(line -> line.contains("Brazil"))
                .mapToPair(linha -> {
                    return new Tuple2<Tuple2<String, String>,  Tuple2<Long, Integer>>(
                            new Tuple2<String, String>(linha.split(";")[3] + " - " + linha.split(";")[1], linha.split(";")[4]),
                            new Tuple2<Long, Integer> (Long.parseLong(linha.split(";")[6]), 1));
                }).reduceByKey((x, y) -> {
                    return new Tuple2<Long, Integer>((x._1 +y._1 ), (x._2 + y._2));
                }).mapValues(tuple -> (double) tuple._1 / tuple._2);

        System.out.println(transactionInputFile.collect());

    }
}
