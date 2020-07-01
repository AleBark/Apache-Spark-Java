import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_08 {

    public static void main(String[] args) {

        // Highest weighted product in all transactions divided by years
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaPairRDD<Tuple2<Integer, String>, Long> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("weight_kg") && !line.isEmpty() && !line.split(";")[6].isEmpty())
                .mapToPair(linha -> {
                    return new Tuple2<Tuple2<Integer, String>, Long>(
                            new Tuple2<Integer, String>
                                    (Integer.parseInt(linha.split(";")[1]), linha.split(";")[3]), Long.parseLong(linha.split(";")[6]));
                }).reduceByKey((x, y) -> x + y);


        JavaPairRDD<Integer, Tuple2<String, Long>> transactionInputFile2 = transactionInputFile
                .mapToPair(line -> {
                    return new Tuple2<Integer, Tuple2<String, Long>>(line._1._1,
                            new Tuple2<String, Long>
                                    (line._1._2, line._2));
                }).reduceByKey((x, y) -> x._2() > y._2() ? x : y);

        System.out.println(transactionInputFile2.collect());
    }
}
