import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_13 {

    public static void main(String[] args) {

        // Maximum code value
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //<<Flux , Year + Product>,<Weight, Occurrences>>
        JavaPairRDD<String, Integer> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("comm_code") && !line.isEmpty() && !line.split(";")[2].isEmpty())
                .mapToPair(line -> {
                    try {
                        return new Tuple2<String, Integer>(line.split(";")[3], Integer.parseInt(line.split(";")[2]));
                    } catch (Exception e) {
                        return new Tuple2<String, Integer>("null", 0);
                    }
                })
                .reduceByKey((x, y) -> Math.max(x, y));

        List<Tuple2<String, Integer>> results = transactionInputFile.collect();

        Integer max_code = 0;
        String product = null;

        for (Tuple2<String, Integer> tuple : results) {
            if (tuple._2() > max_code) {
                product = tuple._1();
                max_code = tuple._2();
            }
        }

        System.out.println("Product with the highest code (" + max_code + ") is: " + product);
    }
}
