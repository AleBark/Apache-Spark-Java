import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ex_09 {

    public static void main(String[] args) {

        // Average weight per product, separated according to year
        Logger.getLogger("log").setLevel(Level.INFO);
        SparkConf conf = new SparkConf().setAppName("Hello").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //<<Ano,Mercadoria>,<Peso, Ocorrencia>>
        JavaPairRDD<Tuple2<Integer, String>,Double> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("weight_kg") && !line.isEmpty() && !line.split(";")[6].isEmpty())
                .mapToPair(linha -> {
                    return new Tuple2<Tuple2<Integer, String>,  Tuple2<Long, Integer>>(
                            new Tuple2<Integer, String>(Integer.parseInt(linha.split(";")[1]), linha.split(";")[3]),
                            new Tuple2<Long, Integer> (Long.parseLong(linha.split(";")[6]), 1));
                }).reduceByKey((x, y) -> {
//                  (<<Ano,Mercadoria>) --> <Peso, Ocorrencia> = <2020,Feijao><20,1>
//                  (<<Ano,Mercadoria>) --> <Peso, Ocorrencia> = <2020,Arroz ><50,1>
//                  x = <20,1>, y = <30,1> - x._1 = 50
                    return new Tuple2<Long, Integer>((x._1 +y._1 ), (x._2 + y._2));
                }).mapValues(tuple -> (double) tuple._1 / tuple._2);

        System.out.println(transactionInputFile.collect());

    }
}
