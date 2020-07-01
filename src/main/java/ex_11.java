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

        //<<Ano,Mercadoria>,<Peso, <Fluxo, Ocorrencia>>
        JavaPairRDD<Tuple2<Integer, String>, Tuple2<Long, Tuple2<String, Integer>>> transactionInputFile = sc.textFile("in/transacoes.csv")
                .filter(line -> !line.contains("weight_kg") && !line.isEmpty() && !line.split(";")[6].isEmpty())
                .filter(line -> line.contains("Brazil"))
                .mapToPair(linha -> {
                    return new Tuple2<Tuple2<Integer, String>, Tuple2<Long, Tuple2<String, Integer>>>(
                            new Tuple2<Integer, String>(Integer.parseInt(linha.split(";")[1]), linha.split(";")[3]),
                            new Tuple2<Long, Tuple2<String, Integer>>(Long.parseLong(linha.split(";")[6]),
                                    new Tuple2<String, Integer>(linha.split(";")[4], 1)));
                }).reduceByKey((x, y) -> {
//                  Exempĺo do RDD até aqui:
//                  (<<Ano,Mercadoria>) --> <Peso, <Fluxo, Ocorrencia> = <2020,Feijao> <20,<Importacao, 1>>>
//                  x = <20, <Importacao, 1>> , y = <30, <Exportacao , 1>>

//                  ((1990,Rivets, iron or steel),(115830,(ImportExport,2))), ((2006,Mixed alkylbenzenes, nes),(8635053,(ImportExport,2)))
                    return new Tuple2<Long, Tuple2<String, Integer>>(
//                            Long => Somatória dos pesos
                              (x._1 + y._1 ),
//                             String => Fluxo (está concatenando as strings, necessário lógica para agrupar), Integer = somatório ocorrências
                               new Tuple2<String, Integer>(x._2._1 + y._2._1, x._2._2 + y._2._2));
                });

        System.out.println(transactionInputFile.collect());

    }
}
