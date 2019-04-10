import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;

import java.util.Arrays;

public class LambdaWordCount {

    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("LambdaWordCount");
        //创建sparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //指定以后从哪里读取数据
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //切分压平
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
        //和1组合
        JavaPairRDD<String, Integer> wordAndOne = words.mapToPair(word -> new Tuple2<>(word,1));
        //聚合
        JavaPairRDD<String,Integer> reduces = wordAndOne.reduceByKey((m,n) -> m+n);
        //调整顺序
        JavaPairRDD<Integer,String> swaped = reduces.mapToPair(tp -> tp.swap());
        //排序
        JavaPairRDD<Integer,String> sorted = swaped.sortByKey(false);
        //调整顺序
        JavaPairRDD<String,Integer> result = sorted.mapToPair(tp -> tp.swap());
        //保存结果
        result.saveAsTextFile(args[1]);
        //释放资源
        jsc.stop();
    }
}
