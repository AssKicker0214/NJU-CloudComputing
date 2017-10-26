import com.mongodb.util.JSON;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.bson.Document;
import org.codehaus.jettison.json.JSONObject;
import java.io.Serializable;
import scala.Tuple2;

/**
 * author: Qiao Hongbo
 * time: {$time}
 **/
public class StreamHandler{
    public static final String STREAM_SERVER_HOST = "114.212.245.176";
    public static final int STREAM_SERVER_PORT = 9999;
    private JavaStreamingContext jssc;

    public StreamHandler() {
        SparkConf conf = new SparkConf()
                .setMaster("spark://pyq-master:7077")
                .set("spark.driver.host", "114.212.245.176")
//                .set("SPARK_LOCAL_IP", "114.212.242.132")
//                .set("SPARK_")
                .setAppName("Team13");
        //.setJars(new String[]{"/home/puyvqi/test/NJU-CloudComputing/target/spark-streaming-jingdong-1.0-SNAPSHOT.jar"});
        jssc = new JavaStreamingContext(conf, Durations.seconds(1));


//        JavaReceiverInputDStream<String> lines = jssc.file
    }

    public void defineProcess() {
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream(STREAM_SERVER_HOST, STREAM_SERVER_PORT);
        JavaDStream<Document> docs = lines.map(new Function<String, Document>() {
            @Override
            public Document call(String s) throws Exception {
                return Document.parse(s);
            }
        });
//        JavaPairDStream<String, Integer> commentPairs = docs.mapToPair(doc -> new Tuple2<>("" + doc.get("comment_id"), 1));
        JavaPairDStream<String, Integer> commentPairs = docs.mapToPair(new PairFunction<Document, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Document document) throws Exception {
                return new Tuple2<>(document.get("comment_id")+"", 1);
            }
        });
//        JavaPairDStream<String, Integer> commentCounts = commentPairs.reduceByKey((cnt1, cnt2) -> cnt1 + cnt2);
        JavaPairDStream<String, Integer> commentCounts = commentPairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        commentCounts.print(10);

    }

    public void start() {

        try {
            jssc.start();
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("conf/log4j.properties");
        Logger logger = Logger.getLogger(StreamHandler.class);
        logger.debug("debug");
        logger.error("error");

        StreamHandler handler = new StreamHandler();
        handler.defineProcess();
        handler.start();
    }
}
