package indep.elvis;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DemoStreamWordCount {

    public static void main(String[] args) throws Exception {
        String hostName = "127.0.0.1";
        int port = 9000;
        // 设置运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<String> stream = env.socketTextStream(hostName, port);
        // 计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stream
                .flatMap((new LineSplitter()))
                .keyBy(0)
                .sum(1);
        // 输出
        sum.print();
        // 提交任务
        env.execute("Hello World");
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(token, 1));
                }
            }
        }
    }
}