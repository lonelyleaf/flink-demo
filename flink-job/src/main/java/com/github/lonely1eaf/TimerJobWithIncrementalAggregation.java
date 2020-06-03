package com.github.lonely1eaf;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 模拟一个监控设备离线的业务，使用一个定时发送消息的timer来模拟报文的定时发送，
 * 然后会定时掉线（长时间不发送消息），来模拟网络离线
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-release-1.10/dev/stream/operators/windows.html#processwindowfunction-with-incremental-aggregation
 */
@Slf4j
public class TimerJobWithIncrementalAggregation {

    private static final OutputTag<String> OUTPUT_TAG = new OutputTag<String>("clear-tag", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //设备是否在线的判断是使用处理报文的时间！因为设备可能有离线报文
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //这种写法，process时，是会收到该window的所有element
        //也就是窗口关闭是才进行process
        final SingleOutputStreamOperator<String> streamOperator = see.addSource(new TimerSource())
                .keyBy(value -> Long.valueOf(1))//把key写死，模拟单个设备
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))//使用seesion window，模拟5秒没收到消息就判断离线
                //为了让上游每次发送的消息都触发计算，这里需要配置trigger
                .trigger(new LongEveryTimeFireTrigger())
                //这里会先触发聚合函数，然后给process function处理
                .aggregate(new ReportAggFun(), new CountWithTimeoutFunction());

        //关闭窗口时，会发送side out到这个stream
        streamOperator.getSideOutput(OUTPUT_TAG)
                .map(str -> "====side-out-put:" + str)
                .print();

        //这里是window的输出
        streamOperator
                .map(str -> "====source:" + str)
                .print();

        log.info("任务启动");
        see.execute("timer-job");
    }

    /**
     * 定时发送数据到下游
     */
    private static class TimerSource implements SourceFunction<Long> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            int count = 50;
            while (count >= 0) {
                ctx.collect(Long.valueOf(count));
                if (count % 10 == 0) {
                    Thread.sleep(10000);
                } else {
                    Thread.sleep(1000);
                }
                count--;
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


    /**
     * 聚合函数
     */
    private static class ReportAggFun implements AggregateFunction<Long, String, String> {

        @Override
        public String createAccumulator() {
            return "start:";
        }

        @Override
        public String add(Long value, String accumulator) {
            return accumulator + "," + value;
        }

        @Override
        public String getResult(String accumulator) {
            return accumulator;
        }

        @Override
        public String merge(String a, String b) {
            if (a.length() > b.length()) {
                return a;
            } else {
                return b;
            }
        }
    }

    /**
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static class CountWithTimeoutFunction
            extends ProcessWindowFunction<String, String, Long, TimeWindow> {

        /**
         * The state that is maintained by this process function
         */
        private ValueState<String> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", String.class));
        }

        @Override
        public void process(Long key, Context context,
                            Iterable<String> elements, Collector<String> out) throws Exception {
            final String next = elements.iterator().next();
            state.update(next);
            out.collect(next);
        }

        @Override
        public void clear(Context context) throws Exception {
            //clears触发时也是窗口关闭时，这里发送一个side output来发送设备离线的消息
            System.out.println("====window state" + context.window().toString());
            context.output(OUTPUT_TAG, "clear()");
        }

        @Override
        public void close() throws Exception {
            System.out.println("====close()");
        }
    }

}
