package com.github.lonely1eaf;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Timer;
import java.util.TimerTask;

@Slf4j
public class TimerJob {

    private static final OutputTag<String> OUTPUT_TAG = new OutputTag<String>("clear-tag", TypeInformation.of(String.class));

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        //设备是否在线的判断是使用处理报文的时间！因为设备可能有离线报文
        see.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        //这种写法，process时，是会收到该window的所有element
        //也就是窗口关闭是才进行process
        final SingleOutputStreamOperator<String> streamOperator = see.addSource(new TimerSource())
                .keyBy(value -> Long.valueOf(1))
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .process(new CountWithTimeoutFunction());

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
     * The implementation of the ProcessFunction that maintains the count and timeouts
     */
    public static class CountWithTimeoutFunction
            extends ProcessWindowFunction<Long, String, Long, TimeWindow> {

        /**
         * The state that is maintained by this process function
         */
        private ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", Long.class));
        }

        @Override
        public void process(Long aLong, Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
            for (Long element : elements) {
                System.out.println("====循环中");
                out.collect("key:" + aLong + " element:" + element);
            }
        }

        @Override
        public void clear(Context context) throws Exception {
            super.clear(context);
            System.out.println("====window state"+context.window().toString());
            context.output(OUTPUT_TAG, "clear()");
        }

        @Override
        public void close() throws Exception {
            System.out.println("====close()");
        }
    }

}
