package com.github.lonely1eaf.opt;

import com.github.lonely1eaf.util.JsonUtil;
import org.apache.flink.api.common.functions.MapFunction;

public class JsonMapFunction<T> implements MapFunction<T, String> {

    public static <T> JsonMapFunction<T> newInstance() {
        return new JsonMapFunction<>();
    }

    @Override
    public String map(T value) throws Exception {
        return JsonUtil.INSTANCE.toJson(value);
    }


}
