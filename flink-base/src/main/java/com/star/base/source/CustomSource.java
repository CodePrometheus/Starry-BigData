package com.star.base.source;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;

/**
 * 自定义 Source
 *
 * @Author: Starry
 * @Date: 11-14-2022 19:45
 */
public class CustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2)
                .addSource(new CustomRichImpl())
                .map(JSON::toJSONString)
                .print();
        env.execute();
    }
}

class CustomSourceImpl implements SourceFunction<Log> {
    volatile boolean flag = true;

    @Override
    public void run(SourceContext ctx) throws InterruptedException {
        // 不断被调用 run()
        Log log = new Log();
        while (flag) {
            log.setUid(RandomUtils.nextLong(1, 1000))
                    .setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase())
                    .setTime(new Date());
            ctx.collect(log);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

class CustomRichImpl extends RichParallelSourceFunction<Log> {
    volatile boolean flag = true;

    @Override
    public void open(Configuration parameters) {
        RuntimeContext ctx = getRuntimeContext();
        String taskName = ctx.getTaskName();
        System.out.println("taskName = " + taskName);
        int subtask = ctx.getIndexOfThisSubtask();
        System.out.println("subtask = " + subtask);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        Log log = new Log();
        while (flag) {
            log.setUid(RandomUtils.nextLong(1, 1000))
                    .setSessionId(RandomStringUtils.randomAlphabetic(12).toUpperCase())
                    .setTime(new Date());
            ctx.collect(log);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
@Accessors(chain = true)
class Log {
    private Long uid;
    private String sessionId;
    private Date time;
}
