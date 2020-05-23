package spendreport.function;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MyProcessFunction2 extends ProcessFunction<Transaction, Transaction> {
    private static final Logger LOG = LoggerFactory.getLogger(MyProcessFunction2.class);
    @Override
    public void processElement(Transaction value, Context ctx, Collector<Transaction> out) throws Exception {
        LOG.info("[打印数据2] {}",value.toString());
        out.collect(value);
    }
}
