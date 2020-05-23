package spendreport.v;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spendreport.bounded.TransactionSourceBounded;
import spendreport.function.MyProcessFunction1;
import spendreport.function.MyProcessFunction2;


/**
 * Skeleton code for the datastream walkthrough
 * 配置文件没生效，手动设置配置，代码中设置配置
 * 方便本地地调试，否则，报超时异常
 */
public class v7 {
	private static final Logger LOG = LoggerFactory.getLogger(v7.class);

	public static void main(String[] args) throws Exception {

		int timeount = 1000 * 2000;
		Configuration configuration = new Configuration();
		configuration.setInteger("akka.ask.timeout",timeount);
		configuration.setInteger("web.timeout",timeount);
		configuration.setInteger("heartbeat.timeout",timeount);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(2,configuration);

		env.setParallelism(2);
		DataStream<Transaction> transactions = env
			.addSource(new TransactionSourceBounded())
			.name("transactions");

		SingleOutputStreamOperator<Transaction> streamOperator1= transactions.process(new MyProcessFunction1());

		streamOperator1.process(new MyProcessFunction2());


		env.execute("诈骗检测");

		//输出执行计划
		//System.out.println(env.getExecutionPlan());
		System.out.println("============完成============");



	}
}
