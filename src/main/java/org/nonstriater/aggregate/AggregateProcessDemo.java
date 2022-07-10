package org.nonstriater.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregateProcessDemo {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 读取数据
        DataStream<Tuple3<String, String, Long>> input = env.fromElements(ENGLISH);

        // 求各个班级英语成绩平均分
        DataStream<Double> avgScore = input.
                keyBy(0).
                countWindow(3).
                aggregate(new AverageAggrate());

        avgScore.print();
        env.execute("TestAggFunctionOnWindow");


    }

    public static final Tuple3[] ENGLISH = new Tuple3[] {
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L),
    };

    //Tuple3<String, String, Long> 输入类型
    //Tuple2<Long, Long> 累加器ACC类型，保存中间状态
    //Double 输出类型
    public static class AverageAggrate implements AggregateFunction<Tuple3<String, String, Long>, Tuple2<Long, Long>, Double> {

        /**
         * 创建累加器保存中间状态（sum count）
         * sum 英语总成绩
         * count 学生个数
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }


        /**
         * 将元素添加到累加器并返回新的累加器
         * @param value 输入类型
         * @param acc 累加器ACC类型
         * @return 返回新的累加器
         */
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> acc) {
            //acc.f0 总成绩
            //value.f2 表示成绩
            //acc.f1 人数
            return new Tuple2<>(acc.f0 + value.f2, acc.f1 + 1L);
        }


        /**
         * 从累加器提取结果
         * @param acc
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> acc) {
            return ((double) acc.f0) / acc.f1;
        }


        /**
         * 累加器合并
         * @param acc2
         * @param acc1
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }

}
