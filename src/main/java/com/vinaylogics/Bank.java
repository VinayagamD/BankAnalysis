package com.vinaylogics;

import com.vinaylogics.dto.AlarmedCustomer;
import com.vinaylogics.dto.LostCard;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class Bank {

    public static final MapStateDescriptor<String, AlarmedCustomer> alarmedCustStateDescriptor =
            new MapStateDescriptor<>("alarmed_Customers", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(AlarmedCustomer.class));
    public static final MapStateDescriptor<String, LostCard> lostCardStateDescriptor =
            new MapStateDescriptor<>("lost_cards", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(LostCard.class));

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> alarmedCustomerSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),  
                        new Path("/home/vinaylogics/alarmed_cust.txt"))
                .build();

        DataStream<AlarmedCustomer> alarmedCustomers = env.fromSource(alarmedCustomerSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        ).map((MapFunction<String, AlarmedCustomer>) AlarmedCustomer::new);

        // Broadcast alarmed customer data
        BroadcastStream<AlarmedCustomer> alarmedCustBroadcast = alarmedCustomers.broadcast(alarmedCustStateDescriptor);


        FileSource<String> lostCardSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path("/home/vinaylogics/lost_cards.txt"))
                .build();

        DataStream<LostCard> lostCard = env.fromSource(lostCardSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        ).map((MapFunction<String, LostCard>) LostCard::new);

        // Broadcast alarmed customer data
        BroadcastStream<LostCard> lostCardBroadcast = lostCard.broadcast(lostCardStateDescriptor);

        // transaction data keyed by customer_id
        DataStream<Tuple2<String, String>> data = env.socketTextStream("localhost", 9090)
                .map((MapFunction<String, Tuple2<String, String>>) value -> {
                    String[] words = value.split(",");
                    return new Tuple2<>(words[3], value);  //{(id_347hfx) (HFXR347924,2018-06-14 23:32:23,Chandigarh,id_347hfx,hf98678167,123302773033,774
                }).returns(new TupleTypeInfo<>(Types.STRING, Types.STRING));


        DataStream<Tuple2<String,String>> alarmedCustTransactions = data
                .keyBy(f -> f.f0)
                        .connect(alarmedCustBroadcast)
                                .process(new AlarmedCustCheck()).returns(new TupleTypeInfo<>(Types.STRING, Types.STRING));

        DataStream<Tuple2<String,String>> lostCardTransactions = data
                .keyBy(f -> f.f0)
                        .connect(lostCardBroadcast)
                                .process(new LostCardCheck()).returns(new TupleTypeInfo<>(Types.STRING, Types.STRING));


        DataStream<Tuple2<String,String>> excessiveTransactions = data
                .map((MapFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>) value -> new Tuple3<>(value.f0, value.f1, 1)).returns(new TupleTypeInfo<>(Types.STRING,Types.STRING,Types.INT))
                .keyBy(f-> f.f0)
                        .window(TumblingProcessingTimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
                                .sum(2)
                                        .flatMap(new FilterAndMapMoreThan10());

        DataStream<Tuple2<String,String>> freqCityChangeTransactions = data
                .keyBy(f -> f.f0)
                        .window(TumblingProcessingTimeWindows.of(Duration.of(10, ChronoUnit.SECONDS)))
                                .process(new CityChange());
        DataStream<Tuple2<String,String>> allFlaggedTxn =
                alarmedCustTransactions.union(lostCardTransactions,excessiveTransactions,freqCityChangeTransactions);
        allFlaggedTxn.addSink(StreamingFileSink.
                forRowFormat(new Path("/home/vinaylogics/flagged_transaction"),
                        new SimpleStringEncoder<Tuple2<String,String>>(StandardCharsets.UTF_8.name()))
                .withRollingPolicy(DefaultRollingPolicy.builder().build())
                        .build());




       // execute program
        env.execute("Streaming Bank");

    }

    public static class AlarmedCustCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String,String>, AlarmedCustomer,
            Tuple2<String, String>> {
        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String,
                Tuple2<String, String>, AlarmedCustomer, Tuple2<String, String>>.ReadOnlyContext ctx,
                                   Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(alarmedCustStateDescriptor).immutableEntries().forEach(entry -> {
                final String alarmCustId = entry.getKey();
                final AlarmedCustomer cust = entry.getValue();

                // get customer_id of current transaction
                final String tId = value.f1.split(",")[3];
                if (tId.equals(alarmCustId)) {
                    out.collect(new Tuple2<>("____ALARM___",
                            "Transaction: "+value+" is by an ALARMED customer"));
                }
            });
        }

        @Override
        public void processBroadcastElement(AlarmedCustomer cust, KeyedBroadcastProcessFunction<String, Tuple2<String, String>,
                AlarmedCustomer, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(alarmedCustStateDescriptor).put(cust.id,cust);
        }
    }

    private static class LostCardCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String,String>, LostCard, Tuple2<String,String>> {

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, LostCard,
                Tuple2<String, String>>.ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(lostCardStateDescriptor).immutableEntries().forEach(entry -> {
               final String lostCardId = entry.getKey();
               final LostCard card = entry.getValue();

               // get card_id of current transaction
                final String tId = value.f1.split(",")[5];
                if (tId.equals(lostCardId)) {
                    out.collect(new Tuple2<>("__ALARM__", "Transaction: "+value+" issued via LOST card"));
                }
            });

        }

        @Override
        public void processBroadcastElement(LostCard card, KeyedBroadcastProcessFunction<String, Tuple2<String, String>,
                LostCard, Tuple2<String, String>>.Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            ctx.getBroadcastState(lostCardStateDescriptor).put(card.id, card);

        }
    }

    public static class CityChange extends ProcessWindowFunction<Tuple2<String,String>, Tuple2<String,String>, String, TimeWindow> {
        @Override
        public void process(String key, ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>.Context ctx,
                            Iterable<Tuple2<String, String>> input, Collector<Tuple2<String, String>> out) throws Exception {
            String lastCity = "";
            int changeCount = 0;
            for (Tuple2<String, String> element : input) {
                String city = element.f1.split(",")[2].toLowerCase();
                if (lastCity.isEmpty()) {
                    lastCity = city;
                } else {
                    if (!city.equals(lastCity)) {
                        lastCity = city;
                        changeCount++;
                    }
                }
                if (changeCount >= 2) {
                    out.collect(new Tuple2<>("__ALARM__", element+" marked for FREQUENT city changes"));
                }
            }

        }
    }

    public static class FilterAndMapMoreThan10 implements FlatMapFunction<Tuple3<String,String,Integer>, Tuple2<String,String>> {
        @Override
        public void flatMap(Tuple3<String, String, Integer> value, Collector<Tuple2<String, String>> out) throws Exception {
            if (value.f2 > 10) {
                out.collect(new Tuple2<>("__ALARM__", value+" marked for >10 TXNs"));
            }
        }
    }
}
