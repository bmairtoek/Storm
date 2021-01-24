package pl.edu.storm;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class SumAggregator extends BaseAggregator<SumAggregator.SumState> {

    static class SumState {
        long sum = 0;
    }


    @Override
    public SumState init(Object o, TridentCollector tridentCollector) {
        return new SumState();
    }

    @Override
    public void aggregate(SumState sumState, TridentTuple tridentTuple, TridentCollector tridentCollector) {
        sumState.sum += Long.parseLong(tridentTuple.get(0).toString());
    }

    @Override
    public void complete(SumState sumState, TridentCollector tridentCollector) {
        tridentCollector.emit(new Values(sumState.sum));
    }

}
