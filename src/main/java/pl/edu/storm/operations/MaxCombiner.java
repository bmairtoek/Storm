package pl.edu.storm.operations;

import clojure.lang.Numbers;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class MaxCombiner implements CombinerAggregator<Number> {

    @Override
    public Number init(TridentTuple tuple) {
        return (Number) tuple.getValue(0);
    }

    @Override
    public Number combine(Number val1, Number val2) {
        return Math.max(val1.longValue(), val2.longValue());
    }

    @Override
    public Number zero() {
        return 0;
    }
}
