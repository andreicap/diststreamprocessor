package it.polimi.util;

import com.google.common.collect.Maps;
import it.polimi.supervisor.graph.Aggregation;

import java.util.EnumMap;
import java.util.OptionalDouble;
import java.util.function.Function;
import java.util.stream.DoubleStream;

public class AggregationFunc {
    private static EnumMap<Aggregation, Function<DoubleStream, OptionalDouble>> map = Maps.newEnumMap(Aggregation.class);

    static {
        map.put(Aggregation.AVERAGE, DoubleStream::average);
        map.put(Aggregation.SUM, (doubles) -> doubles.reduce((x, y) -> x + y));
        map.put(Aggregation.MIN, DoubleStream::min);
        map.put(Aggregation.MAX, DoubleStream::max);
    }

    public static Function<DoubleStream, OptionalDouble> getFunc(final Aggregation aggregation) {
        return map.get(aggregation);
    }
}
