package it.polimi.util;

import com.google.common.collect.Maps;
import it.polimi.supervisor.graph.Aggregation;

import java.util.EnumMap;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.primitives.Doubles.max;
import static com.google.common.primitives.Doubles.min;

public class AggregationFunc {
    private static EnumMap<Aggregation, Function<Stream<Double>, Optional<Double>>> map = Maps.newEnumMap(Aggregation.class);

    static {
        map.put(Aggregation.AVERAGE, (doubles) -> doubles
                .reduce((x, y) -> x + y)
                .map((value) -> value / doubles.count()));
        map.put(Aggregation.SUM, (doubles) -> doubles.reduce((x, y) -> x + y));
        map.put(Aggregation.MIN, (doubles) -> doubles.reduce((x, y) -> min(x, y)));
        map.put(Aggregation.MAX, (doubles) -> doubles.reduce((x, y) -> max(x, y)));
    }

    public static Function<Stream<Double>, Optional<Double>> getFunc(final Aggregation aggregation) {
        return map.get(aggregation);
    }
}
