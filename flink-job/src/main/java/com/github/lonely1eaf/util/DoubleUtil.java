package com.github.lonely1eaf.util;

import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;

public class DoubleUtil {

    public static Double avg(List<Double> values) {
        OptionalDouble average = values.stream()
                .filter(Objects::nonNull)
                .mapToDouble(Double::doubleValue)
                .average();

        if (average.isPresent()) {
            return average.getAsDouble();
        } else {
            return null;
        }
    }

}
