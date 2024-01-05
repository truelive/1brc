/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import jdk.incubator.vector.DoubleVector;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CalculateAverage_truelive {

    private static final String FILE = "./measurements.txt";
    private static final long CHUNK_SIZE = 1024 * 1024 * 10L;

    private static final Map<DankString, DankString> dankPool = new ConcurrentHashMap<>(500);

    static class DankString implements Comparable<DankString> {
        private final byte[] array;
        private final int hcode;

        public DankString(byte[] arr, int start, int len, int hash) {
            this.array = Arrays.copyOfRange(arr, start, len);
            this.hcode = hash;
        }

        public static DankString of(byte[] arr, int start, int len, int hash) {
            return new DankString(arr, start, len, hash);
        }


        @Override
        public int hashCode() {
            return this.hcode;
        }


        @Override
        public String toString() {
            return new String(this.array);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final DankString that = (DankString) o;
            return Arrays.equals(array, that.array);
        }

        @Override
        public int compareTo(DankString o) {
            if (this == o) {
                return 0;
            }
            if (o == null) {
                return 1;
            }
            return Arrays.compare(this.array, o.array);
        }
    }

    private static double getDouble(byte[] arr, int pos) {
        final int negative = ~(arr[pos] >> 4) & 1;
        int sig = 1;
        sig -= 2*negative;
        pos+=negative;
        int digit1 = arr[pos] - '0';
        pos++;
        if (arr[pos] == '.') {
            return sig*(digit1 + (arr[pos + 1] - '0') / 10.0);
        } else {
            return sig*(digit1 * 10 + (arr[pos] - '0') + (arr[pos + 2] - '0') / 10.0);
        }
    }

    private interface Measurement {
        static Measurement of(DankString keyValue, double temp) {
            return MeasurementA.of(keyValue, temp);
        }

        public DankString getKey();
        public double getMin();
        public double getMax();
        public double getSum();
        public long getCount();
        public Measurement add(final double measurment);
        public Measurement combineWith(final Measurement m) ;
    }

    private record MeasurementA(DankString key, DoubleAccumulator min, DoubleAccumulator max, DoubleAccumulator sum, LongAdder count) implements Measurement{
        public static Measurement of(DankString key, final Double initialMeasurement) {
            final MeasurementA measurement = new MeasurementA(
                    key,
                    new DoubleAccumulator(Math::min, initialMeasurement),
                    new DoubleAccumulator(Math::max, initialMeasurement),
                    new DoubleAccumulator(Double::sum, initialMeasurement),
                    new LongAdder()
            );
            measurement.count.increment();
            return measurement;
        }

        @Override
        public DankString getKey() {
            return key;
        }

        @Override
        public double getMin() {
            return min.doubleValue();
        }

        @Override
        public double getMax() {
            return max.doubleValue();
        }

        @Override
        public double getSum() {
            return sum.doubleValue();
        }

        @Override
        public long getCount() {
            return count.sum();
        }

        @Override
        public Measurement add(final double measurment) {
            min.accumulate(measurment);
            max.accumulate(measurment);
            sum.accumulate(measurment);
            count.increment();
            return this;
        }

        public String toString() {
            return round(min.doubleValue()) +
                   "/" +
                   round(sum.doubleValue() / count.sum()) +
                   "/" +
                   round(max.doubleValue());
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public Measurement combineWith(final Measurement m) {
            MeasurementA m1 = this;
            if (!Objects.equals(m1.key, m.getKey())) {
                throw new IllegalArgumentException("Merging Mismached entries "+new String(m1.key.array)+" != "+ new String(m.getKey().array));
            }
            m1.min.accumulate(m.getMin());
            m1.max.accumulate(m.getMax());
            m1.sum.accumulate(m.getSum());
            m1.count.add(m.getCount());
            return new MeasurementA(
                    m1.key,
                    m1.min,
                    m1.max,
                    m1.sum,
                    m1.count
            );
        }

    }

    static class MeasurementB implements Measurement{
        DankString key; double min; double max; double sum; long count;

        public MeasurementB(
                DankString key,
                Double min,
                Double max,
                Double sum,
                long count
        ) {
            this.key = key;
            this.max = max;
            this.min = min;
            this.sum = sum;
            this.count = count;
        }

        public static Measurement of(DankString key, final Double initialMeasurement) {
            final MeasurementB measurement = new MeasurementB(
                    key,
                    initialMeasurement,
                    initialMeasurement,
                    initialMeasurement,
                    1
            );
            return measurement;
        }

        @Override
        public DankString getKey() {
            return key;
        }

        @Override
        public double getMin() {
            return min;
        }

        @Override
        public double getMax() {
            return max;
        }

        @Override
        public double getSum() {
            return sum;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public Measurement add(final double measurment) {
            min = Math.min(min, measurment);
            max = Math.max(max, measurment);
            sum += measurment;
            count++;
            return this;
        }

        public String toString() {
            return round(min) +
                   "/" +
                   round(sum/ count) +
                   "/" +
                   round(max);
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public Measurement combineWith(final Measurement m) {
            MeasurementB m1 = this;
            if (!Objects.equals(m1.key, m.getKey())) {
                throw new IllegalArgumentException("Merging Mismached entries "+new String(m1.key.array)+" != "+ new String(m.getKey().array));
            }

            return new MeasurementB(
                    m1.key,
                    m1.min + m.getMin(),
                    m1.max + m.getMax(),
                    m1.sum + m.getSum(),
                    m1.count + m.getCount()
            );
        }

    }

    static class MeasurementD implements Measurement{
        DankString key; double min; double max; double sum; long count;

        public MeasurementD(
                DankString key,
                Double min,
                Double max,
                Double sum,
                long count
        ) {
            this.key = key;
            this.max = max;
            this.min = min;
            this.sum = sum;
            this.count = count;
        }

        public static Measurement of(DankString key, final Double initialMeasurement) {
            ByteBuffer minBB = ByteBuffer.allocate(128);
            MemorySegment ms = MemorySegment.ofBuffer(minBB);
            DoubleVector minV = DoubleVector.fromMemorySegment(DoubleVector.SPECIES_128, ms,0, ByteOrder.nativeOrder());
            final MeasurementB measurement = new MeasurementB(
                    key,
                    initialMeasurement,
                    initialMeasurement,
                    initialMeasurement,
                    1
            );
            return measurement;
        }

        @Override
        public DankString getKey() {
            return key;
        }

        @Override
        public double getMin() {
            return min;
        }

        @Override
        public double getMax() {
            return max;
        }

        @Override
        public double getSum() {
            return sum;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public Measurement add(final double measurment) {
            min = Math.min(min, measurment);
            max = Math.max(max, measurment);
            sum += measurment;
            count++;
            return this;
        }

        public String toString() {
            return round(min) +
                   "/" +
                   round(sum/ count) +
                   "/" +
                   round(max);
        }

        private double round(final double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        @Override
        public Measurement combineWith(final Measurement m) {
            MeasurementD m1 = this;
            if (!Objects.equals(m1.key, m.getKey())) {
                throw new IllegalArgumentException("Merging Mismached entries "+new String(m1.key.array)+" != "+ new String(m.getKey().array));
            }

            return new MeasurementB(
                    m1.key,
                    m1.min + m.getMin(),
                    m1.max + m.getMax(),
                    m1.sum + m.getSum(),
                    m1.count + m.getCount()
            );
        }

    }


    public static void main(final String[] args) throws IOException {
        //long before = System.currentTimeMillis();
        /**
         * Shoutout to bjhara
         */
        final Iterator<ByteBuffer> iterator = new Iterator<>() {
            final FileChannel in = new FileInputStream(FILE).getChannel();
            final long total = in.size();
            long start;

            @Override
            public boolean hasNext() {
                return start < total;
            }

            @Override
            public ByteBuffer next() {
                try {
                    final MappedByteBuffer mbb = in.map(
                            FileChannel.MapMode.READ_ONLY,
                            start,
                            Math.min(CHUNK_SIZE, total - start)
                    );
                    int realEnd = mbb.limit() - 1;
                    while (mbb.get(realEnd) != '\n') {
                        realEnd--;
                    }

                    realEnd++;
                    mbb.limit(realEnd);
                    start += realEnd;

                    return mbb;
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        final Map<String, Measurement> reduce = StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                                                               iterator, Spliterator.IMMUTABLE), true)
                                                             //.parallel()
                                                             .map(CalculateAverage_truelive::parseBuffer)
                .flatMap((DankMap dankMap) -> dankMap.getAll().stream())
                .collect(Collectors.toMap(k -> k.getKey().toString(), v->v, Measurement::combineWith, TreeMap::new));

        System.out.print("{");
        System.out.print(
                reduce
                       );
        System.out.println("}");

        //System.out.println("Took: " + (System.currentTimeMillis() - before));

    }

    private static DankMap parseBuffer(final ByteBuffer bug) {

        final DankMap resultMap = new DankMap();
        bug.mark();
        DankString name = null;
        final byte[] arr = new byte[128];
        int cur = 0;
        int hash = 0;
        while (bug.hasRemaining()) {
            char c = (char) bug.get();
            arr[cur++]= (byte) c;
            while (c != ';') {
                hash += 31*hash + c;
                c = (char) bug.get();
                arr[cur++]= (byte) c;
            }
            name = DankString.of(arr,0,cur-1, hash);
            cur = 0;
            hash = 0;
            while (c != '\n') {
                c = (char) bug.get();
                arr[cur++]= (byte) c;
            }
            final double temp = getDouble(arr, 0);
            resultMap.putOrDefault(name, temp);
            cur = 0;
        }
        return resultMap;
    }

    static class  DankMap  {
        private final static int MAP_SIZE = 1024*128;
        private final Measurement[] values = new Measurement[MAP_SIZE];
        private final DankString[] keys = new DankString[MAP_SIZE];

        public void putOrDefault(DankString keyValue, double temp) {
            int pos = keyValue.hcode & (values.length - 1);
            final byte[] key = keyValue.array;
            Measurement value = values[pos];
            while (value != null && key.length != keys[pos].array.length && Arrays.equals(key, keys[pos].array)) {
                pos = (pos + 1) & (values.length - 1);
                value = values[pos];
            }
            if (value == null) {
                keys[pos] = keyValue;
                values[pos] = Measurement.of(keyValue, temp);
            } else {
                value.add(temp);
            }
        }

        public List<Measurement> getAll() {
            return Arrays.stream(values).filter(Objects::nonNull).toList();
        }
    }
}
