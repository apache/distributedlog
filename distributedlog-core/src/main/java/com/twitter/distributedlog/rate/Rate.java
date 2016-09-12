// =================================================================================================
// Copyright 2011 Twitter, Inc.
// -------------------------------------------------------------------------------------------------
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this work except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file, or at:
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =================================================================================================
package com.twitter.distributedlog.rate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Ticker;
import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Function to compute a windowed per-second rate of a value.
 *
 * {@link https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/stats/Rate.java}
 */
public class Rate<T extends Number> {

    private static final int DEFAULT_WINDOW_SIZE = 1;
    private static final double DEFAULT_SCALE_FACTOR = 1;
    private static final long NANOS_PER_SEC = TimeUnit.SECONDS.toNanos(1);

    private final Supplier<T> inputAccessor;
    private final Ticker ticker;
    private final double scaleFactor;

    private final LinkedBlockingDeque<Pair<Long, Double>> samples;

    private Rate(Supplier<T> inputAccessor,
                 int windowSize,
                 double scaleFactor,
                 Ticker ticker) {
        this.inputAccessor = Preconditions.checkNotNull(inputAccessor);
        this.ticker = Preconditions.checkNotNull(ticker);
        samples = new LinkedBlockingDeque<Pair<Long, Double>>(windowSize);
        Preconditions.checkArgument(scaleFactor != 0, "Scale factor must be non-zero!");
        this.scaleFactor = scaleFactor;
    }

    public static Builder<AtomicLong> of(AtomicLong input) {
        return new Builder<AtomicLong>(input);
    }

    public Double doSample() {
        T newSample = inputAccessor.get();
        long newTimestamp = ticker.read();

        double rate = 0;
        if (!samples.isEmpty()) {
            Pair<Long, Double> oldestSample = samples.peekLast();

            double dy = newSample.doubleValue() - oldestSample.getRight();
            double dt = newTimestamp - oldestSample.getLeft();
            rate = dt == 0 ? 0 : (NANOS_PER_SEC * scaleFactor * dy) / dt;
        }

        if (samples.remainingCapacity() == 0) samples.removeLast();
        samples.addFirst(Pair.of(newTimestamp, newSample.doubleValue()));

        return rate;
    }

    public static class Builder<T extends Number> {

        private int windowSize = DEFAULT_WINDOW_SIZE;
        private double scaleFactor = DEFAULT_SCALE_FACTOR;
        private Supplier<T> inputAccessor;
        private Ticker ticker = Ticker.systemTicker();

        Builder(final T input) {
            inputAccessor = Suppliers.ofInstance(input);
        }

        public Builder<T> withWindowSize(int windowSize) {
            this.windowSize = windowSize;
            return this;
        }

        public Builder<T> withScaleFactor(double scaleFactor) {
            this.scaleFactor = scaleFactor;
            return this;
        }

        @VisibleForTesting
        Builder<T> withTicker(Ticker ticker ) {
            this.ticker = ticker;
            return this;
        }

        public Rate<T> build() {
            return new Rate<T>(inputAccessor, windowSize, scaleFactor, ticker);
        }
    }
}
