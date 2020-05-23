/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spendreport.bounded;

import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.source.FromIteratorFunction;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * A stream of transactions.
 */
@Public
public class TransactionSourceBounded extends FromIteratorFunction<Transaction> {

	private static final long serialVersionUID = 1L;

	public TransactionSourceBounded() {
		super(new RateLimitedIterator<>(TransactionIterator.bounded()));
	}

	private static class RateLimitedIterator<T> implements Iterator<T>, Serializable {

		private static final long serialVersionUID = 1L;

		private final Iterator<T> inner;

		private RateLimitedIterator(Iterator<T> inner) {
			this.inner = inner;
		}

		@Override
		public boolean hasNext() {
			return inner.hasNext();
		}

		@Override
		public T next() {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			return inner.next();
		}
	}
}
