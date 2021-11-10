/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.engine.flink.result;

import java.util.Iterator;

public class IterableFetchIterator<T> implements FetchIterator<T> {

  private Iterable<T> iterable;
  private Iterator<T> iterator;

  private long fetchStart = 0;
  private long position = 0;

  public IterableFetchIterator(Iterable<T> iterable) {
    this.iterable = iterable;
    this.iterator = iterable.iterator();
  }

  @Override
  public void fetchNext() {
    fetchStart = position;
  }

  @Override
  public void fetchAbsolute(long pos) {
    int newPos = Math.max((int) pos, 0);
    if (newPos < position) {
      resetPosition();
    }

    while (position < newPos && hasNext()) {
      next();
    }

    fetchStart = position;
  }

  @Override
  public long getFetchStart() {
    return fetchStart;
  }

  @Override
  public long getPosition() {
    return position;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public T next() {
    position += 1;
    return iterator.next();
  }

  private void resetPosition() {
    if (position != 0) {
      iterator = iterable.iterator();
      position = 0;
      fetchStart = 0;
    }
  }
}
