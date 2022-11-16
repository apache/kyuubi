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
package org.apache.kyuubi.jdbc.hive.arrow;

import org.apache.hive.service.rpc.thrift.TTypeId;

abstract class CatalogType {
  String name;

  public TTypeId toTTypeId() {
    switch (name) {
      case "void":
        return TTypeId.NULL_TYPE;
      case "int":
        return TTypeId.INT_TYPE;
      case "array":
        return TTypeId.ARRAY_TYPE;
      default:
        throw new IllegalStateException("unsupport name: " + name);
    }
  }
}

class SimpleType extends CatalogType {
  public SimpleType() {}

  public SimpleType(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return name;
  }
}

class ArrayType extends CatalogType {
  CatalogType elementType;

  public ArrayType(CatalogType elementType) {
    this.name = "array";
    this.elementType = elementType;
  }

  @Override
  public String toString() {
    return "array (" + elementType.toString() + ")";
  }
}

class MapType extends CatalogType {
  CatalogType keyType;
  CatalogType valueType;

  MapType() {}

  MapType(CatalogType keyType, CatalogType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public String toString() {
    return "array (" + keyType.toString() + "," + valueType.toString() + ")";
  }
}
