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

package org.apache.kyuubi.engine.spark.monitor

import java.lang.reflect.{Field, Modifier, ParameterizedType, Type}

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * This class is used for generating schema and row by reflection.
 */
object DataFrameUtil {

  /**
   * This map store the mapping relationship between Class and StructType.
   * Each class will only generate its structType once.
   */
  private val classToStructTypeMap = new java.util.HashMap[Class[_], Option[StructType]]()
  /**
   * This map store the mapping relationship between sqlType
   * and the field's type
   */
  private val sqlTypeToFieldTypeMap = new java.util.HashMap[Type, Option[DataType]]
  private val scalaIterableClassSet = collection.mutable.Set[Class[_]]()

  private val predefinedDataType: collection.mutable.Map[Class[_], DataType] =
    collection.mutable.Map(
      (classOf[Int], IntegerType),
      (classOf[java.lang.Integer], IntegerType),
      (classOf[Long], LongType),
      (classOf[java.lang.Long], LongType),
      (classOf[String], StringType),
      (classOf[Any], StringType))

  private val classConverter: Map[Class[_], (Any) => _ <: Any] =
    Map(
      classOf[Char] ->
        ((o: Any) => o.asInstanceOf[Char].toString),
      classOf[java.lang.Character] ->
        ((o: Any) => o.asInstanceOf[java.lang.Character].toString),
      classOf[Array[Char]] ->
        ((o: Any) => new String(o.asInstanceOf[Array[Char]])),
      classOf[Array[java.lang.Character]] ->
        ((o: Any) => new String(o.asInstanceOf[Array[java.lang.Character]].map(_.charValue))),
      classOf[Any] ->
        ((o: Any) => o.toString))

  /**
   * Returns the StructType that the class you give.
   *
   * @param clazz
   * @return
   */
  def getStructType(clazz: Class[_]): Option[StructType] = {
    val cachedStructType = classToStructTypeMap.get(clazz)
    if (cachedStructType == null) {
      val fields = getFields(clazz)
      val newStructType =
        if (fields.isEmpty) {
          None
        } else {
          val types = fields.map(f => {
            val dataType = getDataType(f.getGenericType).get
            StructField(f.getName, dataType, true)
          })
          if (types.isEmpty) {
            None
          } else {
            Some(StructType(types))
          }
        }
      classToStructTypeMap.put(clazz, newStructType)
      return newStructType
    } else {
      return cachedStructType.asInstanceOf[Option[StructType]]
    }
  }

  def getFields(clazz: Class[_]): Array[Field] = {
    val fields = clazz.getDeclaredFields
      .filterNot(f => Modifier.isTransient(f.getModifiers))
      .flatMap(f =>
        getDataType(f.getGenericType) match {
          case Some(_) => Some(f)
          case None => None
        })
    fields.foreach(_.setAccessible(true))
    return fields
  }

  /**
   * Returns the Row that the object you give.
   *
   * @param clazz
   * @param any
   * @return
   */
  def getRow(clazz: Class[_], any: Any): Option[Row] = {
    getStructType(clazz) match {
      case Some(_) =>
        if (any == null) {
          Some(null)
        } else {
          Some(Row(getFields(clazz).flatMap(f => getCell(f.getGenericType, f.get(any))): _*))
        }
      case None => None
    }
  }

  private def getDataType(tp: java.lang.reflect.Type): Option[DataType] = {
    val cachedDataType = sqlTypeToFieldTypeMap.get(tp)
    if (cachedDataType == null) {
      val newDataType: Option[DataType] = tp match {
        // ParameterizedType represents a parameterized type such as Collection<String>
        case ptp: ParameterizedType =>
          val clazz = ptp.getRawType.asInstanceOf[Class[_]]
          val rowTypes = ptp.getActualTypeArguments
          if (isScalaIterableClass(clazz)) {
            getDataType(rowTypes(0)) match {
              case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
              case None => None
            }
          } else {
            getStructType(clazz)
          }
        case clazz: Class[_] =>
          predefinedDataType.get(clazz) match {
            case Some(tp) => Some(tp)
            case None =>
              if (clazz.isArray) {
                getDataType(clazz.getComponentType) match {
                  case Some(dataType) => Some(DataTypes.createArrayType(dataType, true))
                  case None => None
                }
              } else if (isScalaIterableClass(clazz)) {
                Some(DataTypes.createArrayType(StringType, true))
              } else {
                getStructType(clazz)
              }
          }
        case _ =>
          throw new IllegalArgumentException("")
      }
      sqlTypeToFieldTypeMap.put(tp, newDataType)
      return newDataType
    } else {
      return cachedDataType.asInstanceOf[Option[DataType]]
    }
  }

  private def getCell(tp: java.lang.reflect.Type, value: Any): Option[Any] = {
    tp match {
      case ptp: ParameterizedType =>
        val clazz = ptp.getRawType.asInstanceOf[Class[_]]
        val rowTypes = ptp.getActualTypeArguments
        if (isScalaIterableClass(clazz)) {
          getDataType(rowTypes(0)) match {
            case Some(_) =>
              if (value == null) {
                Some(null)
              } else {
                Some(value.asInstanceOf[Iterable[Any]].filter(_ != null)
                  .map(v => getCell(rowTypes(0), v).get.toString).toSeq)
              }
            case None => None
          }
        } else {
          getCell(clazz, value)
        }
      case clazz: Class[_] =>
        predefinedDataType.get(clazz) match {
          case Some(_) =>
            classConverter.get(clazz) match {
              case Some(converter) => Some(if (value == null) null else converter(value))
              case None => Some(value)
            }
          case None =>
            if (clazz.isArray) {
              getDataType(clazz.getComponentType) match {
                case Some(dataType) =>
                  if (value == null) {
                    Some(null)
                  }
                  else {
                    Some(value.asInstanceOf[Array[_]].filter(_ != null)
                      .flatMap(v => getCell(clazz.getComponentType, v)).toSeq)
                  }
                case None => None
              }
            } else if (isScalaIterableClass(clazz)) {
              Some(value.asInstanceOf[Iterable[Any]].filter(_ != null)
                .map(v => getCell(classOf[Any], v).get).toSeq)
            } else {
              getRow(clazz, value)
            }
        }
      case _ =>
        throw new IllegalArgumentException("")
    }
  }

  def isScalaIterableClass(clazz: Class[_]): Boolean = {
    if (scalaIterableClassSet.contains(clazz)) {
      return true
    } else if (classOf[Iterable[_]].isAssignableFrom(clazz)) {
      scalaIterableClassSet += clazz
      return true
    } else {
      return false
    }
  }
}
