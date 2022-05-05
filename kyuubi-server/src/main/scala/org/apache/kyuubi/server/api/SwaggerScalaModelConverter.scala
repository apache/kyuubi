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

package org.apache.kyuubi.server.api

// scalastyle:off
import java.lang.annotation.Annotation
import java.lang.reflect.ParameterizedType
import java.util.Iterator

import scala.language.existentials
import scala.util.Try
import scala.util.control.NonFatal

import com.fasterxml.jackson.databind.`type`.ReferenceType
import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration}
import com.fasterxml.jackson.module.scala.introspect.{BeanIntrospector, PropertyDescriptor}
import io.swagger.v3.core.converter._
import io.swagger.v3.core.jackson.ModelResolver
import io.swagger.v3.core.util.{Json, PrimitiveType}
import io.swagger.v3.oas.annotations.Parameter
import io.swagger.v3.oas.annotations.media.{ArraySchema, Schema => SchemaAnnotation}
import io.swagger.v3.oas.models.media.Schema
import org.slf4j.LoggerFactory

/**
 * Copied from https://github.com/swagger-akka-http/swagger-scala-module
 */
class AnnotatedTypeForOption extends AnnotatedType

object SwaggerScalaModelConverter {
  val objectMapper = Json.mapper().registerModule(DefaultScalaModule)
}

class SwaggerScalaModelConverter extends ModelResolver(SwaggerScalaModelConverter.objectMapper) {
  SwaggerScalaModelConverter

  private val logger = LoggerFactory.getLogger(classOf[SwaggerScalaModelConverter])
  private val EnumClass = classOf[scala.Enumeration]
  private val OptionClass = classOf[scala.Option[_]]
  private val IterableClass = classOf[scala.collection.Iterable[_]]
  private val SetClass = classOf[scala.collection.Set[_]]
  private val BigDecimalClass = classOf[BigDecimal]
  private val BigIntClass = classOf[BigInt]
  private val ProductClass = classOf[Product]
  private val AnyClass = classOf[Any]

  override def resolve(
      `type`: AnnotatedType,
      context: ModelConverterContext,
      chain: Iterator[ModelConverter]): Schema[_] = {
    val javaType = _mapper.constructType(`type`.getType)
    val cls = javaType.getRawClass

    matchScalaPrimitives(`type`, cls).getOrElse {
      // Unbox scala options
      val annotatedOverrides = getRequiredSettings(`type`)
      if (_isOptional(`type`, cls)) {
        val baseType =
          if (annotatedOverrides.headOption.getOrElse(false)) new AnnotatedType()
          else new AnnotatedTypeForOption()
        resolve(nextType(baseType, `type`, javaType), context, chain)
      } else if (!annotatedOverrides.headOption.getOrElse(true)) {
        resolve(nextType(new AnnotatedTypeForOption(), `type`, javaType), context, chain)
      } else if (isCaseClass(cls)) {
        caseClassSchema(cls, `type`, context, chain).getOrElse(None.orNull)
      } else if (chain.hasNext) {
        val nextResolved = Option(chain.next().resolve(`type`, context, chain))
        nextResolved match {
          case Some(property) => {
            if (isIterable(cls)) {
              property.setRequired(null)
              property.setProperties(null)
            }
            setRequired(`type`)
            property
          }
          case None => None.orNull
        }
      } else {
        None.orNull
      }
    }
  }

  private def caseClassSchema(
      cls: Class[_],
      `type`: AnnotatedType,
      context: ModelConverterContext,
      chain: Iterator[ModelConverter]): Option[Schema[_]] = {
    if (chain.hasNext) {
      Option(chain.next().resolve(`type`, context, chain)).map { schema =>
        val introspector = BeanIntrospector(cls)
        introspector.properties.foreach { property =>
          getPropertyAnnotations(property) match {
            case Seq() => {
              val propertyClass = getPropertyClass(property)
              val optionalFlag = isOption(propertyClass)
              if (optionalFlag && schema.getRequired != null && schema.getRequired.contains(
                  property.name)) {
                schema.getRequired.remove(property.name)
              } else if (!optionalFlag) {
                addRequiredItem(schema, property.name)
              }
            }
            case annotations => {
              val required = getRequiredSettings(annotations).headOption
                .getOrElse(!isOption(getPropertyClass(property)))
              if (required) addRequiredItem(schema, property.name)
            }
          }
        }
        schema
      }
    } else {
      None
    }
  }

  private def getRequiredSettings(annotatedType: AnnotatedType): Seq[Boolean] =
    annotatedType match {
      case _: AnnotatedTypeForOption => Seq.empty
      case _ => getRequiredSettings(nullSafeList(annotatedType.getCtxAnnotations))
    }

  private def getRequiredSettings(annotations: Seq[Annotation]): Seq[Boolean] = {
    annotations.collect {
      case p: Parameter => p.required()
      case s: SchemaAnnotation => s.required()
      case a: ArraySchema => a.arraySchema().required()
    }
  }

  private def matchScalaPrimitives(
      `type`: AnnotatedType,
      nullableClass: Class[_]): Option[Schema[_]] = {
    val annotations = Option(`type`.getCtxAnnotations).map(_.toSeq).getOrElse(Seq.empty)
    annotations.collectFirst { case ann: SchemaAnnotation => ann } match {
      case Some(_) => None
      case _ => {
        annotations.collectFirst { case ann: JsonScalaEnumeration => ann } match {
          case Some(enumAnnotation: JsonScalaEnumeration) => {
            val pt = enumAnnotation.value().getGenericSuperclass.asInstanceOf[ParameterizedType]
            val args = pt.getActualTypeArguments
            val cls = args(0).asInstanceOf[Class[_]]
            val sp: Schema[String] =
              PrimitiveType.STRING.createProperty().asInstanceOf[Schema[String]]
            setRequired(`type`)
            try {
              val mainClass = getMainClass(cls)
              val valueMethods = mainClass.getMethods.toSeq.filter { m =>
                m.getDeclaringClass != EnumClass &&
                m.getReturnType.getName == "scala.Enumeration$Value" && m.getParameterCount == 0
              }
              val enumValues = valueMethods.map(_.invoke(None.orNull))
              enumValues.foreach { v =>
                sp.addEnumItemObject(v.toString)
              }
            } catch {
              case NonFatal(t) => logger.warn(s"Failed to get values for enum ${cls.getName}", t)
            }
            Some(sp)
          }
          case _ => {
            Option(nullableClass).flatMap { cls =>
              if (cls == BigDecimalClass) {
                val dp = PrimitiveType.DECIMAL.createProperty()
                setRequired(`type`)
                Some(dp)
              } else if (cls == BigIntClass) {
                val ip = PrimitiveType.INT.createProperty()
                setRequired(`type`)
                Some(ip)
              } else {
                None
              }
            }
          }
        }
      }
    }
  }

  private def getMainClass(clazz: Class[_]): Class[_] = {
    val cname = clazz.getName
    if (cname.endsWith("$")) {
      Try(Class.forName(cname.substring(0, cname.length - 1))).getOrElse(clazz)
    } else {
      clazz
    }
  }

  private def _isOptional(annotatedType: AnnotatedType, cls: Class[_]): Boolean = {
    annotatedType.getType match {
      case _: ReferenceType if isOption(cls) => true
      case _ => false
    }
  }

  private def underlyingJavaType(annotatedType: AnnotatedType, javaType: JavaType): JavaType = {
    annotatedType.getType match {
      case rt: ReferenceType => rt.getContentType
      case _ => javaType
    }
  }

  private def nextType(
      baseType: AnnotatedType,
      `type`: AnnotatedType,
      javaType: JavaType): AnnotatedType = {
    baseType.`type`(underlyingJavaType(`type`, javaType))
      .ctxAnnotations(`type`.getCtxAnnotations)
      .parent(`type`.getParent)
      .schemaProperty(`type`.isSchemaProperty)
      .name(`type`.getName)
      .propertyName(`type`.getPropertyName)
      .resolveAsRef(`type`.isResolveAsRef)
      .jsonViewAnnotation(`type`.getJsonViewAnnotation)
      .skipOverride(`type`.isSkipOverride)
  }

  override def _isOptionalType(propType: JavaType): Boolean = {
    isOption(propType.getRawClass) || super._isOptionalType(propType)
  }

  override def _isSetType(cls: Class[_]): Boolean = {
    val setInterfaces = cls.getInterfaces.find { interface =>
      interface == SetClass
    }
    setInterfaces.isDefined || super._isSetType(cls)
  }

  private def setRequired(annotatedType: AnnotatedType): Unit = annotatedType match {
    case _: AnnotatedTypeForOption => // not required
    case _ => {
      val required = getRequiredSettings(annotatedType).headOption.getOrElse(true)
      if (required) {
        Option(annotatedType.getParent).foreach { parent =>
          Option(annotatedType.getPropertyName).foreach { n =>
            addRequiredItem(parent, n)
          }
        }
      }
    }
  }

  private def getPropertyClass(property: PropertyDescriptor): Class[_] = {
    property.param match {
      case Some(constructorParameter) => {
        val types = constructorParameter.constructor.getParameterTypes
        if (constructorParameter.index > types.size) {
          AnyClass
        } else {
          types(constructorParameter.index)
        }
      }
      case _ => property.field match {
          case Some(field) => field.getType
          case _ => property.setter match {
              case Some(setter) if setter.getParameterCount == 1 => {
                setter.getParameterTypes()(0)
              }
              case _ => property.beanSetter match {
                  case Some(setter) if setter.getParameterCount == 1 => {
                    setter.getParameterTypes()(0)
                  }
                  case _ => AnyClass
                }
            }
        }
    }
  }

  private def getPropertyAnnotations(property: PropertyDescriptor): Seq[Annotation] = {
    property.param match {
      case Some(constructorParameter) => {
        val types = constructorParameter.constructor.getParameterAnnotations
        if (constructorParameter.index > types.size) {
          Seq.empty
        } else {
          types(constructorParameter.index).toSeq
        }
      }
      case _ => property.field match {
          case Some(field) => field.getAnnotations.toSeq
          case _ => property.setter match {
              case Some(setter) if setter.getParameterCount == 1 => {
                setter.getAnnotations().toSeq
              }
              case _ => property.beanSetter match {
                  case Some(setter) if setter.getParameterCount == 1 => {
                    setter.getAnnotations().toSeq
                  }
                  case _ => Seq.empty
                }
            }
        }
    }
  }

  private def isOption(cls: Class[_]): Boolean = cls == OptionClass
  private def isIterable(cls: Class[_]): Boolean = IterableClass.isAssignableFrom(cls)
  private def isCaseClass(cls: Class[_]): Boolean = ProductClass.isAssignableFrom(cls)

  private def nullSafeList[T](array: Array[T]): List[T] = Option(array) match {
    case None => List.empty[T]
    case Some(arr) => arr.toList
  }
}
