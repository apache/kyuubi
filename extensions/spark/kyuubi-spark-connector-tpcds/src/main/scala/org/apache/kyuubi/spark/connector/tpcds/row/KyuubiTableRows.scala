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

package org.apache.kyuubi.spark.connector.tpcds.row

import io.trino.tpcds.`type`.{Address, Decimal => TPCDSDecimal, Pricing}
import io.trino.tpcds.generator.CallCenterGeneratorColumn._
import io.trino.tpcds.generator.CatalogPageGeneratorColumn._
import io.trino.tpcds.generator.CatalogReturnsGeneratorColumn._
import io.trino.tpcds.generator.CatalogSalesGeneratorColumn._
import io.trino.tpcds.generator.CustomerAddressGeneratorColumn._
import io.trino.tpcds.generator.CustomerDemographicsGeneratorColumn._
import io.trino.tpcds.generator.CustomerGeneratorColumn._
import io.trino.tpcds.generator.DateDimGeneratorColumn._
import io.trino.tpcds.generator.DbgenVersionGeneratorColumn._
import io.trino.tpcds.generator.GeneratorColumn
import io.trino.tpcds.generator.HouseholdDemographicsGeneratorColumn._
import io.trino.tpcds.generator.IncomeBandGeneratorColumn._
import io.trino.tpcds.generator.InventoryGeneratorColumn._
import io.trino.tpcds.generator.ItemGeneratorColumn._
import io.trino.tpcds.generator.PromotionGeneratorColumn._
import io.trino.tpcds.generator.ReasonGeneratorColumn._
import io.trino.tpcds.generator.ShipModeGeneratorColumn._
import io.trino.tpcds.generator.StoreGeneratorColumn._
import io.trino.tpcds.generator.StoreReturnsGeneratorColumn._
import io.trino.tpcds.generator.StoreSalesGeneratorColumn._
import io.trino.tpcds.generator.TimeDimGeneratorColumn._
import io.trino.tpcds.generator.WarehouseGeneratorColumn._
import io.trino.tpcds.generator.WebPageGeneratorColumn._
import io.trino.tpcds.generator.WebReturnsGeneratorColumn._
import io.trino.tpcds.generator.WebSalesGeneratorColumn._
import io.trino.tpcds.generator.WebSiteGeneratorColumn._
import io.trino.tpcds.row.{CallCenterRow, CatalogPageRow, CatalogReturnsRow, CatalogSalesRow, CustomerAddressRow, CustomerDemographicsRow, CustomerRow, DateDimRow, DbgenVersionRow, HouseholdDemographicsRow, IncomeBandRow, InventoryRow, ItemRow, PromotionRow, ReasonRow, ShipModeRow, StoreReturnsRow, StoreRow, StoreSalesRow, TableRow, TableRowWithNulls, TimeDimRow, WarehouseRow, WebPageRow, WebReturnsRow, WebSalesRow, WebSiteRow}

import org.apache.kyuubi.spark.connector.tpcds.row.KyuubiTPCDSTableRowWithNullsUtils._
import org.apache.kyuubi.util.reflect.{DynFields, DynMethods}

object KyuubiTableRows {

  implicit class StoreRowImplicits(storeRow: StoreRow) {
    def getStoreSk: Long = StoreRowImplicits.storeSk.get(storeRow)
    def getStoreId: String = StoreRowImplicits.storeId.get(storeRow)
    def getRecStartDateId: Long = StoreRowImplicits.recStartDateId.get(storeRow)
    def getRecEndDateId: Long = StoreRowImplicits.recEndDateId.get(storeRow)
    def getClosedDateId: Long = StoreRowImplicits.closedDateId.get(storeRow)
    def getStoreName: String = StoreRowImplicits.storeName.get(storeRow)
    def getEmployees: Int = StoreRowImplicits.employees.get(storeRow)
    def getFloorSpace: Int = StoreRowImplicits.floorSpace.get(storeRow)
    def getHours: String = StoreRowImplicits.hours.get(storeRow)
    def getStoreManager: String = StoreRowImplicits.storeManager.get(storeRow)
    def getMarketId: Int = StoreRowImplicits.marketId.get(storeRow)
    def getDTaxPercentage: TPCDSDecimal = StoreRowImplicits.dTaxPercentage.get(storeRow)
    def getGeographyClass: String = StoreRowImplicits.geographyClass.get(storeRow)
    def getMarketDesc: String = StoreRowImplicits.marketDesc.get(storeRow)
    def getMarketManager: String = StoreRowImplicits.marketManager.get(storeRow)
    def getDivisionId: Long = StoreRowImplicits.divisionId.get(storeRow)
    def getDivisionName: String = StoreRowImplicits.divisionName.get(storeRow)
    def getCompanyId: Long = StoreRowImplicits.companyId.get(storeRow)
    def getCompanyName: String = StoreRowImplicits.companyName.get(storeRow)
    def getAddress: Address = StoreRowImplicits.address.get(storeRow)
  }
  object StoreRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[StoreRow], field)
        .buildChecked[T]()

    lazy val storeSk = invoke[Long]("storeSk")
    lazy val storeId = invoke[String]("storeId")
    lazy val recStartDateId = invoke[Long]("recStartDateId")
    lazy val recEndDateId = invoke[Long]("recEndDateId")
    lazy val closedDateId = invoke[Long]("closedDateId")
    lazy val storeName = invoke[String]("storeName")
    lazy val employees = invoke[Int]("employees")
    lazy val floorSpace = invoke[Int]("floorSpace")
    lazy val hours = invoke[String]("hours")
    lazy val storeManager = invoke[String]("storeManager")
    lazy val marketId = invoke[Int]("marketId")
    lazy val dTaxPercentage = invoke[TPCDSDecimal]("dTaxPercentage")
    lazy val geographyClass = invoke[String]("geographyClass")
    lazy val marketDesc = invoke[String]("marketDesc")
    lazy val marketManager = invoke[String]("marketManager")
    lazy val divisionId = invoke[Long]("divisionId")
    lazy val divisionName = invoke[String]("divisionName")
    lazy val companyId = invoke[Long]("companyId")
    lazy val companyName = invoke[String]("companyName")
    lazy val address = invoke[Address]("address")

    def values(row: StoreRow): Array[Any] = Array(
      getOrNullForKey(row, row.getStoreSk, W_STORE_SK),
      getOrNull(row, row.getStoreId, W_STORE_ID),
      getDateOrNullFromJulianDays(row, row.getRecStartDateId, W_STORE_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getRecEndDateId, W_STORE_REC_END_DATE_ID),
      getOrNullForKey(row, row.getClosedDateId, W_STORE_CLOSED_DATE_ID),
      getOrNull(row, row.getStoreName, W_STORE_NAME),
      getOrNull(row, row.getEmployees, W_STORE_EMPLOYEES),
      getOrNull(row, row.getFloorSpace, W_STORE_FLOOR_SPACE),
      getOrNull(row, row.getHours, W_STORE_HOURS),
      getOrNull(row, row.getStoreManager, W_STORE_MANAGER),
      getOrNull(row, row.getMarketId, W_STORE_MARKET_ID),
      getOrNull(row, row.getGeographyClass, W_STORE_GEOGRAPHY_CLASS),
      getOrNull(row, row.getMarketDesc, W_STORE_MARKET_DESC),
      getOrNull(row, row.getMarketManager, W_STORE_MARKET_MANAGER),
      getOrNullForKey(row, row.getDivisionId, W_STORE_DIVISION_ID),
      getOrNull(row, row.getDivisionName, W_STORE_DIVISION_NAME),
      getOrNullForKey(row, row.getCompanyId, W_STORE_COMPANY_ID),
      getOrNull(row, row.getCompanyName, W_STORE_COMPANY_NAME),
      getOrNull(row, row.getAddress.getStreetNumber, W_STORE_ADDRESS_STREET_NUM),
      getOrNull(row, row.getAddress.getStreetName, W_STORE_ADDRESS_STREET_NAME1),
      getOrNull(row, row.getAddress.getStreetType, W_STORE_ADDRESS_STREET_TYPE),
      getOrNull(row, row.getAddress.getSuiteNumber, W_STORE_ADDRESS_SUITE_NUM),
      getOrNull(row, row.getAddress.getCity, W_STORE_ADDRESS_CITY),
      getOrNull(row, row.getAddress.getCounty, W_STORE_ADDRESS_COUNTY),
      getOrNull(row, row.getAddress.getState, W_STORE_ADDRESS_STATE),
      getOrNull(
        row,
        java.lang.String.format("%05d", row.getAddress.getZip.asInstanceOf[Object]),
        W_STORE_ADDRESS_ZIP),
      getOrNull(row, row.getAddress.getCountry, W_STORE_ADDRESS_COUNTRY),
      getOrNull(row, row.getAddress.getGmtOffset, W_STORE_ADDRESS_GMT_OFFSET),
      getOrNull(row, row.getDTaxPercentage, W_STORE_TAX_PERCENTAGE))
  }

  implicit class ReasonRowImplicits(reasonRow: ReasonRow) {
    def getRReasonSk: Long = ReasonRowImplicits.rReasonSk.get(reasonRow)
    def getRReasonId: String = ReasonRowImplicits.rReasonId.get(reasonRow)
    def getRReasonDescription: String = ReasonRowImplicits.rReasonDescription.get(reasonRow)
  }

  object ReasonRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[ReasonRow], field)
        .buildChecked[T]()
    lazy val rReasonSk = invoke[Long]("rReasonSk")
    lazy val rReasonId = invoke[String]("rReasonId")
    lazy val rReasonDescription = invoke[String]("rReasonDescription")

    def values(row: ReasonRow): Array[Any] = Array(
      getOrNullForKey(row, row.getRReasonSk, R_REASON_SK),
      getOrNull(row, row.getRReasonId, R_REASON_ID),
      getOrNull(row, row.getRReasonDescription, R_REASON_DESCRIPTION))
  }

  implicit class DbgenVersionRowImplicits(dbgenVersionRow: DbgenVersionRow) {
    def getDvVersion: String = DbgenVersionRowImplicits.dvVersion.get(dbgenVersionRow)
    def getDvCreateDate: String = DbgenVersionRowImplicits.dvCreateDate.get(dbgenVersionRow)
    def getDvCreateTime: String = DbgenVersionRowImplicits.dvCreateTime.get(dbgenVersionRow)
    def getDvCmdlineArgs: String = DbgenVersionRowImplicits.dvCmdlineArgs.get(dbgenVersionRow)
  }

  object DbgenVersionRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[DbgenVersionRow], field)
        .buildChecked[T]()

    lazy val dvVersion = invoke[String]("dvVersion")
    lazy val dvCreateDate = invoke[String]("dvCreateDate")
    lazy val dvCreateTime = invoke[String]("dvCreateTime")
    lazy val dvCmdlineArgs = invoke[String]("dvCmdlineArgs")

    def values(row: DbgenVersionRow): Array[Any] = Array(
      getOrNull(row, row.getDvVersion, DV_VERSION),
      getOrNull(row, row.getDvCreateDate, DV_CREATE_DATE),
      getOrNull(row, row.getDvCreateTime, DV_CREATE_TIME),
      getOrNull(row, row.getDvCmdlineArgs, DV_CMDLINE_ARGS))
  }

  implicit class ShipModeRowImplicits(shipModeRow: ShipModeRow) {
    def getSmShipModeSk: Long = ShipModeRowImplicits.smShipModeSk.get(shipModeRow)
    def getSmShipModeId: String = ShipModeRowImplicits.smShipModeId.get(shipModeRow)
    def getSmType: String = ShipModeRowImplicits.smType.get(shipModeRow)
    def getSmCode: String = ShipModeRowImplicits.smCode.get(shipModeRow)
    def getSmCarrier: String = ShipModeRowImplicits.smCarrier.get(shipModeRow)
    def getSmContract: String = ShipModeRowImplicits.smContract.get(shipModeRow)
  }

  object ShipModeRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[ShipModeRow], field)
        .buildChecked[T]()

    lazy val smShipModeSk = invoke[Long]("smShipModeSk")
    lazy val smShipModeId = invoke[String]("smShipModeId")
    lazy val smType = invoke[String]("smType")
    lazy val smCode = invoke[String]("smCode")
    lazy val smCarrier = invoke[String]("smCarrier")
    lazy val smContract = invoke[String]("smContract")

    def values(row: ShipModeRow): Array[Any] = Array(
      getOrNullForKey(row, row.getSmShipModeSk, SM_SHIP_MODE_SK),
      getOrNull(row, row.getSmShipModeId, SM_SHIP_MODE_ID),
      getOrNull(row, row.getSmType, SM_TYPE),
      getOrNull(row, row.getSmCode, SM_CODE),
      getOrNull(row, row.getSmCarrier, SM_CARRIER),
      getOrNull(row, row.getSmContract, SM_CONTRACT))
  }

  implicit class IncomeBandRowImplicits(incomeBandRow: IncomeBandRow) {
    def getIbIncomeBandId: Int = IncomeBandRowImplicits.ibIncomeBandId.get(incomeBandRow)
    def getIbLowerBound: Int = IncomeBandRowImplicits.ibLowerBound.get(incomeBandRow)
    def getIbUpperBound: Int = IncomeBandRowImplicits.ibUpperBound.get(incomeBandRow)
  }

  object IncomeBandRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[IncomeBandRow], field)
        .buildChecked[T]()

    lazy val ibIncomeBandId = invoke[Int]("ibIncomeBandId")
    lazy val ibLowerBound = invoke[Int]("ibLowerBound")
    lazy val ibUpperBound = invoke[Int]("ibUpperBound")

    def values(row: IncomeBandRow): Array[Any] = Array(
      getOrNull(row, row.getIbIncomeBandId, IB_INCOME_BAND_ID),
      getOrNull(row, row.getIbLowerBound, IB_LOWER_BOUND),
      getOrNull(row, row.getIbUpperBound, IB_UPPER_BOUND))
  }

  implicit class ItemRowImplicits(itemRow: ItemRow) {
    def getIItemSk: Long = ItemRowImplicits.iItemSk.get(itemRow)
    def getIItemId: String = ItemRowImplicits.iItemId.get(itemRow)
    def getIRecStartDateId: Long = ItemRowImplicits.iRecStartDateId.get(itemRow)
    def getIRecEndDateId: Long = ItemRowImplicits.iRecEndDateId.get(itemRow)
    def getIItemDesc: String = ItemRowImplicits.iItemDesc.get(itemRow)
    def getICurrentPrice: TPCDSDecimal = ItemRowImplicits.iCurrentPrice.get(itemRow)
    def getIWholesaleCost: TPCDSDecimal = ItemRowImplicits.iWholesaleCost.get(itemRow)
    def getIBrandId: Long = ItemRowImplicits.iBrandId.get(itemRow)
    def getIBrand: String = ItemRowImplicits.iBrand.get(itemRow)
    def getIClassId: Long = ItemRowImplicits.iClassId.get(itemRow)
    def getIClass: String = ItemRowImplicits.iClass.get(itemRow)
    def getICategoryId: Long = ItemRowImplicits.iCategoryId.get(itemRow)
    def getICategory: String = ItemRowImplicits.iCategory.get(itemRow)
    def getIManufactId: Long = ItemRowImplicits.iManufactId.get(itemRow)
    def getIManufact: String = ItemRowImplicits.iManufact.get(itemRow)
    def getISize: String = ItemRowImplicits.iSize.get(itemRow)
    def getIFormulation: String = ItemRowImplicits.iFormulation.get(itemRow)
    def getIColor: String = ItemRowImplicits.iColor.get(itemRow)
    def getIUnits: String = ItemRowImplicits.iUnits.get(itemRow)
    def getIContainer: String = ItemRowImplicits.iContainer.get(itemRow)
    def getIManagerId: Long = ItemRowImplicits.iManagerId.get(itemRow)
    def getIProductName: String = ItemRowImplicits.iProductName.get(itemRow)
    def getIPromoSk: Long = ItemRowImplicits.iPromoSk.get(itemRow)
  }

  object ItemRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[ItemRow], field)
        .buildChecked[T]()

    lazy val iItemSk = invoke[Long]("iItemSk")
    lazy val iItemId = invoke[String]("iItemId")
    lazy val iRecStartDateId = invoke[Long]("iRecStartDateId")
    lazy val iRecEndDateId = invoke[Long]("iRecEndDateId")
    lazy val iItemDesc = invoke[String]("iItemDesc")
    lazy val iCurrentPrice = invoke[TPCDSDecimal]("iCurrentPrice")
    lazy val iWholesaleCost = invoke[TPCDSDecimal]("iWholesaleCost")
    lazy val iBrandId = invoke[Long]("iBrandId")
    lazy val iBrand = invoke[String]("iBrand")
    lazy val iClassId = invoke[Long]("iClassId")
    lazy val iClass = invoke[String]("iClass")
    lazy val iCategoryId = invoke[Long]("iCategoryId")
    lazy val iCategory = invoke[String]("iCategory")
    lazy val iManufactId = invoke[Long]("iManufactId")
    lazy val iManufact = invoke[String]("iManufact")
    lazy val iSize = invoke[String]("iSize")
    lazy val iFormulation = invoke[String]("iFormulation")
    lazy val iColor = invoke[String]("iColor")
    lazy val iUnits = invoke[String]("iUnits")
    lazy val iContainer = invoke[String]("iContainer")
    lazy val iManagerId = invoke[Long]("iManagerId")
    lazy val iProductName = invoke[String]("iProductName")
    lazy val iPromoSk = invoke[Long]("iPromoSk")

    def values(row: ItemRow): Array[Any] = Array(
      getOrNullForKey(row, row.getIItemSk, I_ITEM_SK),
      getOrNull(row, row.getIItemId, I_ITEM_ID),
      getDateOrNullFromJulianDays(row, row.getIRecStartDateId, I_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getIRecEndDateId, I_REC_END_DATE_ID),
      getOrNull(row, row.getIItemDesc, I_ITEM_DESC),
      getOrNull(row, row.getICurrentPrice, I_CURRENT_PRICE),
      getOrNull(row, row.getIWholesaleCost, I_WHOLESALE_COST),
      getOrNullForKey(row, row.getIBrandId, I_BRAND_ID),
      getOrNull(row, row.getIBrand, I_BRAND),
      getOrNullForKey(row, row.getIClassId, I_CLASS_ID),
      getOrNull(row, row.getIClass, I_CLASS),
      getOrNullForKey(row, row.getICategoryId, I_CATEGORY_ID),
      getOrNull(row, row.getICategory, I_CATEGORY),
      getOrNullForKey(row, row.getIManufactId, I_MANUFACT_ID),
      getOrNull(row, row.getIManufact, I_MANUFACT),
      getOrNull(row, row.getISize, I_SIZE),
      getOrNull(row, row.getIFormulation, I_FORMULATION),
      getOrNull(row, row.getIColor, I_COLOR),
      getOrNull(row, row.getIUnits, I_UNITS),
      getOrNull(row, row.getIContainer, I_CONTAINER),
      getOrNullForKey(row, row.getIManagerId, I_MANAGER_ID),
      getOrNull(row, row.getIProductName, I_PRODUCT_NAME))
  }

  implicit class CustomerDemographicsRowImplicits(
      customerDemographicsRow: CustomerDemographicsRow) {
    def getCdDemoSk: Long = CustomerDemographicsRowImplicits.cdDemoSk.get(customerDemographicsRow)
    def getCdGender: String = CustomerDemographicsRowImplicits.cdGender.get(customerDemographicsRow)
    def getCdMaritalStatus: String =
      CustomerDemographicsRowImplicits.cdMaritalStatus.get(customerDemographicsRow)
    def getCdEducationStatus: String =
      CustomerDemographicsRowImplicits.cdEducationStatus.get(customerDemographicsRow)
    def getCdPurchaseEstimate: Int =
      CustomerDemographicsRowImplicits.cdPurchaseEstimate.get(customerDemographicsRow)
    def getCdCreditRating: String =
      CustomerDemographicsRowImplicits.cdCreditRating.get(customerDemographicsRow)
    def getCdDepCount: Int =
      CustomerDemographicsRowImplicits.cdDepCount.get(customerDemographicsRow)
    def getCdDepEmployedCount: Int =
      CustomerDemographicsRowImplicits.cdDepEmployedCount.get(customerDemographicsRow)
    def getCdDepCollegeCount: Int =
      CustomerDemographicsRowImplicits.cdDepCollegeCount.get(customerDemographicsRow)
  }

  object CustomerDemographicsRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CustomerDemographicsRow], field)
        .buildChecked[T]()

    lazy val cdDemoSk = invoke[Long]("cdDemoSk")
    lazy val cdGender = invoke[String]("cdGender")
    lazy val cdMaritalStatus = invoke[String]("cdMaritalStatus")
    lazy val cdEducationStatus = invoke[String]("cdEducationStatus")
    lazy val cdPurchaseEstimate = invoke[Int]("cdPurchaseEstimate")
    lazy val cdCreditRating = invoke[String]("cdCreditRating")
    lazy val cdDepCount = invoke[Int]("cdDepCount")
    lazy val cdDepEmployedCount = invoke[Int]("cdDepEmployedCount")
    lazy val cdDepCollegeCount = invoke[Int]("cdDepCollegeCount")

    def values(row: CustomerDemographicsRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCdDemoSk, CD_DEMO_SK),
      getOrNull(row, row.getCdGender, CD_GENDER),
      getOrNull(row, row.getCdMaritalStatus, CD_MARITAL_STATUS),
      getOrNull(row, row.getCdEducationStatus, CD_EDUCATION_STATUS),
      getOrNull(row, row.getCdPurchaseEstimate, CD_PURCHASE_ESTIMATE),
      getOrNull(row, row.getCdCreditRating, CD_CREDIT_RATING),
      getOrNull(row, row.getCdDepCount, CD_DEP_COUNT),
      getOrNull(row, row.getCdDepEmployedCount, CD_DEP_EMPLOYED_COUNT),
      getOrNull(row, row.getCdDepCollegeCount, CD_DEP_COLLEGE_COUNT))
  }

  implicit class TimeDimRowImplicits(timeDimRow: TimeDimRow) {
    def getTTimeSk: Long = TimeDimRowImplicits.tTimeSk.get(timeDimRow)
    def getTTimeId: String = TimeDimRowImplicits.tTimeId.get(timeDimRow)
    def getTTime: Int = TimeDimRowImplicits.tTime.get(timeDimRow)
    def getTHour: Int = TimeDimRowImplicits.tHour.get(timeDimRow)
    def getTMinute: Int = TimeDimRowImplicits.tMinute.get(timeDimRow)
    def getTSecond: Int = TimeDimRowImplicits.tSecond.get(timeDimRow)
    def getTAmPm: String = TimeDimRowImplicits.tAmPm.get(timeDimRow)
    def getTShift: String = TimeDimRowImplicits.tShift.get(timeDimRow)
    def getTSubShift: String = TimeDimRowImplicits.tSubShift.get(timeDimRow)
    def getTMealTime: String = TimeDimRowImplicits.tMealTime.get(timeDimRow)
  }

  object TimeDimRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[TimeDimRow], field)
        .buildChecked[T]()

    lazy val tTimeSk = invoke[Long]("tTimeSk")
    lazy val tTimeId = invoke[String]("tTimeId")
    lazy val tTime = invoke[Int]("tTime")
    lazy val tHour = invoke[Int]("tHour")
    lazy val tMinute = invoke[Int]("tMinute")
    lazy val tSecond = invoke[Int]("tSecond")
    lazy val tAmPm = invoke[String]("tAmPm")
    lazy val tShift = invoke[String]("tShift")
    lazy val tSubShift = invoke[String]("tSubShift")
    lazy val tMealTime = invoke[String]("tMealTime")

    def values(row: TimeDimRow): Array[Any] = Array(
      getOrNullForKey(row, row.getTTimeSk, T_TIME_SK),
      getOrNull(row, row.getTTimeId, T_TIME_ID),
      getOrNull(row, row.getTTime, T_TIME),
      getOrNull(row, row.getTHour, T_HOUR),
      getOrNull(row, row.getTMinute, T_MINUTE),
      getOrNull(row, row.getTSecond, T_SECOND),
      getOrNull(row, row.getTAmPm, T_AM_PM),
      getOrNull(row, row.getTShift, T_SHIFT),
      getOrNull(row, row.getTSubShift, T_SUB_SHIFT),
      getOrNull(row, row.getTMealTime, T_MEAL_TIME))
  }

  implicit class WebSiteRowImplicits(webSiteRow: WebSiteRow) {
    def getWebSiteSk: Long = WebSiteRowImplicits.webSiteSk.get(webSiteRow)
    def getWebSiteId: String = WebSiteRowImplicits.webSiteId.get(webSiteRow)
    def getWebRecStartDateId: Long = WebSiteRowImplicits.webRecStartDateId.get(webSiteRow)
    def getWebRecEndDateId: Long = WebSiteRowImplicits.webRecEndDateId.get(webSiteRow)
    def getWebName: String = WebSiteRowImplicits.webName.get(webSiteRow)
    def getWebOpenDate: Long = WebSiteRowImplicits.webOpenDate.get(webSiteRow)
    def getWebCloseDate: Long = WebSiteRowImplicits.webCloseDate.get(webSiteRow)
    def getWebClass: String = WebSiteRowImplicits.webClass.get(webSiteRow)
    def getWebManager: String = WebSiteRowImplicits.webManager.get(webSiteRow)
    def getWebMarketId: Int = WebSiteRowImplicits.webMarketId.get(webSiteRow)
    def getWebMarketClass: String = WebSiteRowImplicits.webMarketClass.get(webSiteRow)
    def getWebMarketDesc: String = WebSiteRowImplicits.webMarketDesc.get(webSiteRow)
    def getWebMarketManager: String = WebSiteRowImplicits.webMarketManager.get(webSiteRow)
    def getWebCompanyId: Int = WebSiteRowImplicits.webCompanyId.get(webSiteRow)
    def getWebCompanyName: String = WebSiteRowImplicits.webCompanyName.get(webSiteRow)
    def getWebAddress: Address = WebSiteRowImplicits.webAddress.get(webSiteRow)
    def getWebTaxPercentage: TPCDSDecimal = WebSiteRowImplicits.webTaxPercentage.get(webSiteRow)
  }

  object WebSiteRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[WebSiteRow], field)
        .buildChecked[T]()

    lazy val webSiteSk = invoke[Long]("webSiteSk")
    lazy val webSiteId = invoke[String]("webSiteId")
    lazy val webRecStartDateId = invoke[Long]("webRecStartDateId")
    lazy val webRecEndDateId = invoke[Long]("webRecEndDateId")
    lazy val webName = invoke[String]("webName")
    lazy val webOpenDate = invoke[Long]("webOpenDate")
    lazy val webCloseDate = invoke[Long]("webCloseDate")
    lazy val webClass = invoke[String]("webClass")
    lazy val webManager = invoke[String]("webManager")
    lazy val webMarketId = invoke[Int]("webMarketId")
    lazy val webMarketClass = invoke[String]("webMarketClass")
    lazy val webMarketDesc = invoke[String]("webMarketDesc")
    lazy val webMarketManager = invoke[String]("webMarketManager")
    lazy val webCompanyId = invoke[Int]("webCompanyId")
    lazy val webCompanyName = invoke[String]("webCompanyName")
    lazy val webAddress = invoke[Address]("webAddress")
    lazy val webTaxPercentage = invoke[TPCDSDecimal]("webTaxPercentage")

    def values(row: WebSiteRow): Array[Any] = Array(
      getOrNullForKey(row, row.getWebSiteSk, WEB_SITE_SK),
      getOrNull(row, row.getWebSiteId, WEB_SITE_ID),
      getDateOrNullFromJulianDays(row, row.getWebRecStartDateId, WEB_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getWebRecEndDateId, WEB_REC_END_DATE_ID),
      getOrNull(row, row.getWebName, WEB_NAME),
      getOrNullForKey(row, row.getWebOpenDate, WEB_OPEN_DATE),
      getOrNullForKey(row, row.getWebCloseDate, WEB_CLOSE_DATE),
      getOrNull(row, row.getWebClass, WEB_CLASS),
      getOrNull(row, row.getWebManager, WEB_MANAGER),
      getOrNull(row, row.getWebMarketId, WEB_MARKET_ID),
      getOrNull(row, row.getWebMarketClass, WEB_MARKET_CLASS),
      getOrNull(row, row.getWebMarketDesc, WEB_MARKET_DESC),
      getOrNull(row, row.getWebMarketManager, WEB_MARKET_MANAGER),
      getOrNull(row, row.getWebCompanyId, WEB_COMPANY_ID),
      getOrNull(row, row.getWebCompanyName, WEB_COMPANY_NAME),
      getOrNull(row, row.getWebAddress.getStreetNumber(), WEB_ADDRESS_STREET_NUM),
      getOrNull(row, row.getWebAddress.getStreetName(), WEB_ADDRESS_STREET_NAME1),
      getOrNull(row, row.getWebAddress.getStreetType(), WEB_ADDRESS_STREET_TYPE),
      getOrNull(row, row.getWebAddress.getSuiteNumber(), WEB_ADDRESS_SUITE_NUM),
      getOrNull(row, row.getWebAddress.getCity(), WEB_ADDRESS_CITY),
      getOrNull(row, row.getWebAddress.getCounty(), WEB_ADDRESS_COUNTY),
      getOrNull(row, row.getWebAddress.getState(), WEB_ADDRESS_STATE),
      getOrNull(
        row,
        java.lang.String.format("%05d", row.getWebAddress.getZip().asInstanceOf[Object]),
        WEB_ADDRESS_ZIP),
      getOrNull(row, row.getWebAddress.getCountry(), WEB_ADDRESS_COUNTRY),
      getOrNull(row, row.getWebAddress.getGmtOffset(), WEB_ADDRESS_GMT_OFFSET),
      getOrNull(row, row.getWebTaxPercentage, WEB_TAX_PERCENTAGE))
  }

  implicit class HouseholdDemographicsRowImplicits(
      householdDemographicsRow: HouseholdDemographicsRow) {
    def getHdDemoSk: Long = HouseholdDemographicsRowImplicits.hdDemoSk.get(householdDemographicsRow)
    def getHdIncomeBandId: Long =
      HouseholdDemographicsRowImplicits.hdIncomeBandId.get(householdDemographicsRow)
    def getHdBuyPotential: String =
      HouseholdDemographicsRowImplicits.hdBuyPotential.get(householdDemographicsRow)
    def getHdDepCount: Int =
      HouseholdDemographicsRowImplicits.hdDepCount.get(householdDemographicsRow)
    def getHdVehicleCount: Int =
      HouseholdDemographicsRowImplicits.hdVehicleCount.get(householdDemographicsRow)
  }

  object HouseholdDemographicsRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[HouseholdDemographicsRow], field)
        .buildChecked[T]()

    lazy val hdDemoSk = invoke[Long]("hdDemoSk")
    lazy val hdIncomeBandId = invoke[Long]("hdIncomeBandId")
    lazy val hdBuyPotential = invoke[String]("hdBuyPotential")
    lazy val hdDepCount = invoke[Int]("hdDepCount")
    lazy val hdVehicleCount = invoke[Int]("hdVehicleCount")

    def values(row: HouseholdDemographicsRow): Array[Any] = Array(
      getOrNullForKey(row, row.getHdDemoSk, HD_DEMO_SK),
      getOrNullForKey(row, row.getHdIncomeBandId, HD_INCOME_BAND_ID),
      getOrNull(row, row.getHdBuyPotential, HD_BUY_POTENTIAL),
      getOrNull(row, row.getHdDepCount, HD_DEP_COUNT),
      getOrNull(row, row.getHdVehicleCount, HD_VEHICLE_COUNT))
  }

  implicit class PromotionRowImplicits(promotionRow: PromotionRow) {
    def getPPromoSk: Long = PromotionRowImplicits.pPromoSk.get(promotionRow)
    def getPPromoId: String = PromotionRowImplicits.pPromoId.get(promotionRow)
    def getPStartDateId: Long = PromotionRowImplicits.pStartDateId.get(promotionRow)
    def getPEndDateId: Long = PromotionRowImplicits.pEndDateId.get(promotionRow)
    def getPItemSk: Long = PromotionRowImplicits.pItemSk.get(promotionRow)
    def getPCost: TPCDSDecimal = PromotionRowImplicits.pCost.get(promotionRow)
    def getPResponseTarget: Int = PromotionRowImplicits.pResponseTarget.get(promotionRow)
    def getPPromoName: String = PromotionRowImplicits.pPromoName.get(promotionRow)
    def isPChannelDmail: Boolean = PromotionRowImplicits.pChannelDmail.get(promotionRow)
    def isPChannelEmail: Boolean = PromotionRowImplicits.pChannelEmail.get(promotionRow)
    def isPChannelCatalog: Boolean = PromotionRowImplicits.pChannelCatalog.get(promotionRow)
    def isPChannelTv: Boolean = PromotionRowImplicits.pChannelTv.get(promotionRow)
    def isPChannelRadio: Boolean = PromotionRowImplicits.pChannelRadio.get(promotionRow)
    def isPChannelPress: Boolean = PromotionRowImplicits.pChannelPress.get(promotionRow)
    def isPChannelEvent: Boolean = PromotionRowImplicits.pChannelEvent.get(promotionRow)
    def isPChannelDemo: Boolean = PromotionRowImplicits.pChannelDemo.get(promotionRow)
    def getPChannelDetails: String = PromotionRowImplicits.pChannelDetails.get(promotionRow)
    def getPPurpose: String = PromotionRowImplicits.pPurpose.get(promotionRow)
    def isPDiscountActive: Boolean = PromotionRowImplicits.pDiscountActive.get(promotionRow)
  }

  object PromotionRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[PromotionRow], field)
        .buildChecked[T]()

    lazy val pPromoSk = invoke[Long]("pPromoSk")
    lazy val pPromoId = invoke[String]("pPromoId")
    lazy val pStartDateId = invoke[Long]("pStartDateId")
    lazy val pEndDateId = invoke[Long]("pEndDateId")
    lazy val pItemSk = invoke[Long]("pItemSk")
    lazy val pCost = invoke[TPCDSDecimal]("pCost")
    lazy val pResponseTarget = invoke[Int]("pResponseTarget")
    lazy val pPromoName = invoke[String]("pPromoName")
    lazy val pChannelDmail = invoke[Boolean]("pChannelDmail")
    lazy val pChannelEmail = invoke[Boolean]("pChannelEmail")
    lazy val pChannelCatalog = invoke[Boolean]("pChannelCatalog")
    lazy val pChannelTv = invoke[Boolean]("pChannelTv")
    lazy val pChannelRadio = invoke[Boolean]("pChannelRadio")
    lazy val pChannelPress = invoke[Boolean]("pChannelPress")
    lazy val pChannelEvent = invoke[Boolean]("pChannelEvent")
    lazy val pChannelDemo = invoke[Boolean]("pChannelDemo")
    lazy val pChannelDetails = invoke[String]("pChannelDetails")
    lazy val pPurpose = invoke[String]("pPurpose")
    lazy val pDiscountActive = invoke[Boolean]("pDiscountActive")

    def values(row: PromotionRow): Array[Any] = Array(
      getOrNullForKey(row, row.getPPromoSk, P_PROMO_SK),
      getOrNull(row, row.getPPromoId, P_PROMO_ID),
      getOrNullForKey(row, row.getPStartDateId, P_START_DATE_ID),
      getOrNullForKey(row, row.getPEndDateId, P_END_DATE_ID),
      getOrNullForKey(row, row.getPItemSk, P_ITEM_SK),
      getOrNull(row, row.getPCost, P_COST),
      getOrNull(row, row.getPResponseTarget, P_RESPONSE_TARGET),
      getOrNull(row, row.getPPromoName, P_PROMO_NAME),
      getOrNullForBoolean(row, row.isPChannelDmail, P_CHANNEL_DMAIL),
      getOrNullForBoolean(row, row.isPChannelEmail, P_CHANNEL_EMAIL),
      getOrNullForBoolean(row, row.isPChannelCatalog, P_CHANNEL_CATALOG),
      getOrNullForBoolean(row, row.isPChannelTv, P_CHANNEL_TV),
      getOrNullForBoolean(row, row.isPChannelRadio, P_CHANNEL_RADIO),
      getOrNullForBoolean(row, row.isPChannelPress, P_CHANNEL_PRESS),
      getOrNullForBoolean(row, row.isPChannelEvent, P_CHANNEL_EVENT),
      getOrNullForBoolean(row, row.isPChannelDemo, P_CHANNEL_DEMO),
      getOrNull(row, row.getPChannelDetails, P_CHANNEL_DETAILS),
      getOrNull(row, row.getPPurpose, P_PURPOSE),
      getOrNullForBoolean(row, row.isPDiscountActive, P_DISCOUNT_ACTIVE))
  }

  implicit class CatalogPageRowImplicits(catalogPageRow: CatalogPageRow) {
    def getCpCatalogPageSk: Long = CatalogPageRowImplicits.cpCatalogPageSk.get(catalogPageRow)
    def getCpCatalogPageId: String = CatalogPageRowImplicits.cpCatalogPageId.get(catalogPageRow)
    def getCpStartDateId: Long = CatalogPageRowImplicits.cpStartDateId.get(catalogPageRow)
    def getCpEndDateId: Long = CatalogPageRowImplicits.cpEndDateId.get(catalogPageRow)
    def getCpDepartment: String = CatalogPageRowImplicits.cpDepartment.get(catalogPageRow)
    def getCpCatalogNumber: Int = CatalogPageRowImplicits.cpCatalogNumber.get(catalogPageRow)
    def getCpCatalogPageNumber: Int =
      CatalogPageRowImplicits.cpCatalogPageNumber.get(catalogPageRow)
    def getCpDescription: String = CatalogPageRowImplicits.cpDescription.get(catalogPageRow)
    def getCpType: String = CatalogPageRowImplicits.cpType.get(catalogPageRow)
  }

  object CatalogPageRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CatalogPageRow], field)
        .buildChecked[T]()

    lazy val cpCatalogPageSk = invoke[Long]("cpCatalogPageSk")
    lazy val cpCatalogPageId = invoke[String]("cpCatalogPageId")
    lazy val cpStartDateId = invoke[Long]("cpStartDateId")
    lazy val cpEndDateId = invoke[Long]("cpEndDateId")
    lazy val cpDepartment = invoke[String]("cpDepartment")
    lazy val cpCatalogNumber = invoke[Int]("cpCatalogNumber")
    lazy val cpCatalogPageNumber = invoke[Int]("cpCatalogPageNumber")
    lazy val cpDescription = invoke[String]("cpDescription")
    lazy val cpType = invoke[String]("cpType")

    def values(row: CatalogPageRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCpCatalogPageSk, CP_CATALOG_PAGE_SK),
      getOrNull(row, row.getCpCatalogPageId, CP_CATALOG_PAGE_ID),
      getOrNullForKey(row, row.getCpStartDateId, CP_START_DATE_ID),
      getOrNullForKey(row, row.getCpEndDateId, CP_END_DATE_ID),
      getOrNull(row, row.getCpDepartment, CP_DEPARTMENT),
      getOrNull(row, row.getCpCatalogNumber, CP_CATALOG_NUMBER),
      getOrNull(row, row.getCpCatalogPageNumber, CP_CATALOG_PAGE_NUMBER),
      getOrNull(row, row.getCpDescription, CP_DESCRIPTION),
      getOrNull(row, row.getCpType, CP_TYPE))
  }

  implicit class WebSalesRowImplicits(webSalesRow: WebSalesRow) {
    def getWsSoldDateSk: Long = WebSalesRowImplicits.wsSoldDateSk.get(webSalesRow)
    def getWsSoldTimeSk: Long = WebSalesRowImplicits.wsSoldTimeSk.get(webSalesRow)
    def getWsShipDateSk: Long = WebSalesRowImplicits.wsShipDateSk.get(webSalesRow)
    def getWsItemSk: Long = WebSalesRowImplicits.wsItemSk.get(webSalesRow)
    def getWsBillCustomerSk: Long = WebSalesRowImplicits.wsBillCustomerSk.get(webSalesRow)
    def getWsBillCdemoSk: Long = WebSalesRowImplicits.wsBillCdemoSk.get(webSalesRow)
    def getWsBillHdemoSk: Long = WebSalesRowImplicits.wsBillHdemoSk.get(webSalesRow)
    def getWsBillAddrSk: Long = WebSalesRowImplicits.wsBillAddrSk.get(webSalesRow)
    def getWsShipCustomerSk: Long = WebSalesRowImplicits.wsShipCustomerSk.get(webSalesRow)
    def getWsShipCdemoSk: Long = WebSalesRowImplicits.wsShipCdemoSk.get(webSalesRow)
    def getWsShipHdemoSk: Long = WebSalesRowImplicits.wsShipHdemoSk.get(webSalesRow)
    def getWsShipAddrSk: Long = WebSalesRowImplicits.wsShipAddrSk.get(webSalesRow)
    def getWsWebPageSk: Long = WebSalesRowImplicits.wsWebPageSk.get(webSalesRow)
    def getWsWebSiteSk: Long = WebSalesRowImplicits.wsWebSiteSk.get(webSalesRow)
    def getWsShipModeSk: Long = WebSalesRowImplicits.wsShipModeSk.get(webSalesRow)
    def getWsWarehouseSk: Long = WebSalesRowImplicits.wsWarehouseSk.get(webSalesRow)
    def getWsPromoSk: Long = WebSalesRowImplicits.wsPromoSk.get(webSalesRow)
    def getWsOrderNumber: Long = WebSalesRowImplicits.wsOrderNumber.get(webSalesRow)
    def getWsPricing: Pricing = WebSalesRowImplicits.wsPricing.get(webSalesRow)
  }

  object WebSalesRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[WebSalesRow], field)
        .buildChecked[T]()

    lazy val wsSoldDateSk = invoke[Long]("wsSoldDateSk")
    lazy val wsSoldTimeSk = invoke[Long]("wsSoldTimeSk")
    lazy val wsShipDateSk = invoke[Long]("wsShipDateSk")
    lazy val wsItemSk = invoke[Long]("wsItemSk")
    lazy val wsBillCustomerSk = invoke[Long]("wsBillCustomerSk")
    lazy val wsBillCdemoSk = invoke[Long]("wsBillCdemoSk")
    lazy val wsBillHdemoSk = invoke[Long]("wsBillHdemoSk")
    lazy val wsBillAddrSk = invoke[Long]("wsBillAddrSk")
    lazy val wsShipCustomerSk = invoke[Long]("wsShipCustomerSk")
    lazy val wsShipCdemoSk = invoke[Long]("wsShipCdemoSk")
    lazy val wsShipHdemoSk = invoke[Long]("wsShipHdemoSk")
    lazy val wsShipAddrSk = invoke[Long]("wsShipAddrSk")
    lazy val wsWebPageSk = invoke[Long]("wsWebPageSk")
    lazy val wsWebSiteSk = invoke[Long]("wsWebSiteSk")
    lazy val wsShipModeSk = invoke[Long]("wsShipModeSk")
    lazy val wsWarehouseSk = invoke[Long]("wsWarehouseSk")
    lazy val wsPromoSk = invoke[Long]("wsPromoSk")
    lazy val wsOrderNumber = invoke[Long]("wsOrderNumber")
    lazy val wsPricing = invoke[Pricing]("wsPricing")

    def values(row: WebSalesRow): Array[Any] = Array(
      getOrNullForKey(row, row.getWsSoldDateSk, WS_SOLD_DATE_SK),
      getOrNullForKey(row, row.getWsSoldTimeSk, WS_SOLD_TIME_SK),
      getOrNullForKey(row, row.getWsShipDateSk, WS_SHIP_DATE_SK),
      getOrNullForKey(row, row.getWsItemSk, WS_ITEM_SK),
      getOrNullForKey(row, row.getWsBillCustomerSk, WS_BILL_CUSTOMER_SK),
      getOrNullForKey(row, row.getWsBillCdemoSk, WS_BILL_CDEMO_SK),
      getOrNullForKey(row, row.getWsBillHdemoSk, WS_BILL_HDEMO_SK),
      getOrNullForKey(row, row.getWsBillAddrSk, WS_BILL_ADDR_SK),
      getOrNullForKey(row, row.getWsShipCustomerSk, WS_SHIP_CUSTOMER_SK),
      getOrNullForKey(row, row.getWsShipCdemoSk, WS_SHIP_CDEMO_SK),
      getOrNullForKey(row, row.getWsShipHdemoSk, WS_SHIP_HDEMO_SK),
      getOrNullForKey(row, row.getWsShipAddrSk, WS_SHIP_ADDR_SK),
      getOrNullForKey(row, row.getWsWebPageSk, WS_WEB_PAGE_SK),
      getOrNullForKey(row, row.getWsWebSiteSk, WS_WEB_SITE_SK),
      getOrNullForKey(row, row.getWsShipModeSk, WS_SHIP_MODE_SK),
      getOrNullForKey(row, row.getWsWarehouseSk, WS_WAREHOUSE_SK),
      getOrNullForKey(row, row.getWsPromoSk, WS_PROMO_SK),
      getOrNullForKey(row, row.getWsOrderNumber, WS_ORDER_NUMBER),
      getOrNull(row, row.getWsPricing.getQuantity(), WS_PRICING_QUANTITY),
      getOrNull(row, row.getWsPricing.getWholesaleCost(), WS_PRICING_WHOLESALE_COST),
      getOrNull(row, row.getWsPricing.getListPrice(), WS_PRICING_LIST_PRICE),
      getOrNull(row, row.getWsPricing.getSalesPrice(), WS_PRICING_SALES_PRICE),
      getOrNull(row, row.getWsPricing.getExtDiscountAmount(), WS_PRICING_EXT_DISCOUNT_AMT),
      getOrNull(row, row.getWsPricing.getExtSalesPrice(), WS_PRICING_EXT_SALES_PRICE),
      getOrNull(row, row.getWsPricing.getExtWholesaleCost(), WS_PRICING_EXT_WHOLESALE_COST),
      getOrNull(row, row.getWsPricing.getExtListPrice(), WS_PRICING_EXT_LIST_PRICE),
      getOrNull(row, row.getWsPricing.getExtTax(), WS_PRICING_EXT_TAX),
      getOrNull(row, row.getWsPricing.getCouponAmount(), WS_PRICING_COUPON_AMT),
      getOrNull(row, row.getWsPricing.getExtShipCost(), WS_PRICING_EXT_SHIP_COST),
      getOrNull(row, row.getWsPricing.getNetPaid(), WS_PRICING_NET_PAID),
      getOrNull(row, row.getWsPricing.getNetPaidIncludingTax(), WS_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getWsPricing.getNetPaidIncludingShipping(), WS_PRICING_NET_PAID_INC_SHIP),
      getOrNull(
        row,
        row.getWsPricing.getNetPaidIncludingShippingAndTax(),
        WS_PRICING_NET_PAID_INC_SHIP_TAX),
      getOrNull(row, row.getWsPricing.getNetProfit(), WS_PRICING_NET_PROFIT))
  }

  implicit class StoreSalesRowImplicits(storeSalesRow: StoreSalesRow) {
    def getSsSoldDateSk: Long = StoreSalesRowImplicits.ssSoldDateSk.get(storeSalesRow)
    def getSsSoldTimeSk: Long = StoreSalesRowImplicits.ssSoldTimeSk.get(storeSalesRow)
    def getSsSoldItemSk: Long = StoreSalesRowImplicits.ssSoldItemSk.get(storeSalesRow)
    def getSsSoldCustomerSk: Long = StoreSalesRowImplicits.ssSoldCustomerSk.get(storeSalesRow)
    def getSsSoldCdemoSk: Long = StoreSalesRowImplicits.ssSoldCdemoSk.get(storeSalesRow)
    def getSsSoldHdemoSk: Long = StoreSalesRowImplicits.ssSoldHdemoSk.get(storeSalesRow)
    def getSsSoldAddrSk: Long = StoreSalesRowImplicits.ssSoldAddrSk.get(storeSalesRow)
    def getSsSoldStoreSk: Long = StoreSalesRowImplicits.ssSoldStoreSk.get(storeSalesRow)
    def getSsSoldPromoSk: Long = StoreSalesRowImplicits.ssSoldPromoSk.get(storeSalesRow)
    def getSsTicketNumber: Long = StoreSalesRowImplicits.ssTicketNumber.get(storeSalesRow)
    def getSsPricing: Pricing = StoreSalesRowImplicits.ssPricing.get(storeSalesRow)
  }

  object StoreSalesRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[StoreSalesRow], field)
        .buildChecked[T]()

    lazy val ssSoldDateSk = invoke[Long]("ssSoldDateSk")
    lazy val ssSoldTimeSk = invoke[Long]("ssSoldTimeSk")
    lazy val ssSoldItemSk = invoke[Long]("ssSoldItemSk")
    lazy val ssSoldCustomerSk = invoke[Long]("ssSoldCustomerSk")
    lazy val ssSoldCdemoSk = invoke[Long]("ssSoldCdemoSk")
    lazy val ssSoldHdemoSk = invoke[Long]("ssSoldHdemoSk")
    lazy val ssSoldAddrSk = invoke[Long]("ssSoldAddrSk")
    lazy val ssSoldStoreSk = invoke[Long]("ssSoldStoreSk")
    lazy val ssSoldPromoSk = invoke[Long]("ssSoldPromoSk")
    lazy val ssTicketNumber = invoke[Long]("ssTicketNumber")
    lazy val ssPricing = invoke[Pricing]("ssPricing")

    def values(row: StoreSalesRow): Array[Any] = Array(
      getOrNullForKey(row, row.getSsSoldDateSk, SS_SOLD_DATE_SK),
      getOrNullForKey(row, row.getSsSoldTimeSk, SS_SOLD_TIME_SK),
      getOrNullForKey(row, row.getSsSoldItemSk, SS_SOLD_ITEM_SK),
      getOrNullForKey(row, row.getSsSoldCustomerSk, SS_SOLD_CUSTOMER_SK),
      getOrNullForKey(row, row.getSsSoldCdemoSk, SS_SOLD_CDEMO_SK),
      getOrNullForKey(row, row.getSsSoldHdemoSk, SS_SOLD_HDEMO_SK),
      getOrNullForKey(row, row.getSsSoldAddrSk, SS_SOLD_ADDR_SK),
      getOrNullForKey(row, row.getSsSoldStoreSk, SS_SOLD_STORE_SK),
      getOrNullForKey(row, row.getSsSoldPromoSk, SS_SOLD_PROMO_SK),
      getOrNullForKey(row, row.getSsTicketNumber, SS_TICKET_NUMBER),
      getOrNull(row, row.getSsPricing.getQuantity(), SS_PRICING_QUANTITY),
      getOrNull(row, row.getSsPricing.getWholesaleCost(), SS_PRICING_WHOLESALE_COST),
      getOrNull(row, row.getSsPricing.getListPrice(), SS_PRICING_LIST_PRICE),
      getOrNull(row, row.getSsPricing.getSalesPrice(), SS_PRICING_SALES_PRICE),
      getOrNull(row, row.getSsPricing.getCouponAmount(), SS_PRICING_COUPON_AMT),
      getOrNull(row, row.getSsPricing.getExtSalesPrice(), SS_PRICING_EXT_SALES_PRICE),
      getOrNull(row, row.getSsPricing.getExtWholesaleCost(), SS_PRICING_EXT_WHOLESALE_COST),
      getOrNull(row, row.getSsPricing.getExtListPrice(), SS_PRICING_EXT_LIST_PRICE),
      getOrNull(row, row.getSsPricing.getExtTax(), SS_PRICING_EXT_TAX),
      getOrNull(row, row.getSsPricing.getCouponAmount(), SS_PRICING_COUPON_AMT),
      getOrNull(row, row.getSsPricing.getNetPaid(), SS_PRICING_NET_PAID),
      getOrNull(row, row.getSsPricing.getNetPaidIncludingTax(), SS_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getSsPricing.getNetProfit(), SS_PRICING_NET_PROFIT))
  }

  implicit class InventoryRowImplicits(inventoryRow: InventoryRow) {
    def getInvDateSk: Long = InventoryRowImplicits.invDateSk.get(inventoryRow)
    def getInvItemSk: Long = InventoryRowImplicits.invItemSk.get(inventoryRow)
    def getInvWarehouseSk: Long = InventoryRowImplicits.invWarehouseSk.get(inventoryRow)
    def getInvQuantityOnHand: Int = InventoryRowImplicits.invQuantityOnHand.get(inventoryRow)
  }

  object InventoryRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[InventoryRow], field)
        .buildChecked[T]()

    lazy val invDateSk = invoke[Long]("invDateSk")
    lazy val invItemSk = invoke[Long]("invItemSk")
    lazy val invWarehouseSk = invoke[Long]("invWarehouseSk")
    lazy val invQuantityOnHand = invoke[Int]("invQuantityOnHand")

    def values(row: InventoryRow): Array[Any] = Array(
      getOrNullForKey(row, row.getInvDateSk, INV_DATE_SK),
      getOrNullForKey(row, row.getInvItemSk, INV_ITEM_SK),
      getOrNullForKey(row, row.getInvWarehouseSk, INV_WAREHOUSE_SK),
      getOrNull(row, row.getInvQuantityOnHand, INV_QUANTITY_ON_HAND))
  }

  implicit class WebReturnsRowImplicits(webReturnsRow: WebReturnsRow) {
    def getWrReturnedDateSk: Long = WebReturnsRowImplicits.wrReturnedDateSk.get(webReturnsRow)
    def getWrReturnedTimeSk: Long = WebReturnsRowImplicits.wrReturnedTimeSk.get(webReturnsRow)
    def getWrItemSk: Long = WebReturnsRowImplicits.wrItemSk.get(webReturnsRow)
    def getWrRefundedCustomerSk: Long =
      WebReturnsRowImplicits.wrRefundedCustomerSk.get(webReturnsRow)
    def getWrRefundedCdemoSk: Long = WebReturnsRowImplicits.wrRefundedCdemoSk.get(webReturnsRow)
    def getWrRefundedHdemoSk: Long = WebReturnsRowImplicits.wrRefundedHdemoSk.get(webReturnsRow)
    def getWrRefundedAddrSk: Long = WebReturnsRowImplicits.wrRefundedAddrSk.get(webReturnsRow)
    def getWrReturningCustomerSk: Long =
      WebReturnsRowImplicits.wrReturningCustomerSk.get(webReturnsRow)
    def getWrReturningCdemoSk: Long = WebReturnsRowImplicits.wrReturningCdemoSk.get(webReturnsRow)
    def getWrReturningHdemoSk: Long = WebReturnsRowImplicits.wrReturningHdemoSk.get(webReturnsRow)
    def getWrReturningAddrSk: Long = WebReturnsRowImplicits.wrReturningAddrSk.get(webReturnsRow)
    def getWrWebPageSk: Long = WebReturnsRowImplicits.wrWebPageSk.get(webReturnsRow)
    def getWrReasonSk: Long = WebReturnsRowImplicits.wrReasonSk.get(webReturnsRow)
    def getWrOrderNumber: Long = WebReturnsRowImplicits.wrOrderNumber.get(webReturnsRow)
    def getWrPricing: Pricing = WebReturnsRowImplicits.wrPricing.get(webReturnsRow)
  }

  object WebReturnsRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[WebReturnsRow], field)
        .buildChecked[T]()

    lazy val wrReturnedDateSk = invoke[Long]("wrReturnedDateSk")
    lazy val wrReturnedTimeSk = invoke[Long]("wrReturnedTimeSk")
    lazy val wrItemSk = invoke[Long]("wrItemSk")
    lazy val wrRefundedCustomerSk = invoke[Long]("wrRefundedCustomerSk")
    lazy val wrRefundedCdemoSk = invoke[Long]("wrRefundedCdemoSk")
    lazy val wrRefundedHdemoSk = invoke[Long]("wrRefundedHdemoSk")
    lazy val wrRefundedAddrSk = invoke[Long]("wrRefundedAddrSk")
    lazy val wrReturningCustomerSk = invoke[Long]("wrReturningCustomerSk")
    lazy val wrReturningCdemoSk = invoke[Long]("wrReturningCdemoSk")
    lazy val wrReturningHdemoSk = invoke[Long]("wrReturningHdemoSk")
    lazy val wrReturningAddrSk = invoke[Long]("wrReturningAddrSk")
    lazy val wrWebPageSk = invoke[Long]("wrWebPageSk")
    lazy val wrReasonSk = invoke[Long]("wrReasonSk")
    lazy val wrOrderNumber = invoke[Long]("wrOrderNumber")
    lazy val wrPricing = invoke[Pricing]("wrPricing")

    def values(row: WebReturnsRow): Array[Any] = Array(
      getOrNullForKey(row, row.getWrReturnedDateSk, WR_RETURNED_DATE_SK),
      getOrNullForKey(row, row.getWrReturnedTimeSk, WR_RETURNED_TIME_SK),
      getOrNullForKey(row, row.getWrItemSk, WR_ITEM_SK),
      getOrNullForKey(row, row.getWrRefundedCustomerSk, WR_REFUNDED_CUSTOMER_SK),
      getOrNullForKey(row, row.getWrRefundedCdemoSk, WR_REFUNDED_CDEMO_SK),
      getOrNullForKey(row, row.getWrRefundedHdemoSk, WR_REFUNDED_HDEMO_SK),
      getOrNullForKey(row, row.getWrRefundedAddrSk, WR_REFUNDED_ADDR_SK),
      getOrNullForKey(row, row.getWrReturningCustomerSk, WR_RETURNING_CUSTOMER_SK),
      getOrNullForKey(row, row.getWrReturningCdemoSk, WR_RETURNING_CDEMO_SK),
      getOrNullForKey(row, row.getWrReturningHdemoSk, WR_RETURNING_HDEMO_SK),
      getOrNullForKey(row, row.getWrReturningAddrSk, WR_RETURNING_ADDR_SK),
      getOrNullForKey(row, row.getWrWebPageSk, WR_WEB_PAGE_SK),
      getOrNullForKey(row, row.getWrReasonSk, WR_REASON_SK),
      getOrNullForKey(row, row.getWrOrderNumber, WR_ORDER_NUMBER),
      getOrNull(row, row.getWrPricing.getQuantity(), WR_PRICING_QUANTITY),
      getOrNull(row, row.getWrPricing.getNetPaid(), WR_PRICING_NET_PAID),
      getOrNull(row, row.getWrPricing.getExtTax(), WR_PRICING_EXT_TAX),
      getOrNull(row, row.getWrPricing.getNetPaidIncludingTax(), WR_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getWrPricing.getFee(), WR_PRICING_FEE),
      getOrNull(row, row.getWrPricing.getExtShipCost(), WR_PRICING_EXT_SHIP_COST),
      getOrNull(row, row.getWrPricing.getRefundedCash(), WR_PRICING_REFUNDED_CASH),
      getOrNull(row, row.getWrPricing.getReversedCharge(), WR_PRICING_REVERSED_CHARGE),
      getOrNull(row, row.getWrPricing.getStoreCredit(), WR_PRICING_STORE_CREDIT),
      getOrNull(row, row.getWrPricing.getNetLoss(), WR_PRICING_NET_LOSS))
  }

  implicit class WarehouseRowImplicits(warehouseRow: WarehouseRow) {
    def getWWarehouseSk: Long = WarehouseRowImplicits.wWarehouseSk.get(warehouseRow)
    def getWWarehouseId: String = WarehouseRowImplicits.wWarehouseId.get(warehouseRow)
    def getWWarehouseName: String = WarehouseRowImplicits.wWarehouseName.get(warehouseRow)
    def getWWarehouseSqFt: Int = WarehouseRowImplicits.wWarehouseSqFt.get(warehouseRow)
    def getWAddress: Address = WarehouseRowImplicits.wAddress.get(warehouseRow)
  }

  object WarehouseRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[WarehouseRow], field)
        .buildChecked[T]()

    lazy val wWarehouseSk = invoke[Long]("wWarehouseSk")
    lazy val wWarehouseId = invoke[String]("wWarehouseId")
    lazy val wWarehouseName = invoke[String]("wWarehouseName")
    lazy val wWarehouseSqFt = invoke[Int]("wWarehouseSqFt")
    lazy val wAddress = invoke[Address]("wAddress")

    def values(row: WarehouseRow): Array[Any] = Array(
      getOrNullForKey(row, row.getWWarehouseSk, W_WAREHOUSE_SK),
      getOrNull(row, row.getWWarehouseId, W_WAREHOUSE_ID),
      getOrNull(row, row.getWWarehouseName, W_WAREHOUSE_NAME),
      getOrNull(row, row.getWWarehouseSqFt, W_WAREHOUSE_SQ_FT),
      getOrNull(row, row.getWAddress.getStreetNumber(), W_ADDRESS_STREET_NUM),
      getOrNull(row, row.getWAddress.getStreetName(), W_ADDRESS_STREET_NAME1),
      getOrNull(row, row.getWAddress.getStreetType(), W_ADDRESS_STREET_TYPE),
      getOrNull(row, row.getWAddress.getSuiteNumber(), W_ADDRESS_SUITE_NUM),
      getOrNull(row, row.getWAddress.getCity(), W_ADDRESS_CITY),
      getOrNull(row, row.getWAddress.getCounty(), W_ADDRESS_COUNTY),
      getOrNull(row, row.getWAddress.getState(), W_ADDRESS_STATE),
      getOrNull(
        row,
        java.lang.String.format("%05d", row.getWAddress.getZip.asInstanceOf[Object]),
        W_ADDRESS_ZIP),
      getOrNull(row, row.getWAddress.getCountry(), W_ADDRESS_COUNTRY),
      getOrNull(row, row.getWAddress.getGmtOffset(), W_ADDRESS_GMT_OFFSET))
  }

  implicit class CustomerRowImplicits(customerRow: CustomerRow) {
    def getCCustomerSk: Long = CustomerRowImplicits.cCustomerSk.get(customerRow)
    def getCCustomerId: String = CustomerRowImplicits.cCustomerId.get(customerRow)
    def getCCurrentCdemoSk: Long = CustomerRowImplicits.cCurrentCdemoSk.get(customerRow)
    def getCCurrentHdemoSk: Long = CustomerRowImplicits.cCurrentHdemoSk.get(customerRow)
    def getCCurrentAddrSk: Long = CustomerRowImplicits.cCurrentAddrSk.get(customerRow)
    def getCFirstShiptoDateId: Int = CustomerRowImplicits.cFirstShiptoDateId.get(customerRow)
    def getCFirstSalesDateId: Int = CustomerRowImplicits.cFirstSalesDateId.get(customerRow)
    def getCSalutation: String = CustomerRowImplicits.cSalutation.get(customerRow)
    def getCFirstName: String = CustomerRowImplicits.cFirstName.get(customerRow)
    def getCLastName: String = CustomerRowImplicits.cLastName.get(customerRow)
    def isCPreferredCustFlag: Boolean = CustomerRowImplicits.cPreferredCustFlag.get(customerRow)
    def getCBirthDay: Int = CustomerRowImplicits.cBirthDay.get(customerRow)
    def getCBirthMonth: Int = CustomerRowImplicits.cBirthMonth.get(customerRow)
    def getCBirthYear: Int = CustomerRowImplicits.cBirthYear.get(customerRow)
    def getCBirthCountry: String = CustomerRowImplicits.cBirthCountry.get(customerRow)
    def getCLogin: String = CustomerRowImplicits.cLogin.get(customerRow)
    def getCEmailAddress: String = CustomerRowImplicits.cEmailAddress.get(customerRow)
    def getCLastReviewDate: Int = CustomerRowImplicits.cLastReviewDate.get(customerRow)
  }

  object CustomerRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CustomerRow], field)
        .buildChecked[T]()

    lazy val cCustomerSk = invoke[Long]("cCustomerSk")
    lazy val cCustomerId = invoke[String]("cCustomerId")
    lazy val cCurrentCdemoSk = invoke[Long]("cCurrentCdemoSk")
    lazy val cCurrentHdemoSk = invoke[Long]("cCurrentHdemoSk")
    lazy val cCurrentAddrSk = invoke[Long]("cCurrentAddrSk")
    lazy val cFirstShiptoDateId = invoke[Int]("cFirstShiptoDateId")
    lazy val cFirstSalesDateId = invoke[Int]("cFirstSalesDateId")
    lazy val cSalutation = invoke[String]("cSalutation")
    lazy val cFirstName = invoke[String]("cFirstName")
    lazy val cLastName = invoke[String]("cLastName")
    lazy val cPreferredCustFlag = invoke[Boolean]("cPreferredCustFlag")
    lazy val cBirthDay = invoke[Int]("cBirthDay")
    lazy val cBirthMonth = invoke[Int]("cBirthMonth")
    lazy val cBirthYear = invoke[Int]("cBirthYear")
    lazy val cBirthCountry = invoke[String]("cBirthCountry")
    lazy val cLogin = invoke[String]("cLogin")
    lazy val cEmailAddress = invoke[String]("cEmailAddress")
    lazy val cLastReviewDate = invoke[Int]("cLastReviewDate")

    def values(row: CustomerRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCCustomerSk, C_CUSTOMER_SK),
      getOrNull(row, row.getCCustomerId, C_CUSTOMER_ID),
      getOrNullForKey(row, row.getCCurrentCdemoSk, C_CURRENT_CDEMO_SK),
      getOrNullForKey(row, row.getCCurrentHdemoSk, C_CURRENT_HDEMO_SK),
      getOrNullForKey(row, row.getCCurrentAddrSk, C_CURRENT_ADDR_SK),
      getOrNull(row, row.getCFirstShiptoDateId, C_FIRST_SHIPTO_DATE_ID),
      getOrNull(row, row.getCFirstSalesDateId, C_FIRST_SALES_DATE_ID),
      getOrNull(row, row.getCSalutation, C_SALUTATION),
      getOrNull(row, row.getCFirstName, C_FIRST_NAME),
      getOrNull(row, row.getCLastName, C_LAST_NAME),
      getOrNullForBoolean(row, row.isCPreferredCustFlag, C_PREFERRED_CUST_FLAG),
      getOrNull(row, row.getCBirthDay, C_BIRTH_DAY),
      getOrNull(row, row.getCBirthMonth, C_BIRTH_MONTH),
      getOrNull(row, row.getCBirthYear, C_BIRTH_YEAR),
      getOrNull(row, row.getCBirthCountry, C_BIRTH_COUNTRY),
      row.getCLogin,
      getOrNull(row, row.getCEmailAddress, C_EMAIL_ADDRESS),
      getOrNull(row, row.getCLastReviewDate, C_LAST_REVIEW_DATE))
  }

  implicit class StoreReturnsRowImplicits(storeReturnsRow: StoreReturnsRow) {
    def getSrReturnedDateSk: Long = StoreReturnsRowImplicits.srReturnedDateSk.get(storeReturnsRow)
    def getSrReturnedTimeSk: Long = StoreReturnsRowImplicits.srReturnedTimeSk.get(storeReturnsRow)
    def getSrItemSk: Long = StoreReturnsRowImplicits.srItemSk.get(storeReturnsRow)
    def getSrCustomerSk: Long = StoreReturnsRowImplicits.srCustomerSk.get(storeReturnsRow)
    def getSrCdemoSk: Long = StoreReturnsRowImplicits.srCdemoSk.get(storeReturnsRow)
    def getSrHdemoSk: Long = StoreReturnsRowImplicits.srHdemoSk.get(storeReturnsRow)
    def getSrAddrSk: Long = StoreReturnsRowImplicits.srAddrSk.get(storeReturnsRow)
    def getSrStoreSk: Long = StoreReturnsRowImplicits.srStoreSk.get(storeReturnsRow)
    def getSrReasonSk: Long = StoreReturnsRowImplicits.srReasonSk.get(storeReturnsRow)
    def getSrTicketNumber: Long = StoreReturnsRowImplicits.srTicketNumber.get(storeReturnsRow)
    def getSrPricing: Pricing = StoreReturnsRowImplicits.srPricing.get(storeReturnsRow)
  }

  object StoreReturnsRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[StoreReturnsRow], field)
        .buildChecked[T]()

    lazy val srReturnedDateSk = invoke[Long]("srReturnedDateSk")
    lazy val srReturnedTimeSk = invoke[Long]("srReturnedTimeSk")
    lazy val srItemSk = invoke[Long]("srItemSk")
    lazy val srCustomerSk = invoke[Long]("srCustomerSk")
    lazy val srCdemoSk = invoke[Long]("srCdemoSk")
    lazy val srHdemoSk = invoke[Long]("srHdemoSk")
    lazy val srAddrSk = invoke[Long]("srAddrSk")
    lazy val srStoreSk = invoke[Long]("srStoreSk")
    lazy val srReasonSk = invoke[Long]("srReasonSk")
    lazy val srTicketNumber = invoke[Long]("srTicketNumber")
    lazy val srPricing = invoke[Pricing]("srPricing")

    def values(row: StoreReturnsRow): Array[Any] = Array(
      getOrNullForKey(row, row.getSrReturnedDateSk, SR_RETURNED_DATE_SK),
      getOrNullForKey(row, row.getSrReturnedTimeSk, SR_RETURNED_TIME_SK),
      getOrNullForKey(row, row.getSrItemSk, SR_ITEM_SK),
      getOrNullForKey(row, row.getSrCustomerSk, SR_CUSTOMER_SK),
      getOrNullForKey(row, row.getSrCdemoSk, SR_CDEMO_SK),
      getOrNullForKey(row, row.getSrHdemoSk, SR_HDEMO_SK),
      getOrNullForKey(row, row.getSrAddrSk, SR_ADDR_SK),
      getOrNullForKey(row, row.getSrStoreSk, SR_STORE_SK),
      getOrNullForKey(row, row.getSrReasonSk, SR_REASON_SK),
      getOrNullForKey(row, row.getSrTicketNumber, SR_TICKET_NUMBER),
      getOrNull(row, row.getSrPricing.getQuantity(), SR_PRICING_QUANTITY),
      getOrNull(row, row.getSrPricing.getNetPaid(), SR_PRICING_NET_PAID),
      getOrNull(row, row.getSrPricing.getExtTax(), SR_PRICING_EXT_TAX),
      getOrNull(row, row.getSrPricing.getNetPaidIncludingTax(), SR_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getSrPricing.getFee(), SR_PRICING_FEE),
      getOrNull(row, row.getSrPricing.getExtShipCost(), SR_PRICING_EXT_SHIP_COST),
      getOrNull(row, row.getSrPricing.getRefundedCash(), SR_PRICING_REFUNDED_CASH),
      getOrNull(row, row.getSrPricing.getReversedCharge(), SR_PRICING_REVERSED_CHARGE),
      getOrNull(row, row.getSrPricing.getStoreCredit(), SR_PRICING_STORE_CREDIT),
      getOrNull(row, row.getSrPricing.getNetLoss(), SR_PRICING_NET_LOSS))
  }

  implicit class CatalogReturnsRowImplicits(catalogReturnsRow: CatalogReturnsRow) {
    def getCrReturnedDateSk: Long =
      CatalogReturnsRowImplicits.crReturnedDateSk.get(catalogReturnsRow)
    def getCrReturnedTimeSk: Long =
      CatalogReturnsRowImplicits.crReturnedTimeSk.get(catalogReturnsRow)
    def getCrItemSk: Long = CatalogReturnsRowImplicits.crItemSk.get(catalogReturnsRow)
    def getCrRefundedCustomerSk: Long =
      CatalogReturnsRowImplicits.crRefundedCustomerSk.get(catalogReturnsRow)
    def getCrRefundedCdemoSk: Long =
      CatalogReturnsRowImplicits.crRefundedCdemoSk.get(catalogReturnsRow)
    def getCrRefundedHdemoSk: Long =
      CatalogReturnsRowImplicits.crRefundedHdemoSk.get(catalogReturnsRow)
    def getCrRefundedAddrSk: Long =
      CatalogReturnsRowImplicits.crRefundedAddrSk.get(catalogReturnsRow)
    def getCrReturningCustomerSk: Long =
      CatalogReturnsRowImplicits.crReturningCustomerSk.get(catalogReturnsRow)
    def getCrReturningCdemoSk: Long =
      CatalogReturnsRowImplicits.crReturningCdemoSk.get(catalogReturnsRow)
    def getCrReturningHdemoSk: Long =
      CatalogReturnsRowImplicits.crReturningHdemoSk.get(catalogReturnsRow)
    def getCrReturningAddrSk: Long =
      CatalogReturnsRowImplicits.crReturningAddrSk.get(catalogReturnsRow)
    def getCrCallCenterSk: Long = CatalogReturnsRowImplicits.crCallCenterSk.get(catalogReturnsRow)
    def getCrCatalogPageSk: Long = CatalogReturnsRowImplicits.crCatalogPageSk.get(catalogReturnsRow)
    def getCrShipModeSk: Long = CatalogReturnsRowImplicits.crShipModeSk.get(catalogReturnsRow)
    def getCrWarehouseSk: Long = CatalogReturnsRowImplicits.crWarehouseSk.get(catalogReturnsRow)
    def getCrReasonSk: Long = CatalogReturnsRowImplicits.crReasonSk.get(catalogReturnsRow)
    def getCrOrderNumber: Long = CatalogReturnsRowImplicits.crOrderNumber.get(catalogReturnsRow)
    def getCrPricing: Pricing = CatalogReturnsRowImplicits.crPricing.get(catalogReturnsRow)
  }

  object CatalogReturnsRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CatalogReturnsRow], field)
        .buildChecked[T]()

    lazy val crReturnedDateSk = invoke[Long]("crReturnedDateSk")
    lazy val crReturnedTimeSk = invoke[Long]("crReturnedTimeSk")
    lazy val crItemSk = invoke[Long]("crItemSk")
    lazy val crRefundedCustomerSk = invoke[Long]("crRefundedCustomerSk")
    lazy val crRefundedCdemoSk = invoke[Long]("crRefundedCdemoSk")
    lazy val crRefundedHdemoSk = invoke[Long]("crRefundedHdemoSk")
    lazy val crRefundedAddrSk = invoke[Long]("crRefundedAddrSk")
    lazy val crReturningCustomerSk = invoke[Long]("crReturningCustomerSk")
    lazy val crReturningCdemoSk = invoke[Long]("crReturningCdemoSk")
    lazy val crReturningHdemoSk = invoke[Long]("crReturningHdemoSk")
    lazy val crReturningAddrSk = invoke[Long]("crReturningAddrSk")
    lazy val crCallCenterSk = invoke[Long]("crCallCenterSk")
    lazy val crCatalogPageSk = invoke[Long]("crCatalogPageSk")
    lazy val crShipModeSk = invoke[Long]("crShipModeSk")
    lazy val crWarehouseSk = invoke[Long]("crWarehouseSk")
    lazy val crReasonSk = invoke[Long]("crReasonSk")
    lazy val crOrderNumber = invoke[Long]("crOrderNumber")
    lazy val crPricing = invoke[Pricing]("crPricing")

    def values(row: CatalogReturnsRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCrReturnedDateSk, CR_RETURNED_DATE_SK),
      getOrNullForKey(row, row.getCrReturnedTimeSk, CR_RETURNED_TIME_SK),
      getOrNullForKey(row, row.getCrItemSk, CR_ITEM_SK),
      getOrNullForKey(row, row.getCrRefundedCustomerSk, CR_REFUNDED_CUSTOMER_SK),
      getOrNullForKey(row, row.getCrRefundedCdemoSk, CR_REFUNDED_CDEMO_SK),
      getOrNullForKey(row, row.getCrRefundedHdemoSk, CR_REFUNDED_HDEMO_SK),
      getOrNullForKey(row, row.getCrRefundedAddrSk, CR_REFUNDED_ADDR_SK),
      getOrNullForKey(row, row.getCrReturningCustomerSk, CR_RETURNING_CUSTOMER_SK),
      getOrNullForKey(row, row.getCrReturningCdemoSk, CR_RETURNING_CDEMO_SK),
      getOrNullForKey(row, row.getCrReturningHdemoSk, CR_RETURNING_HDEMO_SK),
      getOrNullForKey(row, row.getCrReturningAddrSk, CR_RETURNING_ADDR_SK),
      getOrNullForKey(row, row.getCrCallCenterSk, CR_CALL_CENTER_SK),
      getOrNullForKey(row, row.getCrCatalogPageSk, CR_CATALOG_PAGE_SK),
      getOrNullForKey(row, row.getCrShipModeSk, CR_SHIP_MODE_SK),
      getOrNullForKey(row, row.getCrWarehouseSk, CR_WAREHOUSE_SK),
      getOrNullForKey(row, row.getCrReasonSk, CR_REASON_SK),
      getOrNull(row, row.getCrOrderNumber, CR_ORDER_NUMBER),
      getOrNull(row, row.getCrPricing.getQuantity(), CR_PRICING_QUANTITY),
      getOrNull(row, row.getCrPricing.getNetPaid(), CR_PRICING_NET_PAID),
      getOrNull(row, row.getCrPricing.getExtTax(), CR_PRICING_EXT_TAX),
      getOrNull(row, row.getCrPricing.getNetPaidIncludingTax(), CR_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getCrPricing.getFee(), CR_PRICING_FEE),
      getOrNull(row, row.getCrPricing.getExtShipCost(), CR_PRICING_EXT_SHIP_COST),
      getOrNull(row, row.getCrPricing.getRefundedCash(), CR_PRICING_REFUNDED_CASH),
      getOrNull(row, row.getCrPricing.getReversedCharge(), CR_PRICING_REVERSED_CHARGE),
      getOrNull(row, row.getCrPricing.getStoreCredit(), CR_PRICING_STORE_CREDIT),
      getOrNull(row, row.getCrPricing.getNetLoss(), CR_PRICING_NET_LOSS))
  }

  implicit class CatalogSalesRowImplicits(catalogSalesRow: CatalogSalesRow) {
    def getCsSoldDateSk: Long = CatalogSalesRowImplicits.csSoldDateSk.get(catalogSalesRow)
    def getCsSoldTimeSk: Long = CatalogSalesRowImplicits.csSoldTimeSk.get(catalogSalesRow)
    def getCsShipDateSk: Long = CatalogSalesRowImplicits.csShipDateSk.get(catalogSalesRow)
    def getCsBillCustomerSk: Long = CatalogSalesRowImplicits.csBillCustomerSk.get(catalogSalesRow)
    def getCsBillCdemoSk: Long = CatalogSalesRowImplicits.csBillCdemoSk.get(catalogSalesRow)
    def getCsBillHdemoSk: Long = CatalogSalesRowImplicits.csBillHdemoSk.get(catalogSalesRow)
    def getCsBillAddrSk: Long = CatalogSalesRowImplicits.csBillAddrSk.get(catalogSalesRow)
    def getCsShipCustomerSk: Long = CatalogSalesRowImplicits.csShipCustomerSk.get(catalogSalesRow)
    def getCsShipCdemoSk: Long = CatalogSalesRowImplicits.csShipCdemoSk.get(catalogSalesRow)
    def getCsShipHdemoSk: Long = CatalogSalesRowImplicits.csShipHdemoSk.get(catalogSalesRow)
    def getCsShipAddrSk: Long = CatalogSalesRowImplicits.csShipAddrSk.get(catalogSalesRow)
    def getCsCallCenterSk: Long = CatalogSalesRowImplicits.csCallCenterSk.get(catalogSalesRow)
    def getCsCatalogPageSk: Long = CatalogSalesRowImplicits.csCatalogPageSk.get(catalogSalesRow)
    def getCsShipModeSk: Long = CatalogSalesRowImplicits.csShipModeSk.get(catalogSalesRow)
    def getCsWarehouseSk: Long = CatalogSalesRowImplicits.csWarehouseSk.get(catalogSalesRow)
    def getCsSoldItemSk: Long = CatalogSalesRowImplicits.csSoldItemSk.get(catalogSalesRow)
    def getCsPromoSk: Long = CatalogSalesRowImplicits.csPromoSk.get(catalogSalesRow)
    def getCsOrderNumber: Long = CatalogSalesRowImplicits.csOrderNumber.get(catalogSalesRow)
    def getCsPricing: Pricing = CatalogSalesRowImplicits.csPricing.get(catalogSalesRow)
  }

  object CatalogSalesRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CatalogSalesRow], field)
        .buildChecked[T]()

    lazy val csSoldDateSk = invoke[Long]("csSoldDateSk")
    lazy val csSoldTimeSk = invoke[Long]("csSoldTimeSk")
    lazy val csShipDateSk = invoke[Long]("csShipDateSk")
    lazy val csBillCustomerSk = invoke[Long]("csBillCustomerSk")
    lazy val csBillCdemoSk = invoke[Long]("csBillCdemoSk")
    lazy val csBillHdemoSk = invoke[Long]("csBillHdemoSk")
    lazy val csBillAddrSk = invoke[Long]("csBillAddrSk")
    lazy val csShipCustomerSk = invoke[Long]("csShipCustomerSk")
    lazy val csShipCdemoSk = invoke[Long]("csShipCdemoSk")
    lazy val csShipHdemoSk = invoke[Long]("csShipHdemoSk")
    lazy val csShipAddrSk = invoke[Long]("csShipAddrSk")
    lazy val csCallCenterSk = invoke[Long]("csCallCenterSk")
    lazy val csCatalogPageSk = invoke[Long]("csCatalogPageSk")
    lazy val csShipModeSk = invoke[Long]("csShipModeSk")
    lazy val csWarehouseSk = invoke[Long]("csWarehouseSk")
    lazy val csSoldItemSk = invoke[Long]("csSoldItemSk")
    lazy val csPromoSk = invoke[Long]("csPromoSk")
    lazy val csOrderNumber = invoke[Long]("csOrderNumber")
    lazy val csPricing = invoke[Pricing]("csPricing")

    def values(row: CatalogSalesRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCsSoldDateSk, CS_SOLD_DATE_SK),
      getOrNullForKey(row, row.getCsSoldTimeSk, CS_SOLD_TIME_SK),
      getOrNullForKey(row, row.getCsShipDateSk, CS_SHIP_DATE_SK),
      getOrNullForKey(row, row.getCsBillCustomerSk, CS_BILL_CUSTOMER_SK),
      getOrNullForKey(row, row.getCsBillCdemoSk, CS_BILL_CDEMO_SK),
      getOrNullForKey(row, row.getCsBillHdemoSk, CS_BILL_HDEMO_SK),
      getOrNullForKey(row, row.getCsBillAddrSk, CS_BILL_ADDR_SK),
      getOrNullForKey(row, row.getCsShipCustomerSk, CS_SHIP_CUSTOMER_SK),
      getOrNullForKey(row, row.getCsShipCdemoSk, CS_SHIP_CDEMO_SK),
      getOrNullForKey(row, row.getCsShipHdemoSk, CS_SHIP_HDEMO_SK),
      getOrNullForKey(row, row.getCsShipAddrSk, CS_SHIP_ADDR_SK),
      getOrNullForKey(row, row.getCsCallCenterSk, CS_CALL_CENTER_SK),
      getOrNullForKey(row, row.getCsCatalogPageSk, CS_CATALOG_PAGE_SK),
      getOrNullForKey(row, row.getCsShipModeSk, CS_SHIP_MODE_SK),
      getOrNull(row, row.getCsWarehouseSk, CS_WAREHOUSE_SK),
      getOrNullForKey(row, row.getCsSoldItemSk, CS_SOLD_ITEM_SK),
      getOrNullForKey(row, row.getCsPromoSk, CS_PROMO_SK),
      getOrNull(row, row.getCsOrderNumber, CS_ORDER_NUMBER),
      getOrNull(row, row.getCsPricing.getQuantity(), CS_PRICING_QUANTITY),
      getOrNull(row, row.getCsPricing.getWholesaleCost(), CS_PRICING_WHOLESALE_COST),
      getOrNull(row, row.getCsPricing.getListPrice(), CS_PRICING_LIST_PRICE),
      getOrNull(row, row.getCsPricing.getSalesPrice(), CS_PRICING_SALES_PRICE),
      getOrNull(row, row.getCsPricing.getExtDiscountAmount(), CS_PRICING_EXT_DISCOUNT_AMOUNT),
      getOrNull(row, row.getCsPricing.getExtSalesPrice(), CS_PRICING_EXT_SALES_PRICE),
      getOrNull(row, row.getCsPricing.getExtWholesaleCost(), CS_PRICING_EXT_WHOLESALE_COST),
      getOrNull(row, row.getCsPricing.getExtListPrice(), CS_PRICING_EXT_LIST_PRICE),
      getOrNull(row, row.getCsPricing.getExtTax(), CS_PRICING_EXT_TAX),
      getOrNull(row, row.getCsPricing.getCouponAmount(), CS_PRICING_COUPON_AMT),
      getOrNull(row, row.getCsPricing.getExtShipCost(), CS_PRICING_EXT_SHIP_COST),
      getOrNull(row, row.getCsPricing.getNetPaid(), CS_PRICING_NET_PAID),
      getOrNull(row, row.getCsPricing.getNetPaidIncludingTax(), CS_PRICING_NET_PAID_INC_TAX),
      getOrNull(row, row.getCsPricing.getNetPaidIncludingShipping(), CS_PRICING_NET_PAID_INC_SHIP),
      getOrNull(
        row,
        row.getCsPricing.getNetPaidIncludingShippingAndTax(),
        CS_PRICING_NET_PAID_INC_SHIP_TAX),
      getOrNull(row, row.getCsPricing.getNetProfit(), CS_PRICING_NET_PROFIT))
  }

  implicit class WebPageRowImplicits(webPageRow: WebPageRow) {
    def getWpPageSk: Long = WebPageRowImplicits.wpPageSk.get(webPageRow)
    def getWpPageId: String = WebPageRowImplicits.wpPageId.get(webPageRow)
    def getWpRecStartDateId: Long = WebPageRowImplicits.wpRecStartDateId.get(webPageRow)
    def getWpRecEndDateId: Long = WebPageRowImplicits.wpRecEndDateId.get(webPageRow)
    def getWpCreationDateSk: Long = WebPageRowImplicits.wpCreationDateSk.get(webPageRow)
    def getWpAccessDateSk: Long = WebPageRowImplicits.wpAccessDateSk.get(webPageRow)
    def isWpAutogenFlag: Boolean = WebPageRowImplicits.wpAutogenFlag.get(webPageRow)
    def getWpCustomerSk: Long = WebPageRowImplicits.wpCustomerSk.get(webPageRow)
    def getWpUrl: String = WebPageRowImplicits.wpUrl.get(webPageRow)
    def getWpType: String = WebPageRowImplicits.wpType.get(webPageRow)
    def getWpCharCount: Int = WebPageRowImplicits.wpCharCount.get(webPageRow)
    def getWpLinkCount: Int = WebPageRowImplicits.wpLinkCount.get(webPageRow)
    def getWpImageCount: Int = WebPageRowImplicits.wpImageCount.get(webPageRow)
    def getWpMaxAdCount: Int = WebPageRowImplicits.wpMaxAdCount.get(webPageRow)
  }

  object WebPageRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[WebPageRow], field)
        .buildChecked[T]()

    lazy val wpPageSk = invoke[Long]("wpPageSk")
    lazy val wpPageId = invoke[String]("wpPageId")
    lazy val wpRecStartDateId = invoke[Long]("wpRecStartDateId")
    lazy val wpRecEndDateId = invoke[Long]("wpRecEndDateId")
    lazy val wpCreationDateSk = invoke[Long]("wpCreationDateSk")
    lazy val wpAccessDateSk = invoke[Long]("wpAccessDateSk")
    lazy val wpAutogenFlag = invoke[Boolean]("wpAutogenFlag")
    lazy val wpCustomerSk = invoke[Long]("wpCustomerSk")
    lazy val wpUrl = invoke[String]("wpUrl")
    lazy val wpType = invoke[String]("wpType")
    lazy val wpCharCount = invoke[Int]("wpCharCount")
    lazy val wpLinkCount = invoke[Int]("wpLinkCount")
    lazy val wpImageCount = invoke[Int]("wpImageCount")
    lazy val wpMaxAdCount = invoke[Int]("wpMaxAdCount")

    def values(row: WebPageRow): Array[Any] = Array(
      getOrNullForKey(row, row.getWpPageSk, WP_PAGE_SK),
      getOrNull(row, row.getWpPageId, WP_PAGE_ID),
      getDateOrNullFromJulianDays(row, row.getWpRecStartDateId, WP_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getWpRecEndDateId, WP_REC_END_DATE_ID),
      getOrNullForKey(row, row.getWpCreationDateSk, WP_CREATION_DATE_SK),
      getOrNullForKey(row, row.getWpAccessDateSk, WP_ACCESS_DATE_SK),
      getOrNullForBoolean(row, row.isWpAutogenFlag, WP_AUTOGEN_FLAG),
      getOrNullForKey(row, row.getWpCustomerSk, WP_CUSTOMER_SK),
      getOrNull(row, row.getWpUrl, WP_URL),
      getOrNull(row, row.getWpType, WP_TYPE),
      getOrNull(row, row.getWpCharCount, WP_CHAR_COUNT),
      getOrNull(row, row.getWpLinkCount, WP_LINK_COUNT),
      getOrNull(row, row.getWpImageCount, WP_IMAGE_COUNT),
      getOrNull(row, row.getWpMaxAdCount, WP_MAX_AD_COUNT))
  }

  implicit class CallCenterRowImplicits(callCenterRow: CallCenterRow) {
    def getCcCallCenterSk: Long = CallCenterRowImplicits.ccCallCenterSk.get(callCenterRow)
    def getCcCallCenterId: String = CallCenterRowImplicits.ccCallCenterId.get(callCenterRow)
    def getCcRecStartDateId: Long = CallCenterRowImplicits.ccRecStartDateId.get(callCenterRow)
    def getCcRecEndDateId: Long = CallCenterRowImplicits.ccRecEndDateId.get(callCenterRow)
    def getCcClosedDateId: Long = CallCenterRowImplicits.ccClosedDateId.get(callCenterRow)
    def getCcOpenDateId: Long = CallCenterRowImplicits.ccOpenDateId.get(callCenterRow)
    def getCcName: String = CallCenterRowImplicits.ccName.get(callCenterRow)
    def getCcClass: String = CallCenterRowImplicits.ccClass.get(callCenterRow)
    def getCcEmployees: Int = CallCenterRowImplicits.ccEmployees.get(callCenterRow)
    def getCcSqFt: Int = CallCenterRowImplicits.ccSqFt.get(callCenterRow)
    def getCcHours: String = CallCenterRowImplicits.ccHours.get(callCenterRow)
    def getCcManager: String = CallCenterRowImplicits.ccManager.get(callCenterRow)
    def getCcMarketId: Int = CallCenterRowImplicits.ccMarketId.get(callCenterRow)
    def getCcMarketClass: String = CallCenterRowImplicits.ccMarketClass.get(callCenterRow)
    def getCcMarketDesc: String = CallCenterRowImplicits.ccMarketDesc.get(callCenterRow)
    def getCcMarketManager: String = CallCenterRowImplicits.ccMarketManager.get(callCenterRow)
    def getCcDivisionId: Int = CallCenterRowImplicits.ccDivisionId.get(callCenterRow)
    def getCcDivisionName: String = CallCenterRowImplicits.ccDivisionName.get(callCenterRow)
    def getCcCompany: Int = CallCenterRowImplicits.ccCompany.get(callCenterRow)
    def getCcCompanyName: String = CallCenterRowImplicits.ccCompanyName.get(callCenterRow)
    def getCcAddress: Address = CallCenterRowImplicits.ccAddress.get(callCenterRow)
    def getCcTaxPercentage: TPCDSDecimal = CallCenterRowImplicits.ccTaxPercentage.get(callCenterRow)
  }

  object CallCenterRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CallCenterRow], field)
        .buildChecked[T]()

    lazy val ccCallCenterSk = invoke[Long]("ccCallCenterSk")
    lazy val ccCallCenterId = invoke[String]("ccCallCenterId")
    lazy val ccRecStartDateId = invoke[Long]("ccRecStartDateId")
    lazy val ccRecEndDateId = invoke[Long]("ccRecEndDateId")
    lazy val ccClosedDateId = invoke[Long]("ccClosedDateId")
    lazy val ccOpenDateId = invoke[Long]("ccOpenDateId")
    lazy val ccName = invoke[String]("ccName")
    lazy val ccClass = invoke[String]("ccClass")
    lazy val ccEmployees = invoke[Int]("ccEmployees")
    lazy val ccSqFt = invoke[Int]("ccSqFt")
    lazy val ccHours = invoke[String]("ccHours")
    lazy val ccManager = invoke[String]("ccManager")
    lazy val ccMarketId = invoke[Int]("ccMarketId")
    lazy val ccMarketClass = invoke[String]("ccMarketClass")
    lazy val ccMarketDesc = invoke[String]("ccMarketDesc")
    lazy val ccMarketManager = invoke[String]("ccMarketManager")
    lazy val ccDivisionId = invoke[Int]("ccDivisionId")
    lazy val ccDivisionName = invoke[String]("ccDivisionName")
    lazy val ccCompany = invoke[Int]("ccCompany")
    lazy val ccCompanyName = invoke[String]("ccCompanyName")
    lazy val ccAddress = invoke[Address]("ccAddress")
    lazy val ccTaxPercentage = invoke[TPCDSDecimal]("ccTaxPercentage")

    def values(row: CallCenterRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCcCallCenterSk, CC_CALL_CENTER_SK),
      getOrNull(row, row.getCcCallCenterId, CC_CALL_CENTER_ID),
      getDateOrNullFromJulianDays(row, row.getCcRecStartDateId, CC_REC_START_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getCcRecEndDateId, CC_REC_END_DATE_ID),
      getOrNullForKey(row, row.getCcClosedDateId, CC_CLOSED_DATE_ID),
      getOrNullForKey(row, row.getCcOpenDateId, CC_OPEN_DATE_ID),
      getOrNull(row, row.getCcName, CC_NAME),
      getOrNull(row, row.getCcClass, CC_CLASS),
      getOrNull(row, row.getCcEmployees, CC_EMPLOYEES),
      getOrNull(row, row.getCcSqFt, CC_SQ_FT),
      getOrNull(row, row.getCcHours, CC_HOURS),
      getOrNull(row, row.getCcManager, CC_MANAGER),
      getOrNull(row, row.getCcMarketId, CC_MARKET_ID),
      getOrNull(row, row.getCcMarketClass, CC_MARKET_CLASS),
      getOrNull(row, row.getCcMarketDesc, CC_MARKET_DESC),
      getOrNull(row, row.getCcMarketManager, CC_MARKET_MANAGER),
      getOrNull(row, row.getCcDivisionId, CC_DIVISION),
      getOrNull(row, row.getCcDivisionName, CC_DIVISION_NAME),
      getOrNull(row, row.getCcCompany, CC_COMPANY),
      getOrNull(row, row.getCcCompanyName, CC_COMPANY_NAME),
      getOrNull(row, row.getCcAddress.getStreetNumber, CC_STREET_NUMBER),
      getOrNull(row, row.getCcAddress.getStreetName, CC_STREET_NAME),
      getOrNull(row, row.getCcAddress.getStreetType, CC_STREET_TYPE),
      getOrNull(row, row.getCcAddress.getSuiteNumber, CC_SUITE_NUMBER),
      getOrNull(row, row.getCcAddress.getCity, CC_CITY),
      getOrNull(row, row.getCcAddress.getCounty, CC_ADDRESS),
      getOrNull(row, row.getCcAddress.getState, CC_STATE),
      getOrNull(
        row,
        java.lang.String.format("%05d", row.getCcAddress.getZip.asInstanceOf[Object]),
        CC_ZIP),
      getOrNull(row, row.getCcAddress.getCountry, CC_COUNTRY),
      getOrNull(row, row.getCcAddress.getGmtOffset, CC_GMT_OFFSET),
      getOrNull(row, row.getCcTaxPercentage, CC_TAX_PERCENTAGE))
  }

  implicit class CustomerAddressRowImplicits(customerAddressRow: CustomerAddressRow) {
    def getCaAddrSk: Long = CustomerAddressRowImplicits.caAddrSk.get(customerAddressRow)
    def getCaAddrId: String = CustomerAddressRowImplicits.caAddrId.get(customerAddressRow)
    def getCaAddress: Address = CustomerAddressRowImplicits.caAddress.get(customerAddressRow)
    def getCaLocationType: String =
      CustomerAddressRowImplicits.caLocationType.get(customerAddressRow)
  }

  object CustomerAddressRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[CustomerAddressRow], field)
        .buildChecked[T]()

    lazy val caAddrSk = invoke[Long]("caAddrSk")
    lazy val caAddrId = invoke[String]("caAddrId")
    lazy val caAddress = invoke[Address]("caAddress")
    lazy val caLocationType = invoke[String]("caLocationType")

    def values(row: CustomerAddressRow): Array[Any] = Array(
      getOrNullForKey(row, row.getCaAddrSk, CA_ADDRESS_SK),
      getOrNull(row, row.getCaAddrId, CA_ADDRESS_ID),
      getOrNull(row, row.getCaAddress.getStreetNumber(), CA_ADDRESS_STREET_NUM),
      getOrNull(row, row.getCaAddress.getStreetName(), CA_ADDRESS_STREET_NAME),
      getOrNull(row, row.getCaAddress.getStreetType(), CA_ADDRESS_STREET_TYPE),
      getOrNull(row, row.getCaAddress.getSuiteNumber(), CA_ADDRESS_SUITE_NUM),
      getOrNull(row, row.getCaAddress.getCity(), CA_ADDRESS_CITY),
      getOrNull(row, row.getCaAddress.getCounty(), CA_ADDRESS_COUNTY),
      getOrNull(row, row.getCaAddress.getState(), CA_ADDRESS_STATE),
      getOrNull(
        row,
        java.lang.String.format("%05d", row.getCaAddress.getZip.asInstanceOf[Object]),
        CA_ADDRESS_ZIP),
      getOrNull(row, row.getCaAddress.getCountry(), CA_ADDRESS_COUNTRY),
      getOrNull(row, row.getCaAddress.getGmtOffset(), CA_ADDRESS_GMT_OFFSET),
      getOrNull(row, row.getCaLocationType, CA_LOCATION_TYPE))
  }

  implicit class DateDimRowImplicits(dateDimRow: DateDimRow) {
    def getDDateSk: Long = DateDimRowImplicits.dDateSk.get(dateDimRow)
    def getDDateId: String = DateDimRowImplicits.dDateId.get(dateDimRow)
    def getDMonthSeq: Int = DateDimRowImplicits.dMonthSeq.get(dateDimRow)
    def getDWeekSeq: Int = DateDimRowImplicits.dWeekSeq.get(dateDimRow)
    def getDQuarterSeq: Int = DateDimRowImplicits.dQuarterSeq.get(dateDimRow)
    def getDYear: Int = DateDimRowImplicits.dYear.get(dateDimRow)
    def getDDow: Int = DateDimRowImplicits.dDow.get(dateDimRow)
    def getDMoy: Int = DateDimRowImplicits.dMoy.get(dateDimRow)
    def getDDom: Int = DateDimRowImplicits.dDom.get(dateDimRow)
    def getDQoy: Int = DateDimRowImplicits.dQoy.get(dateDimRow)
    def getDFyYear: Int = DateDimRowImplicits.dFyYear.get(dateDimRow)
    def getDFyQuarterSeq: Int = DateDimRowImplicits.dFyQuarterSeq.get(dateDimRow)
    def getDFyWeekSeq: Int = DateDimRowImplicits.dFyWeekSeq.get(dateDimRow)
    def getDDayName: String = DateDimRowImplicits.dDayName.get(dateDimRow)
    def isDHoliday: Boolean = DateDimRowImplicits.dHoliday.get(dateDimRow)
    def isDWeekend: Boolean = DateDimRowImplicits.dWeekend.get(dateDimRow)
    def isDFollowingHoliday: Boolean = DateDimRowImplicits.dFollowingHoliday.get(dateDimRow)
    def getDFirstDom: Int = DateDimRowImplicits.dFirstDom.get(dateDimRow)
    def getDLastDom: Int = DateDimRowImplicits.dLastDom.get(dateDimRow)
    def getDSameDayLy: Int = DateDimRowImplicits.dSameDayLy.get(dateDimRow)
    def getDSameDayLq: Int = DateDimRowImplicits.dSameDayLq.get(dateDimRow)
    def isDCurrentDay: Boolean = DateDimRowImplicits.dCurrentDay.get(dateDimRow)
    def isDCurrentWeek: Boolean = DateDimRowImplicits.dCurrentWeek.get(dateDimRow)
    def isDCurrentMonth: Boolean = DateDimRowImplicits.dCurrentMonth.get(dateDimRow)
    def isDCurrentQuarter: Boolean = DateDimRowImplicits.dCurrentQuarter.get(dateDimRow)
    def isDCurrentYear: Boolean = DateDimRowImplicits.dCurrentYear.get(dateDimRow)
  }

  object DateDimRowImplicits {
    def invoke[T](field: String): DynFields.UnboundField[T] =
      DynFields.builder()
        .hiddenImpl(classOf[DateDimRow], field)
        .buildChecked[T]()

    lazy val dDateSk = invoke[Long]("dDateSk")
    lazy val dDateId = invoke[String]("dDateId")
    lazy val dMonthSeq = invoke[Int]("dMonthSeq")
    lazy val dWeekSeq = invoke[Int]("dWeekSeq")
    lazy val dQuarterSeq = invoke[Int]("dQuarterSeq")
    lazy val dYear = invoke[Int]("dYear")
    lazy val dDow = invoke[Int]("dDow")
    lazy val dMoy = invoke[Int]("dMoy")
    lazy val dDom = invoke[Int]("dDom")
    lazy val dQoy = invoke[Int]("dQoy")
    lazy val dFyYear = invoke[Int]("dFyYear")
    lazy val dFyQuarterSeq = invoke[Int]("dFyQuarterSeq")
    lazy val dFyWeekSeq = invoke[Int]("dFyWeekSeq")
    lazy val dDayName = invoke[String]("dDayName")
    lazy val dHoliday = invoke[Boolean]("dHoliday")
    lazy val dWeekend = invoke[Boolean]("dWeekend")
    lazy val dFollowingHoliday = invoke[Boolean]("dFollowingHoliday")
    lazy val dFirstDom = invoke[Int]("dFirstDom")
    lazy val dLastDom = invoke[Int]("dLastDom")
    lazy val dSameDayLy = invoke[Int]("dSameDayLy")
    lazy val dSameDayLq = invoke[Int]("dSameDayLq")
    lazy val dCurrentDay = invoke[Boolean]("dCurrentDay")
    lazy val dCurrentWeek = invoke[Boolean]("dCurrentWeek")
    lazy val dCurrentMonth = invoke[Boolean]("dCurrentMonth")
    lazy val dCurrentQuarter = invoke[Boolean]("dCurrentQuarter")
    lazy val dCurrentYear = invoke[Boolean]("dCurrentYear")

    def values(row: DateDimRow): Array[Any] = Array(
      getOrNullForKey(row, row.getDDateSk, D_DATE_SK),
      getOrNull(row, row.getDDateId, D_DATE_ID),
      getDateOrNullFromJulianDays(row, row.getDDateSk, D_DATE_SK),
      getOrNull(row, row.getDMonthSeq, D_MONTH_SEQ),
      getOrNull(row, row.getDWeekSeq, D_WEEK_SEQ),
      getOrNull(row, row.getDQuarterSeq, D_QUARTER_SEQ),
      getOrNull(row, row.getDYear, D_YEAR),
      getOrNull(row, row.getDDow, D_DOW),
      getOrNull(row, row.getDMoy, D_MOY),
      getOrNull(row, row.getDDom, D_DOM),
      getOrNull(row, row.getDQoy, D_QOY),
      getOrNull(row, row.getDFyYear, D_FY_YEAR),
      getOrNull(row, row.getDFyQuarterSeq, D_FY_QUARTER_SEQ),
      getOrNull(row, row.getDFyWeekSeq, D_FY_WEEK_SEQ),
      getOrNull(row, row.getDDayName, D_DAY_NAME),
      getOrNull(
        row,
        java.lang.String.format(
          "%4dQ%d",
          row.getDYear.asInstanceOf[Object],
          row.getDQoy.asInstanceOf[Object]),
        D_QUARTER_NAME),
      getOrNullForBoolean(row, row.isDHoliday, D_HOLIDAY),
      getOrNullForBoolean(row, row.isDWeekend, D_WEEKEND),
      getOrNullForBoolean(row, row.isDFollowingHoliday, D_FOLLOWING_HOLIDAY),
      getOrNull(row, row.getDFirstDom, D_FIRST_DOM),
      getOrNull(row, row.getDLastDom, D_LAST_DOM),
      getOrNull(row, row.getDSameDayLy, D_SAME_DAY_LY),
      getOrNull(row, row.getDSameDayLq, D_SAME_DAY_LQ),
      getOrNullForBoolean(row, row.isDCurrentDay, D_CURRENT_DAY),
      getOrNullForBoolean(row, row.isDCurrentWeek, D_CURRENT_WEEK),
      getOrNullForBoolean(row, row.isDCurrentMonth, D_CURRENT_MONTH),
      getOrNullForBoolean(row, row.isDCurrentQuarter, D_CURRENT_QUARTER),
      getOrNullForBoolean(row, row.isDCurrentYear, D_CURRENT_YEAR))
  }

  def getValues: TableRow => Array[Any] = {
    case row: StoreRow => StoreRowImplicits.values(row)
    case row: ReasonRow => ReasonRowImplicits.values(row)
    case row: DbgenVersionRow => DbgenVersionRowImplicits.values(row)
    case row: ShipModeRow => ShipModeRowImplicits.values(row)
    case row: IncomeBandRow => IncomeBandRowImplicits.values(row)
    case row: ItemRow => ItemRowImplicits.values(row)
    case row: CustomerDemographicsRow => CustomerDemographicsRowImplicits.values(row)
    case row: TimeDimRow => TimeDimRowImplicits.values(row)
    case row: WebSiteRow => WebSiteRowImplicits.values(row)
    case row: HouseholdDemographicsRow => HouseholdDemographicsRowImplicits.values(row)
    case row: PromotionRow => PromotionRowImplicits.values(row)
    case row: CatalogPageRow => CatalogPageRowImplicits.values(row)
    case row: WebSalesRow => WebSalesRowImplicits.values(row)
    case row: StoreSalesRow => StoreSalesRowImplicits.values(row)
    case row: InventoryRow => InventoryRowImplicits.values(row)
    case row: WebReturnsRow => WebReturnsRowImplicits.values(row)
    case row: WarehouseRow => WarehouseRowImplicits.values(row)
    case row: CustomerRow => CustomerRowImplicits.values(row)
    case row: StoreReturnsRow => StoreReturnsRowImplicits.values(row)
    case row: CatalogReturnsRow => CatalogReturnsRowImplicits.values(row)
    case row: CatalogSalesRow => CatalogSalesRowImplicits.values(row)
    case row: WebPageRow => WebPageRowImplicits.values(row)
    case row: CallCenterRow => CallCenterRowImplicits.values(row)
    case row: CustomerAddressRow => CustomerAddressRowImplicits.values(row)
    case row: DateDimRow => DateDimRowImplicits.values(row)
  }
}

object KyuubiTPCDSTableRowWithNullsUtils {
  private lazy val isNullMethod = DynMethods.builder("isNull")
    .hiddenImpl(
      classOf[TableRowWithNulls],
      classOf[GeneratorColumn])
    .build()

  private def isNull(
      row: TableRow,
      column: GeneratorColumn): Boolean = isNullMethod.invoke[Boolean](row, column)

  def getDateOrNullFromJulianDays(
      row: TableRow,
      value: Long,
      column: GeneratorColumn): Option[Long] = {
    if (isNull(row, column) || value < 0) None else Some(value)
  }

  def getOrNullForKey(row: TableRow, value: Long, column: GeneratorColumn): Option[Long] = {
    if (isNull(row, column) || value == -1) None else Some(value)
  }

  def getOrNull[T](row: TableRow, value: T, column: GeneratorColumn): Option[T] = {
    if (isNull(row, column)) None else Some(value)
  }

  def getOrNullForBoolean(
      row: TableRow,
      value: Boolean,
      column: GeneratorColumn): Option[Boolean] = {
    if (isNull(row, column)) None else Some(value)
  }
}
