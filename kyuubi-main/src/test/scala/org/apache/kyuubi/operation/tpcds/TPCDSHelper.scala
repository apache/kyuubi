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

package org.apache.kyuubi.operation.tpcds

trait TPCDSHelper {

  protected val format: String = "parquet"
  protected val database: String = "default"

  case class TableIdent(name: String) {
    def qualifiedName: String = database + "." + name
  }

  case class Field(name: String, dataType: String, isPartitionKey: Boolean = false) {
    override def toString: String = s"$name $dataType"
  }

  case class Table(table: TableIdent, fields: Seq[Field]) {
    def create: String = {
      val partitions = fields.filter(_.isPartitionKey).map(_.name)
      s"""
         |CREATE TABLE ${table.qualifiedName} ${fields.map(_.toString).mkString("(", ", ", ")")}
         |USING $format
         |${if (partitions.nonEmpty) partitions.mkString("PARTITIONED BY (", ", ", ")") else ""}
         |""".stripMargin
    }
  }

  private val int: String = "INT"

  private val char1 = "CHAR(1)"
  private val char2 = "CHAR(2)"
  private val char6 = "CHAR(6)"
  private val char9 = "CHAR(9)"
  private val char10 = "CHAR(10)"
  private val char13 = "CHAR(13)"
  private val char15 = "CHAR(15)"
  private val char16 = "CHAR(16)"
  private val char20 = "CHAR(20)"
  private val char30 = "CHAR(30)"
  private val char50 = "CHAR(50)"
  private val char100 = "CHAR(100)"

  private val varchar10 = "VARCHAR(10)"
  private val varchar20 = "VARCHAR(20)"
  private val varchar30 = "VARCHAR(30)"
  private val varchar40 = "VARCHAR(40)"
  private val varchar50 = "VARCHAR(50)"
  private val varchar60 = "VARCHAR(60)"
  private val varchar100 = "VARCHAR(100)"
  private val varchar200 = "VARCHAR(200)"

  private val dec_5_2 = "DECIMAL(5,2)"
  private val dec_7_2 = "DECIMAL(7,2)"
  private val dec_15_2 = "DECIMAL(15,2)"

  private val date: String = "DATE"

  val STORE_SALES: Table = {
    val tableIdent = TableIdent("store_sales")
    val fields = Seq(
      Field("ss_sold_date_sk", int, isPartitionKey = true),
      Field("ss_sold_time_sk", int),
      Field("ss_item_sk", int),
      Field("ss_customer_sk", int),
      Field("ss_cdemo_sk", int),
      Field("ss_hdemo_sk", int),
      Field("ss_addr_sk", int),
      Field("ss_store_sk", int),
      Field("ss_promo_sk", int),
      Field("ss_ticket_number", int),
      Field("ss_quantity", int),
      Field("ss_wholesale_cost", dec_7_2),
      Field("ss_list_price", dec_7_2),
      Field("ss_sales_price", dec_7_2),
      Field("ss_ext_discount_amt", dec_7_2),
      Field("ss_ext_sales_price", dec_7_2),
      Field("ss_ext_wholesale_cost", dec_7_2),
      Field("ss_ext_list_price", dec_7_2),
      Field("ss_ext_tax", dec_7_2),
      Field("ss_coupon_amt", dec_7_2),
      Field("ss_net_paid", dec_7_2),
      Field("ss_net_paid_inc_tax", dec_7_2),
      Field("ss_net_profit", dec_7_2))
    Table(tableIdent, fields)
  }

  val STORE_RETURNS: Table = {
    val tableIdent = TableIdent("store_returns")
    val fields = Seq(
      Field("sr_returned_date_sk", int, isPartitionKey = true),
      Field("sr_return_time_sk", int),
      Field("sr_item_sk", int),
      Field("sr_customer_sk", int),
      Field("sr_cdemo_sk", int),
      Field("sr_hdemo_sk", int),
      Field("sr_addr_sk", int),
      Field("sr_store_sk", int),
      Field("sr_reason_sk", int),
      Field("sr_ticket_number", int),
      Field("sr_return_quantity", int),
      Field("sr_return_amt", int),
      Field("sr_return_tax", dec_7_2),
      Field("sr_return_amt_inc_tax", dec_7_2),
      Field("sr_fee", dec_7_2),
      Field("sr_return_ship_cost", dec_7_2),
      Field("sr_refunded_cash", dec_7_2),
      Field("sr_reversed_charge", dec_7_2),
      Field("sr_store_credit", dec_7_2),
      Field("sr_net_loss", dec_7_2))
    Table(tableIdent, fields)
  }

  val CATALOG_SALES: Table = {
    val tableIdent = TableIdent("catalog_sales")
    val fields = Seq(
      Field("cs_sold_date_sk", int, isPartitionKey = true),
      Field("cs_sold_time_sk", int),
      Field("cs_ship_date_sk", int),
      Field("cs_bill_customer_sk", int),
      Field("cs_bill_cdemo_sk", int),
      Field("cs_bill_hdemo_sk", int),
      Field("cs_bill_addr_sk", int),
      Field("cs_ship_customer_sk", int),
      Field("cs_ship_cdemo_sk", int),
      Field("cs_ship_hdemo_sk", int),
      Field("cs_ship_addr_sk", int),
      Field("cs_call_center_sk", int),
      Field("cs_catalog_page_sk", int),
      Field("cs_ship_mode_sk", int),
      Field("cs_warehouse_sk", int),
      Field("cs_item_sk", int),
      Field("cs_promo_sk", int),
      Field("cs_order_number", int),
      Field("cs_quantity", int),
      Field("cs_wholesale_cost", dec_7_2),
      Field("cs_list_price", dec_7_2),
      Field("cs_sales_price", dec_7_2),
      Field("cs_ext_discount_amt", dec_7_2),
      Field("cs_ext_sales_price", dec_7_2),
      Field("cs_ext_wholesale_cost", dec_7_2),
      Field("cs_ext_list_price", dec_7_2),
      Field("cs_ext_tax", dec_7_2),
      Field("cs_coupon_amt", dec_7_2),
      Field("cs_ext_ship_cost", dec_7_2),
      Field("cs_net_paid", dec_7_2),
      Field("cs_net_paid_inc_tax", dec_7_2),
      Field("cs_net_paid_inc_ship", dec_7_2),
      Field("cs_net_paid_inc_ship_tax", dec_7_2),
      Field("cs_net_profit", dec_7_2))
    Table(tableIdent, fields)
  }

  val CATALOG_RETURNS: Table = {
    val tableIdent = TableIdent("catalog_returns")
    val fields = Seq(
      Field("cr_returned_date_sk", int, isPartitionKey = true),
      Field("cr_returned_time_sk", int),
      Field("cr_item_sk", int),
      Field("cr_refunded_customer_sk", int),
      Field("cr_refunded_cdemo_sk", int),
      Field("cr_refunded_hdemo_sk", int),
      Field("cr_refunded_addr_sk", int),
      Field("cr_returning_customer_sk", int),
      Field("cr_returning_cdemo_sk", int),
      Field("cr_returning_hdemo_sk", int),
      Field("cr_returning_addr_sk", int),
      Field("cr_call_center_sk", int),
      Field("cr_catalog_page_sk", int),
      Field("cr_ship_mode_sk", int),
      Field("cr_warehouse_sk", int),
      Field("cr_reason_sk", int),
      Field("cr_order_number", int),
      Field("cr_return_quantity", int),
      Field("cr_return_amount", dec_7_2),
      Field("cr_return_tax", dec_7_2),
      Field("cr_return_amt_inc_tax", dec_7_2),
      Field("cr_fee", dec_7_2),
      Field("cr_return_ship_cost", dec_7_2),
      Field("cr_refunded_cash", dec_7_2),
      Field("cr_reversed_charge", dec_7_2),
      Field("cr_store_credit", dec_7_2),
      Field("cr_net_loss", dec_7_2))
    Table(tableIdent, fields)
  }

  val WEB_SALES: Table = {
    val tableIdent = TableIdent("web_sales")
    val fields = Seq(
      Field("ws_sold_date_sk", int, isPartitionKey = true),
      Field("ws_sold_time_sk", int),
      Field("ws_ship_date_sk", int),
      Field("ws_item_sk", int),
      Field("ws_bill_customer_sk", int),
      Field("ws_bill_cdemo_sk", int),
      Field("ws_bill_hdemo_sk", int),
      Field("ws_bill_addr_sk", int),
      Field("ws_ship_customer_sk", int),
      Field("ws_ship_cdemo_sk", int),
      Field("ws_ship_hdemo_sk", int),
      Field("ws_ship_addr_sk", int),
      Field("ws_web_page_sk", int),
      Field("ws_web_site_sk", int),
      Field("ws_ship_mode_sk", int),
      Field("ws_warehouse_sk", int),
      Field("ws_promo_sk", int),
      Field("ws_order_number", int),
      Field("ws_quantity", int),
      Field("ws_wholesale_cost", dec_7_2),
      Field("ws_list_price", dec_7_2),
      Field("ws_sales_price", dec_7_2),
      Field("ws_ext_discount_amt", dec_7_2),
      Field("ws_ext_sales_price", dec_7_2),
      Field("ws_ext_wholesale_cost", dec_7_2),
      Field("ws_ext_list_price", dec_7_2),
      Field("ws_ext_tax", dec_7_2),
      Field("ws_coupon_amt", dec_7_2),
      Field("ws_ext_ship_cost", dec_7_2),
      Field("ws_net_paid", dec_7_2),
      Field("ws_net_paid_inc_tax", dec_7_2),
      Field("ws_net_paid_inc_ship", dec_7_2),
      Field("ws_net_paid_inc_ship_tax", dec_7_2),
      Field("ws_net_profit", dec_7_2))
    Table(tableIdent, fields)
  }

  val WEB_RETURNS: Table = {
    val tableIdent = TableIdent("web_returns")
    val fields = Seq(
      Field("wr_returned_date_sk", int, isPartitionKey = true),
      Field("wr_returned_time_sk", int),
      Field("wr_item_sk", int),
      Field("wr_refunded_customer_sk", int),
      Field("wr_refunded_cdemo_sk", int),
      Field("wr_refunded_hdemo_sk", int),
      Field("wr_refunded_addr_sk", int),
      Field("wr_returning_customer_sk", int),
      Field("wr_returning_cdemo_sk", int),
      Field("wr_returning_hdemo_sk", int),
      Field("wr_returning_addr_sk", int),
      Field("wr_web_page_sk", int),
      Field("wr_reason_sk", int),
      Field("wr_order_number", int),
      Field("wr_return_quantity", int),
      Field("wr_return_amt", dec_7_2),
      Field("wr_return_tax", dec_7_2),
      Field("wr_return_amt_inc_tax", dec_7_2),
      Field("wr_fee", dec_7_2),
      Field("wr_return_ship_cost", dec_7_2),
      Field("wr_refunded_cash", dec_7_2),
      Field("wr_reversed_charge", dec_7_2),
      Field("wr_account_credit", dec_7_2),
      Field("wr_net_loss", dec_7_2))
    Table(tableIdent, fields)
  }

  val INVENTORY: Table = {
    val tableIdent = TableIdent("inventory")
    val fields = Seq(
      Field("inv_date_sk", int, isPartitionKey = true),
      Field("inv_item_sk", int),
      Field("inv_warehouse_sk", int),
      Field("inv_quantity_on_hand", int))
    Table(tableIdent, fields)
  }


  val STORE: Table = {
    val tableIdent = TableIdent("store")
    val fields = Seq(
      Field("s_store_sk", int),
      Field("s_store_id", char16),
      Field("s_rec_start_date", date),
      Field("s_rec_end_date", date),
      Field("s_closed_date_sk", int),
      Field("s_store_name", varchar50),
      Field("s_number_employees", int),
      Field("s_floor_space", int),
      Field("s_hours", char20),
      Field("s_manager", varchar40),
      Field("s_market_id", int),
      Field("s_geography_class", varchar100),
      Field("s_market_desc", varchar100),
      Field("s_market_manager", varchar40),
      Field("s_division_id", int),
      Field("s_division_name", varchar50),
      Field("s_company_id", int),
      Field("s_company_name", varchar50),
      Field("s_street_number", varchar10),
      Field("s_street_name", varchar60),
      Field("s_street_type", char15),
      Field("s_suite_number", char10),
      Field("s_city", varchar60),
      Field("s_county", varchar30),
      Field("s_state", char2),
      Field("s_zip", char10),
      Field("s_country", varchar20),
      Field("s_gmt_offset", dec_5_2),
      Field("s_tax_percentage", dec_5_2))
    Table(tableIdent, fields)
  }

  val CALL_CENTER: Table = {
    val tableIdent = TableIdent("call_center")
    val fields = Seq(
      Field("cc_call_center_sk", int),
      Field("cc_call_center_id", char16),
      Field("cc_rec_start_date", date),
      Field("cc_rec_end_date", date),
      Field("cc_closed_date_sk", int),
      Field("cc_open_date_sk", int),
      Field("cc_name", varchar50),
      Field("cc_class", varchar50),
      Field("cc_employees", int),
      Field("cc_sq_ft", int),
      Field("cc_hours", char20),
      Field("cc_manager", varchar40),
      Field("cc_mkt_id", int),
      Field("cc_mkt_class", char50),
      Field("cc_mkt_desc", varchar100),
      Field("cc_market_manager", varchar40),
      Field("cc_division", int),
      Field("cc_division_name", varchar50),
      Field("cc_company", int),
      Field("cc_company_name", varchar50),
      Field("cc_street_number", varchar10),
      Field("cc_street_name", varchar60),
      Field("cc_street_type", char15),
      Field("cc_suite_number", char10),
      Field("cc_city", varchar60),
      Field("cc_county", varchar30),
      Field("cc_state", char2),
      Field("cc_zip", char10),
      Field("cc_country", varchar20),
      Field("cc_gmt_offset", dec_5_2),
      Field("cc_tax_percentage", dec_5_2))
    Table(tableIdent, fields)
  }

  val CATALOG_PAGE: Table = {
    val tableIdent = TableIdent("catalog_page")
    val fields = Seq(
      Field("cp_catalog_page_sk", int),
      Field("cp_catalog_page_id", char16),
      Field("cp_start_date_sk", int),
      Field("cp_end_date_sk", int),
      Field("cp_department", varchar50),
      Field("cp_catalog_number", int),
      Field("cp_catalog_page_number", int),
      Field("cp_description", varchar100),
      Field("cp_type", varchar100))
    Table(tableIdent, fields)
  }

  val WEB_SITE: Table = {
    val tableIdent = TableIdent("web_site")
    val fields = Seq(
      Field("web_site_sk", int),
      Field("web_site_id", char16),
      Field("web_rec_start_date", date),
      Field("web_rec_end_date", date),
      Field("web_name", varchar50),
      Field("web_open_date_sk", int),
      Field("web_close_date_sk", int),
      Field("web_class", varchar50),
      Field("web_manager", int),
      Field("web_mkt_id", int),
      Field("web_mkt_class", char50),
      Field("web_mkt_desc", varchar100),
      Field("web_market_manager", varchar40),
      Field("web_company_id", int),
      Field("web_company_name", varchar50),
      Field("web_street_number", varchar10),
      Field("web_street_name", varchar60),
      Field("web_street_type", char15),
      Field("web_suite_number", char10),
      Field("web_city", varchar60),
      Field("web_county", varchar30),
      Field("web_state", char2),
      Field("web_zip", char10),
      Field("web_country", varchar20),
      Field("web_gmt_offset", dec_5_2),
      Field("web_tax_percentage", dec_5_2))
    Table(tableIdent, fields)
  }

  val WEB_PAGE: Table = {
    val tableIdent = TableIdent("web_page")
    val fields = Seq(
      Field("wp_web_page_sk", int),
      Field("wp_web_page_id", char16),
      Field("wp_rec_start_date", date),
      Field("wp_rec_end_date", date),
      Field("wp_creation_date_sk", int),
      Field("wp_access_date_sk", int),
      Field("wp_autogen_flag", char1),
      Field("wp_customer_sk", int),
      Field("wp_url", varchar100),
      Field("wp_type", char50),
      Field("wp_char_count", int),
      Field("wp_link_count", int),
      Field("wp_image_count", int),
      Field("wp_max_ad_count", int))
    Table(tableIdent, fields)
  }

  val WAREHOUSE: Table = {
    val tableIdent = TableIdent("warehouse")
    val fields = Seq(
      Field("w_warehouse_sk", int),
      Field("w_warehouse_id", char16),
      Field("w_warehouse_name", varchar20),
      Field("w_warehouse_sq_ft", int),
      Field("w_street_number", char10),
      Field("w_street_name", varchar20),
      Field("w_street_type", char15),
      Field("w_suite_number", char10),
      Field("w_city", varchar60),
      Field("w_county", varchar30),
      Field("w_state", char2),
      Field("w_zip", char10),
      Field("w_country", varchar20),
      Field("w_gmt_offset", dec_5_2))
    Table(tableIdent, fields)
  }

  val CUSTOMER: Table = {
    val tableIdent = TableIdent("customer")
    val fields = Seq(
      Field("c_customer_sk", int),
      Field("c_customer_id", char16),
      Field("c_current_cdemo_sk", int),
      Field("c_current_hdemo_sk", int),
      Field("c_current_addr_sk", int),
      Field("c_first_shipto_date_sk", int),
      Field("c_first_sales_date_sk", int),
      Field("c_salutation", char10),
      Field("c_first_name", char20),
      Field("c_last_name", char30),
      Field("c_preferred_cust_flag", char1),
      Field("c_birth_day", int),
      Field("c_birth_month", int),
      Field("c_birth_year", int),
      Field("c_birth_country", varchar20),
      Field("c_login", char13),
      Field("c_email_address", char50),
      Field("c_last_review_date", int))
    Table(tableIdent, fields)
  }

  val CUSTOMER_ADDRESS: Table = {
    val tableIdent = TableIdent("customer_address")
    val fields = Seq(
      Field("ca_address_sk", int),
      Field("ca_address_id", char16),
      Field("ca_street_number", char10),
      Field("ca_street_name", varchar20),
      Field("ca_street_type", char15),
      Field("ca_suite_number", char10),
      Field("ca_city", varchar60),
      Field("ca_county", varchar30),
      Field("ca_state", char2),
      Field("ca_zip", char10),
      Field("ca_country", char20),
      Field("ca_gmt_offset", dec_5_2),
      Field("ca_location_type", char20))
    Table(tableIdent, fields)
  }

  val CUSTOMER_DEMOGRAPHICS: Table = {
    val tableIdent = TableIdent("customer_demographics")
    val fields = Seq(
      Field("cd_demo_sk", int),
      Field("cd_gender", char1),
      Field("cd_marital_status", char1),
      Field("cd_education_status", char20),
      Field("cd_purchase_estimate", int),
      Field("cd_credit_rating", char10),
      Field("cd_dep_count", int),
      Field("cd_dep_employed_count", int),
      Field("cd_dep_college_count", int))
    Table(tableIdent, fields)
  }

  val DATE_DIM: Table = {
    val tableIdent = TableIdent("date_dim")

    val fields = Seq(
      Field("d_date_sk", int),
      Field("d_date_id", char16),
      Field("d_date", date),
      Field("d_month_seq", int),
      Field("d_week_seq", int),
      Field("d_quarter_seq", int),
      Field("d_year", int),
      Field("d_dow", int),
      Field("d_moy", int),
      Field("d_dom", int),
      Field("d_qoy", int),
      Field("d_fy_year", int),
      Field("d_fy_quarter_seq", int),
      Field("d_fy_week_seq", int),
      Field("d_day_name", char9),
      Field("d_quarter_name", char6),
      Field("d_holiday", char1),
      Field("d_weekend", char1),
      Field("d_following_holiday", char1),
      Field("d_first_dom", int),
      Field("d_last_dom", int),
      Field("d_same_day_ly", int),
      Field("d_same_day_lq", int),
      Field("d_current_day", char1),
      Field("d_current_week", char1),
      Field("d_current_month", char1),
      Field("d_current_quarter", char1),
      Field("d_current_year", char1))
    Table(tableIdent, fields)
  }

  val HOUSEHOLD_DEMOGRAPHICS: Table = {
    val tableIdent = TableIdent("household_demographics")
    val fields = Seq(
      Field("hd_demo_sk", int),
      Field("hd_income_band_sk", int),
      Field("hd_buy_potential", char15),
      Field("hd_dep_count", int),
      Field("hd_vehicle_count", int))
    Table(tableIdent, fields)
  }


  val ITEM: Table = {
    val tableIdent = TableIdent("item")
    val fields = Seq(
      Field("i_item_sk", int),
      Field("i_item_id", char16),
      Field("i_rec_start_date", date),
      Field("i_rec_end_date", date),
      Field("i_item_desc", varchar200),
      Field("i_current_price", dec_7_2),
      Field("i_wholesale_cost", dec_7_2),
      Field("i_brand_id", int),
      Field("i_brand", char50),
      Field("i_class_id", int),
      Field("i_class", char50),
      Field("i_category_id", int),
      Field("i_category", char50),
      Field("i_manufact_id", int),
      Field("i_manufact", char50),
      Field("i_size", char20),
      Field("i_formulation", char20),
      Field("i_color", char20),
      Field("i_units", char10),
      Field("i_container", char10),
      Field("i_manager_id", int),
      Field("i_product_name", char50))
    Table(tableIdent, fields)
  }

  val INCOME_BAND: Table = {
    val tableIdent = TableIdent("income_band")
    val fields = Seq(
      Field("ib_income_band_sk", int),
      Field("ib_lower_bound", int),
      Field("ib_upper_bound", int))
    Table(tableIdent, fields)
  }

  val PROMOTION: Table = {
    val tableIdent = TableIdent("promotion")
    val fields = Seq(
      Field("p_promo_sk", int),
      Field("p_promo_id", char16),
      Field("p_start_date_sk", int),
      Field("p_end_date_sk", int),
      Field("p_item_sk", int),
      Field("p_cost", dec_15_2),
      Field("p_response_target", int),
      Field("p_promo_name", char50),
      Field("p_channel_dmail", char1),
      Field("p_channel_email", char1),
      Field("p_channel_catalog", char1),
      Field("p_channel_tv", char1),
      Field("p_channel_radio", char1),
      Field("p_channel_press", char1),
      Field("p_channel_event", char1),
      Field("p_channel_demo", char1),
      Field("p_channel_details", varchar100),
      Field("p_purpose", char15),
      Field("p_discount_active", char1))
    Table(tableIdent, fields)
  }

  val REASON: Table = {
    val tableIdent = TableIdent("reason")
    val fields = Seq(
      Field("r_reason_sk", int),
      Field("r_reason_id", char16),
      Field("r_reason_desc", char100))
    Table(tableIdent, fields)
  }

  val SHIP_MODE: Table = {
    val tableIdent = TableIdent("ship_mode")
    val fields = Seq(
      Field("sm_ship_mode_sk", int),
      Field("sm_ship_mode_id", char16),
      Field("sm_type", char30),
      Field("sm_code", char10),
      Field("sm_carrier", char20),
      Field("sm_contract", char20))
    Table(tableIdent, fields)
  }

  val TIME_DIM: Table = {
    val tableIdent = TableIdent("time_dim")
    val fields = Seq(
      Field("t_time_sk", int),
      Field("t_time_id", char16),
      Field("t_time", int),
      Field("t_hour", int),
      Field("t_minute", int),
      Field("t_second", int),
      Field("t_am_pm", char2),
      Field("t_shift", char20),
      Field("t_sub_shift", char20),
      Field("t_meal_time", char20))
    Table(tableIdent, fields)
  }

  val tables = Seq(
    STORE_SALES,
    STORE_RETURNS,
    CATALOG_SALES,
    CATALOG_RETURNS,
    WEB_SALES,
    WEB_RETURNS,
    INVENTORY,
    STORE,
    CALL_CENTER,
    CATALOG_PAGE,
    WEB_SITE,
    WEB_PAGE,
    WAREHOUSE,
    CUSTOMER,
    CUSTOMER_ADDRESS,
    CUSTOMER_DEMOGRAPHICS,
    DATE_DIM,
    HOUSEHOLD_DEMOGRAPHICS,
    ITEM,
    INCOME_BAND,
    PROMOTION,
    REASON,
    SHIP_MODE,
    TIME_DIM)
}

