package fuzzer.data.tables

import fuzzer.data.types._

object Examples {

  val hardcodedTables: List[TableMetadata] = List(
    TableMetadata(
      _identifier = "users",
      _columns = Seq(
        ColumnMetadata("uid", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "auth_system")
    ),
    TableMetadata(
      _identifier = "orders",
      _columns = Seq(
        ColumnMetadata("oid", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "ecommerce")
    ),
    TableMetadata(
      _identifier = "products",
      _columns = Seq(
        ColumnMetadata("pid", IntegerType, isNullable = false, isKey = true),
      ),
      _metadata = Map("source" -> "inventory")
    )
  )

  val tpcdsTables: List[TableMetadata] = List(

    // ------------------------------------------------------------------------------
    // 1) CALL_CENTER
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "call_center",
      _columns =  Seq(
        ColumnMetadata("cc_call_center_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("cc_call_center_id", StringType, isNullable = false),
        ColumnMetadata("cc_rec_start_date", DateType),
        ColumnMetadata("cc_rec_end_date", DateType),
        ColumnMetadata("cc_closed_date_sk", IntegerType),
        ColumnMetadata("cc_open_date_sk", IntegerType),
        ColumnMetadata("cc_name", StringType),
        ColumnMetadata("cc_class", StringType),
        ColumnMetadata("cc_employees", IntegerType),
        ColumnMetadata("cc_sq_ft", IntegerType),
        ColumnMetadata("cc_hours", StringType),
        ColumnMetadata("cc_manager", StringType),
        ColumnMetadata("cc_mkt_id", IntegerType),
        ColumnMetadata("cc_mkt_class", StringType),
        ColumnMetadata("cc_mkt_desc", StringType),
        ColumnMetadata("cc_market_manager", StringType),
        ColumnMetadata("cc_division", IntegerType),
        ColumnMetadata("cc_division_name", StringType),
        ColumnMetadata("cc_company", IntegerType),
        ColumnMetadata("cc_company_name", StringType),
        ColumnMetadata("cc_street_number", StringType),
        ColumnMetadata("cc_street_name", StringType),
        ColumnMetadata("cc_street_type", StringType),
        ColumnMetadata("cc_suite_number", StringType),
        ColumnMetadata("cc_city", StringType),
        ColumnMetadata("cc_county", StringType),
        ColumnMetadata("cc_state", StringType),
        ColumnMetadata("cc_zip", StringType),
        ColumnMetadata("cc_country", StringType),
        ColumnMetadata("cc_gmt_offset", DecimalType),
        ColumnMetadata("cc_tax_percentage", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 2) CATALOG_PAGE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "catalog_page",
      _columns =  Seq(
        ColumnMetadata("cp_catalog_page_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("cp_catalog_page_id", StringType, isNullable = false),
        ColumnMetadata("cp_start_date_sk", IntegerType),
        ColumnMetadata("cp_end_date_sk", IntegerType),
        ColumnMetadata("cp_department", StringType),
        ColumnMetadata("cp_catalog_number", IntegerType),
        ColumnMetadata("cp_catalog_page_number", IntegerType),
        ColumnMetadata("cp_description", StringType),
        ColumnMetadata("cp_type", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 3) CATALOG_RETURNS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "catalog_returns",
      _columns =  Seq(
        ColumnMetadata("cr_returned_date_sk", IntegerType),
        ColumnMetadata("cr_returned_time_sk", IntegerType),
        ColumnMetadata("cr_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("cr_refunded_customer_sk", IntegerType),
        ColumnMetadata("cr_refunded_cdemo_sk", IntegerType),
        ColumnMetadata("cr_refunded_hdemo_sk", IntegerType),
        ColumnMetadata("cr_refunded_addr_sk", IntegerType),
        ColumnMetadata("cr_returning_customer_sk", IntegerType),
        ColumnMetadata("cr_returning_cdemo_sk", IntegerType),
        ColumnMetadata("cr_returning_hdemo_sk", IntegerType),
        ColumnMetadata("cr_returning_addr_sk", IntegerType),
        ColumnMetadata("cr_call_center_sk", IntegerType),
        ColumnMetadata("cr_catalog_page_sk", IntegerType),
        ColumnMetadata("cr_ship_mode_sk", IntegerType),
        ColumnMetadata("cr_warehouse_sk", IntegerType),
        ColumnMetadata("cr_reason_sk", IntegerType),
        ColumnMetadata("cr_order_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("cr_return_quantity", IntegerType),
        ColumnMetadata("cr_return_amount", DecimalType),
        ColumnMetadata("cr_return_tax", DecimalType),
        ColumnMetadata("cr_return_amt_inc_tax", DecimalType),
        ColumnMetadata("cr_fee", DecimalType),
        ColumnMetadata("cr_return_ship_cost", DecimalType),
        ColumnMetadata("cr_refunded_cash", DecimalType),
        ColumnMetadata("cr_reversed_charge", DecimalType),
        ColumnMetadata("cr_store_credit", DecimalType),
        ColumnMetadata("cr_net_loss", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 4) CATALOG_SALES
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "catalog_sales",
      _columns =  Seq(
        ColumnMetadata("cs_sold_date_sk", IntegerType),
        ColumnMetadata("cs_sold_time_sk", IntegerType),
        ColumnMetadata("cs_ship_date_sk", IntegerType),
        ColumnMetadata("cs_bill_customer_sk", IntegerType),
        ColumnMetadata("cs_bill_cdemo_sk", IntegerType),
        ColumnMetadata("cs_bill_hdemo_sk", IntegerType),
        ColumnMetadata("cs_bill_addr_sk", IntegerType),
        ColumnMetadata("cs_ship_customer_sk", IntegerType),
        ColumnMetadata("cs_ship_cdemo_sk", IntegerType),
        ColumnMetadata("cs_ship_hdemo_sk", IntegerType),
        ColumnMetadata("cs_ship_addr_sk", IntegerType),
        ColumnMetadata("cs_call_center_sk", IntegerType),
        ColumnMetadata("cs_catalog_page_sk", IntegerType),
        ColumnMetadata("cs_ship_mode_sk", IntegerType),
        ColumnMetadata("cs_warehouse_sk", IntegerType),
        ColumnMetadata("cs_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("cs_promo_sk", IntegerType),
        ColumnMetadata("cs_order_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("cs_quantity", IntegerType),
        ColumnMetadata("cs_wholesale_cost", DecimalType),
        ColumnMetadata("cs_list_price", DecimalType),
        ColumnMetadata("cs_sales_price", DecimalType),
        ColumnMetadata("cs_ext_discount_amt", DecimalType),
        ColumnMetadata("cs_ext_sales_price", DecimalType),
        ColumnMetadata("cs_ext_wholesale_cost", DecimalType),
        ColumnMetadata("cs_ext_list_price", DecimalType),
        ColumnMetadata("cs_ext_tax", DecimalType),
        ColumnMetadata("cs_coupon_amt", DecimalType),
        ColumnMetadata("cs_ext_ship_cost", DecimalType),
        ColumnMetadata("cs_net_paid", DecimalType),
        ColumnMetadata("cs_net_paid_inc_tax", DecimalType),
        ColumnMetadata("cs_net_paid_inc_ship", DecimalType),
        ColumnMetadata("cs_net_paid_inc_ship_tax", DecimalType),
        ColumnMetadata("cs_net_profit", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 5) CUSTOMER
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "customer",
      _columns =  Seq(
        ColumnMetadata("c_customer_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("c_customer_id", StringType, isNullable = false),
        ColumnMetadata("c_current_cdemo_sk", IntegerType),
        ColumnMetadata("c_current_hdemo_sk", IntegerType),
        ColumnMetadata("c_current_addr_sk", IntegerType),
        ColumnMetadata("c_first_shipto_date_sk", IntegerType),
        ColumnMetadata("c_first_sales_date_sk", IntegerType),
        ColumnMetadata("c_salutation", StringType),
        ColumnMetadata("c_first_name", StringType),
        ColumnMetadata("c_last_name", StringType),
        ColumnMetadata("c_preferred_cust_flag", StringType),
        ColumnMetadata("c_birth_day", IntegerType),
        ColumnMetadata("c_birth_month", IntegerType),
        ColumnMetadata("c_birth_year", IntegerType),
        ColumnMetadata("c_birth_country", StringType),
        ColumnMetadata("c_login", StringType),
        ColumnMetadata("c_email_address", StringType),
        ColumnMetadata("c_last_review_date_sk", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 6) CUSTOMER_ADDRESS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "customer_address",
      _columns =  Seq(
        ColumnMetadata("ca_address_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("ca_address_id", StringType, isNullable = false),
        ColumnMetadata("ca_street_number", StringType),
        ColumnMetadata("ca_street_name", StringType),
        ColumnMetadata("ca_street_type", StringType),
        ColumnMetadata("ca_suite_number", StringType),
        ColumnMetadata("ca_city", StringType),
        ColumnMetadata("ca_county", StringType),
        ColumnMetadata("ca_state", StringType),
        ColumnMetadata("ca_zip", StringType),
        ColumnMetadata("ca_country", StringType),
        ColumnMetadata("ca_gmt_offset", DecimalType),
        ColumnMetadata("ca_location_type", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 7) CUSTOMER_DEMOGRAPHICS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "customer_demographics",
      _columns =  Seq(
        ColumnMetadata("cd_demo_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("cd_gender", StringType),
        ColumnMetadata("cd_marital_status", StringType),
        ColumnMetadata("cd_education_status", StringType),
        ColumnMetadata("cd_purchase_estimate", IntegerType),
        ColumnMetadata("cd_credit_rating", StringType),
        ColumnMetadata("cd_dep_count", IntegerType),
        ColumnMetadata("cd_dep_employed_count", IntegerType),
        ColumnMetadata("cd_dep_college_count", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 8) DATE_DIM
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "date_dim",
      _columns =  Seq(
        ColumnMetadata("d_date_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("d_date_id", StringType, isNullable = false),
        ColumnMetadata("d_date", DateType),
        ColumnMetadata("d_month_seq", IntegerType),
        ColumnMetadata("d_week_seq", IntegerType),
        ColumnMetadata("d_quarter_seq", IntegerType),
        ColumnMetadata("d_year", IntegerType),
        ColumnMetadata("d_dow", IntegerType),
        ColumnMetadata("d_moy", IntegerType),
        ColumnMetadata("d_dom", IntegerType),
        ColumnMetadata("d_qoy", IntegerType),
        ColumnMetadata("d_fy_year", IntegerType),
        ColumnMetadata("d_fy_quarter_seq", IntegerType),
        ColumnMetadata("d_fy_week_seq", IntegerType),
        ColumnMetadata("d_day_name", StringType),
        ColumnMetadata("d_quarter_name", StringType),
        ColumnMetadata("d_holiday", StringType),
        ColumnMetadata("d_weekend", StringType),
        ColumnMetadata("d_following_holiday", StringType),
        ColumnMetadata("d_first_dom", IntegerType),
        ColumnMetadata("d_last_dom", IntegerType),
        ColumnMetadata("d_same_day_ly", IntegerType),
        ColumnMetadata("d_same_day_lq", IntegerType),
        ColumnMetadata("d_current_day", StringType),
        ColumnMetadata("d_current_week", StringType),
        ColumnMetadata("d_current_month", StringType),
        ColumnMetadata("d_current_quarter", StringType),
        ColumnMetadata("d_current_year", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

//    // ------------------------------------------------------------------------------
//    // 9) DBGEN_VERSION
//    // ------------------------------------------------------------------------------
//    TableMetadata(
//      _identifier =  "dbgen_version",
//      _columns =  Seq(
//        ColumnMetadata("dv_version", StringType),
//        ColumnMetadata("dv_create_date", DateType),
//        ColumnMetadata("dv_create_time", StringType),
//        ColumnMetadata("dv_cmdline_args", StringType)
//      ),
//      _metadata =  Map("source" -> "tpcds")
//    ),

    // ------------------------------------------------------------------------------
    // 10) HOUSEHOLD_DEMOGRAPHICS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "household_demographics",
      _columns =  Seq(
        ColumnMetadata("hd_demo_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("hd_income_band_sk", IntegerType),
        ColumnMetadata("hd_buy_potential", StringType),
        ColumnMetadata("hd_dep_count", IntegerType),
        ColumnMetadata("hd_vehicle_count", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 11) INCOME_BAND
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "income_band",
      _columns =  Seq(
        ColumnMetadata("ib_income_band_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("ib_lower_bound", IntegerType),
        ColumnMetadata("ib_upper_bound", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 12) INVENTORY
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "inventory",
      _columns =  Seq(
        ColumnMetadata("inv_date_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("inv_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("inv_warehouse_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("inv_quantity_on_hand", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 13) ITEM
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "item",
      _columns =  Seq(
        ColumnMetadata("i_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("i_item_id", StringType, isNullable = false),
        ColumnMetadata("i_rec_start_date", DateType),
        ColumnMetadata("i_rec_end_date", DateType),
        ColumnMetadata("i_item_desc", StringType),
        ColumnMetadata("i_current_price", DecimalType),
        ColumnMetadata("i_wholesale_cost", DecimalType),
        ColumnMetadata("i_brand_id", IntegerType),
        ColumnMetadata("i_brand", StringType),
        ColumnMetadata("i_class_id", IntegerType),
        ColumnMetadata("i_class", StringType),
        ColumnMetadata("i_category_id", IntegerType),
        ColumnMetadata("i_category", StringType),
        ColumnMetadata("i_manufact_id", IntegerType),
        ColumnMetadata("i_manufact", StringType),
        ColumnMetadata("i_size", StringType),
        ColumnMetadata("i_formulation", StringType),
        ColumnMetadata("i_color", StringType),
        ColumnMetadata("i_units", StringType),
        ColumnMetadata("i_container", StringType),
        ColumnMetadata("i_manager_id", IntegerType),
        ColumnMetadata("i_product_name", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 14) PROMOTION
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "promotion",
      _columns =  Seq(
        ColumnMetadata("p_promo_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("p_promo_id", StringType, isNullable = false),
        ColumnMetadata("p_start_date_sk", IntegerType),
        ColumnMetadata("p_end_date_sk", IntegerType),
        ColumnMetadata("p_item_sk", IntegerType),
        ColumnMetadata("p_cost", DecimalType),
        ColumnMetadata("p_response_target", IntegerType),
        ColumnMetadata("p_promo_name", StringType),
        ColumnMetadata("p_channel_dmail", StringType),
        ColumnMetadata("p_channel_email", StringType),
        ColumnMetadata("p_channel_catalog", StringType),
        ColumnMetadata("p_channel_tv", StringType),
        ColumnMetadata("p_channel_radio", StringType),
        ColumnMetadata("p_channel_press", StringType),
        ColumnMetadata("p_channel_event", StringType),
        ColumnMetadata("p_channel_demo", StringType),
        ColumnMetadata("p_channel_details", StringType),
        ColumnMetadata("p_purpose", StringType),
        ColumnMetadata("p_discount_active", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 15) REASON
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "reason",
      _columns =  Seq(
        ColumnMetadata("r_reason_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("r_reason_id", StringType, isNullable = false),
        ColumnMetadata("r_reason_desc", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 16) SHIP_MODE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "ship_mode",
      _columns =  Seq(
        ColumnMetadata("sm_ship_mode_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("sm_ship_mode_id", StringType, isNullable = false),
        ColumnMetadata("sm_type", StringType),
        ColumnMetadata("sm_code", StringType),
        ColumnMetadata("sm_carrier", StringType),
        ColumnMetadata("sm_contract", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 17) STORE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "store",
      _columns =  Seq(
        ColumnMetadata("s_store_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("s_store_id", StringType, isNullable = false),
        ColumnMetadata("s_rec_start_date", DateType),
        ColumnMetadata("s_rec_end_date", DateType),
        ColumnMetadata("s_closed_date_sk", IntegerType),
        ColumnMetadata("s_store_name", StringType),
        ColumnMetadata("s_number_employees", IntegerType),
        ColumnMetadata("s_floor_space", IntegerType),
        ColumnMetadata("s_hours", StringType),
        ColumnMetadata("s_manager", StringType),
        ColumnMetadata("s_market_id", IntegerType),
        ColumnMetadata("s_geography_class", StringType),
        ColumnMetadata("s_market_desc", StringType),
        ColumnMetadata("s_market_manager", StringType),
        ColumnMetadata("s_division_id", IntegerType),
        ColumnMetadata("s_division_name", StringType),
        ColumnMetadata("s_company_id", IntegerType),
        ColumnMetadata("s_company_name", StringType),
        ColumnMetadata("s_street_number", StringType),
        ColumnMetadata("s_street_name", StringType),
        ColumnMetadata("s_street_type", StringType),
        ColumnMetadata("s_suite_number", StringType),
        ColumnMetadata("s_city", StringType),
        ColumnMetadata("s_county", StringType),
        ColumnMetadata("s_state", StringType),
        ColumnMetadata("s_zip", StringType),
        ColumnMetadata("s_country", StringType),
        ColumnMetadata("s_gmt_offset", DecimalType),
        ColumnMetadata("s_tax_precentage", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 18) STORE_RETURNS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "store_returns",
      _columns =  Seq(
        ColumnMetadata("sr_returned_date_sk", IntegerType),
        ColumnMetadata("sr_return_time_sk", IntegerType),
        ColumnMetadata("sr_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("sr_customer_sk", IntegerType),
        ColumnMetadata("sr_cdemo_sk", IntegerType),
        ColumnMetadata("sr_hdemo_sk", IntegerType),
        ColumnMetadata("sr_addr_sk", IntegerType),
        ColumnMetadata("sr_store_sk", IntegerType),
        ColumnMetadata("sr_reason_sk", IntegerType),
        ColumnMetadata("sr_ticket_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("sr_return_quantity", IntegerType),
        ColumnMetadata("sr_return_amt", DecimalType),
        ColumnMetadata("sr_return_tax", DecimalType),
        ColumnMetadata("sr_return_amt_inc_tax", DecimalType),
        ColumnMetadata("sr_fee", DecimalType),
        ColumnMetadata("sr_return_ship_cost", DecimalType),
        ColumnMetadata("sr_refunded_cash", DecimalType),
        ColumnMetadata("sr_reversed_charge", DecimalType),
        ColumnMetadata("sr_store_credit", DecimalType),
        ColumnMetadata("sr_net_loss", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 19) STORE_SALES
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "store_sales",
      _columns =  Seq(
        ColumnMetadata("ss_sold_date_sk", IntegerType),
        ColumnMetadata("ss_sold_time_sk", IntegerType),
        ColumnMetadata("ss_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("ss_customer_sk", IntegerType),
        ColumnMetadata("ss_cdemo_sk", IntegerType),
        ColumnMetadata("ss_hdemo_sk", IntegerType),
        ColumnMetadata("ss_addr_sk", IntegerType),
        ColumnMetadata("ss_store_sk", IntegerType),
        ColumnMetadata("ss_promo_sk", IntegerType),
        ColumnMetadata("ss_ticket_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("ss_quantity", IntegerType),
        ColumnMetadata("ss_wholesale_cost", DecimalType),
        ColumnMetadata("ss_list_price", DecimalType),
        ColumnMetadata("ss_sales_price", DecimalType),
        ColumnMetadata("ss_ext_discount_amt", DecimalType),
        ColumnMetadata("ss_ext_sales_price", DecimalType),
        ColumnMetadata("ss_ext_wholesale_cost", DecimalType),
        ColumnMetadata("ss_ext_list_price", DecimalType),
        ColumnMetadata("ss_ext_tax", DecimalType),
        ColumnMetadata("ss_coupon_amt", DecimalType),
        ColumnMetadata("ss_net_paid", DecimalType),
        ColumnMetadata("ss_net_paid_inc_tax", DecimalType),
        ColumnMetadata("ss_net_profit", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 20) TIME_DIM
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "time_dim",
      _columns =  Seq(
        ColumnMetadata("t_time_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("t_time_id", StringType, isNullable = false),
        ColumnMetadata("t_interval", IntegerType),
        ColumnMetadata("t_hour", IntegerType),
        ColumnMetadata("t_minute", IntegerType),
        ColumnMetadata("t_second", IntegerType),
        ColumnMetadata("t_am_pm", StringType),
        ColumnMetadata("t_shift", StringType),
        ColumnMetadata("t_sub_shift", StringType),
        ColumnMetadata("t_meal_time", StringType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 21) WAREHOUSE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "warehouse",
      _columns =  Seq(
        ColumnMetadata("w_warehouse_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("w_warehouse_id", StringType, isNullable = false),
        ColumnMetadata("w_warehouse_name", StringType),
        ColumnMetadata("w_warehouse_sq_ft", IntegerType),
        ColumnMetadata("w_street_number", StringType),
        ColumnMetadata("w_street_name", StringType),
        ColumnMetadata("w_street_type", StringType),
        ColumnMetadata("w_suite_number", StringType),
        ColumnMetadata("w_city", StringType),
        ColumnMetadata("w_county", StringType),
        ColumnMetadata("w_state", StringType),
        ColumnMetadata("w_zip", StringType),
        ColumnMetadata("w_country", StringType),
        ColumnMetadata("w_gmt_offset", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 22) WEB_PAGE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "web_page",
      _columns =  Seq(
        ColumnMetadata("wp_web_page_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("wp_web_page_id", StringType, isNullable = false),
        ColumnMetadata("wp_rec_start_date", DateType),
        ColumnMetadata("wp_rec_end_date", DateType),
        ColumnMetadata("wp_creation_date_sk", IntegerType),
        ColumnMetadata("wp_access_date_sk", IntegerType),
        ColumnMetadata("wp_autogen_flag", StringType),
        ColumnMetadata("wp_customer_sk", IntegerType),
        ColumnMetadata("wp_url", StringType),
        ColumnMetadata("wp_type", StringType),
        ColumnMetadata("wp_char_count", IntegerType),
        ColumnMetadata("wp_link_count", IntegerType),
        ColumnMetadata("wp_image_count", IntegerType),
        ColumnMetadata("wp_max_ad_count", IntegerType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 23) WEB_RETURNS
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "web_returns",
      _columns =  Seq(
        ColumnMetadata("wr_returned_date_sk", IntegerType),
        ColumnMetadata("wr_returned_time_sk", IntegerType),
        ColumnMetadata("wr_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("wr_refunded_customer_sk", IntegerType),
        ColumnMetadata("wr_refunded_cdemo_sk", IntegerType),
        ColumnMetadata("wr_refunded_hdemo_sk", IntegerType),
        ColumnMetadata("wr_refunded_addr_sk", IntegerType),
        ColumnMetadata("wr_returning_customer_sk", IntegerType),
        ColumnMetadata("wr_returning_cdemo_sk", IntegerType),
        ColumnMetadata("wr_returning_hdemo_sk", IntegerType),
        ColumnMetadata("wr_returning_addr_sk", IntegerType),
        ColumnMetadata("wr_web_page_sk", IntegerType),
        ColumnMetadata("wr_reason_sk", IntegerType),
        ColumnMetadata("wr_order_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("wr_return_quantity", IntegerType),
        ColumnMetadata("wr_return_amount", DecimalType),
        ColumnMetadata("wr_return_tax", DecimalType),
        ColumnMetadata("wr_return_amt_inc_tax", DecimalType),
        ColumnMetadata("wr_fee", DecimalType),
        ColumnMetadata("wr_return_ship_cost", DecimalType),
        ColumnMetadata("wr_refunded_cash", DecimalType),
        ColumnMetadata("wr_reversed_charge", DecimalType),
        ColumnMetadata("wr_account_credit", DecimalType),
        ColumnMetadata("wr_net_loss", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 24) WEB_SALES
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "web_sales",
      _columns =  Seq(
        ColumnMetadata("ws_sold_date_sk", IntegerType),
        ColumnMetadata("ws_sold_time_sk", IntegerType),
        ColumnMetadata("ws_ship_date_sk", IntegerType),
        ColumnMetadata("ws_item_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("ws_bill_customer_sk", IntegerType),
        ColumnMetadata("ws_bill_cdemo_sk", IntegerType),
        ColumnMetadata("ws_bill_hdemo_sk", IntegerType),
        ColumnMetadata("ws_bill_addr_sk", IntegerType),
        ColumnMetadata("ws_ship_customer_sk", IntegerType),
        ColumnMetadata("ws_ship_cdemo_sk", IntegerType),
        ColumnMetadata("ws_ship_hdemo_sk", IntegerType),
        ColumnMetadata("ws_ship_addr_sk", IntegerType),
        ColumnMetadata("ws_web_page_sk", IntegerType),
        ColumnMetadata("ws_web_site_sk", IntegerType),
        ColumnMetadata("ws_ship_mode_sk", IntegerType),
        ColumnMetadata("ws_warehouse_sk", IntegerType),
        ColumnMetadata("ws_promo_sk", IntegerType),
        ColumnMetadata("ws_order_number", LongType, isNullable = false, isKey = true),
        ColumnMetadata("ws_quantity", IntegerType),
        ColumnMetadata("ws_wholesale_cost", DecimalType),
        ColumnMetadata("ws_list_price", DecimalType),
        ColumnMetadata("ws_sales_price", DecimalType),
        ColumnMetadata("ws_ext_discount_amt", DecimalType),
        ColumnMetadata("ws_ext_sales_price", DecimalType),
        ColumnMetadata("ws_ext_wholesale_cost", DecimalType),
        ColumnMetadata("ws_ext_list_price", DecimalType),
        ColumnMetadata("ws_ext_tax", DecimalType),
        ColumnMetadata("ws_coupon_amt", DecimalType),
        ColumnMetadata("ws_ext_ship_cost", DecimalType),
        ColumnMetadata("ws_net_paid", DecimalType),
        ColumnMetadata("ws_net_paid_inc_tax", DecimalType),
        ColumnMetadata("ws_net_paid_inc_ship", DecimalType),
        ColumnMetadata("ws_net_paid_inc_ship_tax", DecimalType),
        ColumnMetadata("ws_net_profit", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    ),

    // ------------------------------------------------------------------------------
    // 25) WEB_SITE
    // ------------------------------------------------------------------------------
    TableMetadata(
      _identifier =  "web_site",
      _columns =  Seq(
        ColumnMetadata("web_site_sk", IntegerType, isNullable = false, isKey = true),
        ColumnMetadata("web_site_id", StringType, isNullable = false),
        ColumnMetadata("web_rec_start_date", DateType),
        ColumnMetadata("web_rec_end_date", DateType),
        ColumnMetadata("web_name", StringType),
        ColumnMetadata("web_open_date_sk", IntegerType),
        ColumnMetadata("web_close_date_sk", IntegerType),
        ColumnMetadata("web_class", StringType),
        ColumnMetadata("web_manager", StringType),
        ColumnMetadata("web_mkt_id", IntegerType),
        ColumnMetadata("web_mkt_class", StringType),
        ColumnMetadata("web_mkt_desc", StringType),
        ColumnMetadata("web_market_manager", StringType),
        ColumnMetadata("web_company_id", IntegerType),
        ColumnMetadata("web_company_name", StringType),
        ColumnMetadata("web_street_number", StringType),
        ColumnMetadata("web_street_name", StringType),
        ColumnMetadata("web_street_type", StringType),
        ColumnMetadata("web_suite_number", StringType),
        ColumnMetadata("web_city", StringType),
        ColumnMetadata("web_county", StringType),
        ColumnMetadata("web_state", StringType),
        ColumnMetadata("web_zip", StringType),
        ColumnMetadata("web_country", StringType),
        ColumnMetadata("web_gmt_offset", DecimalType),
        ColumnMetadata("web_tax_percentage", DecimalType)
      ),
      _metadata =  Map("source" -> "tpcds")
    )

  )
}
