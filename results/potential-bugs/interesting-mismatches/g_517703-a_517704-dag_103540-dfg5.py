# ======== Program ========
from pyflink.table import *
from pyflink.table.expressions import *
from pyflink.table.udf import udf
from pyflink.table.types import DataTypes

from pyflink.table.udf import AggregateFunction, udaf
from pyflink.table import DataTypes
import pandas as pd

class MyObject:
    def __init__(self, name, value):
        self.name = name
        self.value = value

# UDF that returns the custom object
@udf(result_type=DataTypes.ROW([
    DataTypes.FIELD("name", DataTypes.STRING()),
    DataTypes.FIELD("value", DataTypes.INT())
]))
def preloaded_udf_complex(*input_val):
    obj = MyObject("test", hash(input_val[0]))
    return (obj.name, obj.value)  # Return as tuple

@udf(result_type=DataTypes.BOOLEAN())
def preloaded_udf_boolean(input_val):
    return True


def preloaded_aggregation(values: pd.Series) -> float:
    return values.max()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_17 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_18 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_19 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_21 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_20 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_20") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_22 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_13 = autonode_17.join(autonode_18, col('ws_bill_addr_sk_node_18') == col('hd_dep_count_node_17'))
autonode_14 = autonode_19.filter(preloaded_udf_boolean(col('sr_return_time_sk_node_19')))
autonode_15 = autonode_20.add_columns(lit("hello"))
autonode_16 = autonode_21.join(autonode_22, col('cc_gmt_offset_node_21') == col('web_gmt_offset_node_22'))
autonode_9 = autonode_13.add_columns(lit("hello"))
autonode_10 = autonode_14.alias('bNDeT')
autonode_11 = autonode_15.limit(8)
autonode_12 = autonode_16.select(col('cc_sq_ft_node_21'))
autonode_5 = autonode_9.order_by(col('ws_list_price_node_18'))
autonode_6 = autonode_10.add_columns(lit("hello"))
autonode_7 = autonode_11.alias('GLZpv')
autonode_8 = autonode_12.distinct()
autonode_3 = autonode_5.join(autonode_6, col('hd_income_band_sk_node_17') == col('sr_item_sk_node_19'))
autonode_4 = autonode_7.join(autonode_8, col('i_manufact_id_node_20') == col('cc_sq_ft_node_21'))
autonode_2 = autonode_3.join(autonode_4, col('i_container_node_20') == col('hd_buy_potential_node_17'))
autonode_1 = autonode_2.group_by(col('ws_net_paid_inc_ship_node_18')).select(col('ws_coupon_amt_node_18').min.alias('ws_coupon_amt_node_18'))
sink = autonode_1.select(col('ws_coupon_amt_node_18'))
print(sink.explain())

# ======== Details ========
"""
{
  "is_same": false,
  "result_name": "MismatchException",
  "result_details": {
    "opt": {
      "success": true,
      "error_name": "",
      "error_message": "",
      "stdout": "== Abstract Syntax Tree ==
LogicalProject(ws_coupon_amt_node_18=[$1])
+- LogicalAggregate(group=[{36}], EXPR$0=[MIN($32)])
   +- LogicalJoin(condition=[=($80, $2)], joinType=[inner])
      :- LogicalJoin(condition=[=($1, $42)], joinType=[inner])
      :  :- LogicalSort(sort0=[$25], dir0=[ASC])
      :  :  +- LogicalProject(hd_demo_sk_node_17=[$0], hd_income_band_sk_node_17=[$1], hd_buy_potential_node_17=[$2], hd_dep_count_node_17=[$3], hd_vehicle_count_node_17=[$4], ws_sold_date_sk_node_18=[$5], ws_sold_time_sk_node_18=[$6], ws_ship_date_sk_node_18=[$7], ws_item_sk_node_18=[$8], ws_bill_customer_sk_node_18=[$9], ws_bill_cdemo_sk_node_18=[$10], ws_bill_hdemo_sk_node_18=[$11], ws_bill_addr_sk_node_18=[$12], ws_ship_customer_sk_node_18=[$13], ws_ship_cdemo_sk_node_18=[$14], ws_ship_hdemo_sk_node_18=[$15], ws_ship_addr_sk_node_18=[$16], ws_web_page_sk_node_18=[$17], ws_web_site_sk_node_18=[$18], ws_ship_mode_sk_node_18=[$19], ws_warehouse_sk_node_18=[$20], ws_promo_sk_node_18=[$21], ws_order_number_node_18=[$22], ws_quantity_node_18=[$23], ws_wholesale_cost_node_18=[$24], ws_list_price_node_18=[$25], ws_sales_price_node_18=[$26], ws_ext_discount_amt_node_18=[$27], ws_ext_sales_price_node_18=[$28], ws_ext_wholesale_cost_node_18=[$29], ws_ext_list_price_node_18=[$30], ws_ext_tax_node_18=[$31], ws_coupon_amt_node_18=[$32], ws_ext_ship_cost_node_18=[$33], ws_net_paid_node_18=[$34], ws_net_paid_inc_tax_node_18=[$35], ws_net_paid_inc_ship_node_18=[$36], ws_net_paid_inc_ship_tax_node_18=[$37], ws_net_profit_node_18=[$38], _c39=[_UTF-16LE'hello'])
      :  :     +- LogicalJoin(condition=[=($12, $3)], joinType=[inner])
      :  :        :- LogicalProject(hd_demo_sk_node_17=[$0], hd_income_band_sk_node_17=[$1], hd_buy_potential_node_17=[$2], hd_dep_count_node_17=[$3], hd_vehicle_count_node_17=[$4])
      :  :        :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
      :  :        +- LogicalProject(ws_sold_date_sk_node_18=[$0], ws_sold_time_sk_node_18=[$1], ws_ship_date_sk_node_18=[$2], ws_item_sk_node_18=[$3], ws_bill_customer_sk_node_18=[$4], ws_bill_cdemo_sk_node_18=[$5], ws_bill_hdemo_sk_node_18=[$6], ws_bill_addr_sk_node_18=[$7], ws_ship_customer_sk_node_18=[$8], ws_ship_cdemo_sk_node_18=[$9], ws_ship_hdemo_sk_node_18=[$10], ws_ship_addr_sk_node_18=[$11], ws_web_page_sk_node_18=[$12], ws_web_site_sk_node_18=[$13], ws_ship_mode_sk_node_18=[$14], ws_warehouse_sk_node_18=[$15], ws_promo_sk_node_18=[$16], ws_order_number_node_18=[$17], ws_quantity_node_18=[$18], ws_wholesale_cost_node_18=[$19], ws_list_price_node_18=[$20], ws_sales_price_node_18=[$21], ws_ext_discount_amt_node_18=[$22], ws_ext_sales_price_node_18=[$23], ws_ext_wholesale_cost_node_18=[$24], ws_ext_list_price_node_18=[$25], ws_ext_tax_node_18=[$26], ws_coupon_amt_node_18=[$27], ws_ext_ship_cost_node_18=[$28], ws_net_paid_node_18=[$29], ws_net_paid_inc_tax_node_18=[$30], ws_net_paid_inc_ship_node_18=[$31], ws_net_paid_inc_ship_tax_node_18=[$32], ws_net_profit_node_18=[$33])
      :  :           +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
      :  +- LogicalProject(bNDeT=[AS($0, _UTF-16LE'bNDeT')], sr_return_time_sk_node_19=[$1], sr_item_sk_node_19=[$2], sr_customer_sk_node_19=[$3], sr_cdemo_sk_node_19=[$4], sr_hdemo_sk_node_19=[$5], sr_addr_sk_node_19=[$6], sr_store_sk_node_19=[$7], sr_reason_sk_node_19=[$8], sr_ticket_number_node_19=[$9], sr_return_quantity_node_19=[$10], sr_return_amt_node_19=[$11], sr_return_tax_node_19=[$12], sr_return_amt_inc_tax_node_19=[$13], sr_fee_node_19=[$14], sr_return_ship_cost_node_19=[$15], sr_refunded_cash_node_19=[$16], sr_reversed_charge_node_19=[$17], sr_store_credit_node_19=[$18], sr_net_loss_node_19=[$19], _c20=[_UTF-16LE'hello'])
      :     +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($1)])
      :        +- LogicalProject(sr_returned_date_sk_node_19=[$0], sr_return_time_sk_node_19=[$1], sr_item_sk_node_19=[$2], sr_customer_sk_node_19=[$3], sr_cdemo_sk_node_19=[$4], sr_hdemo_sk_node_19=[$5], sr_addr_sk_node_19=[$6], sr_store_sk_node_19=[$7], sr_reason_sk_node_19=[$8], sr_ticket_number_node_19=[$9], sr_return_quantity_node_19=[$10], sr_return_amt_node_19=[$11], sr_return_tax_node_19=[$12], sr_return_amt_inc_tax_node_19=[$13], sr_fee_node_19=[$14], sr_return_ship_cost_node_19=[$15], sr_refunded_cash_node_19=[$16], sr_reversed_charge_node_19=[$17], sr_store_credit_node_19=[$18], sr_net_loss_node_19=[$19])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])
      +- LogicalJoin(condition=[=($13, $23)], joinType=[inner])
         :- LogicalProject(GLZpv=[AS($0, _UTF-16LE'GLZpv')], i_item_id_node_20=[$1], i_rec_start_date_node_20=[$2], i_rec_end_date_node_20=[$3], i_item_desc_node_20=[$4], i_current_price_node_20=[$5], i_wholesale_cost_node_20=[$6], i_brand_id_node_20=[$7], i_brand_node_20=[$8], i_class_id_node_20=[$9], i_class_node_20=[$10], i_category_id_node_20=[$11], i_category_node_20=[$12], i_manufact_id_node_20=[$13], i_manufact_node_20=[$14], i_size_node_20=[$15], i_formulation_node_20=[$16], i_color_node_20=[$17], i_units_node_20=[$18], i_container_node_20=[$19], i_manager_id_node_20=[$20], i_product_name_node_20=[$21], _c22=[$22])
         :  +- LogicalSort(fetch=[8])
         :     +- LogicalProject(i_item_sk_node_20=[$0], i_item_id_node_20=[$1], i_rec_start_date_node_20=[$2], i_rec_end_date_node_20=[$3], i_item_desc_node_20=[$4], i_current_price_node_20=[$5], i_wholesale_cost_node_20=[$6], i_brand_id_node_20=[$7], i_brand_node_20=[$8], i_class_id_node_20=[$9], i_class_node_20=[$10], i_category_id_node_20=[$11], i_category_node_20=[$12], i_manufact_id_node_20=[$13], i_manufact_node_20=[$14], i_size_node_20=[$15], i_formulation_node_20=[$16], i_color_node_20=[$17], i_units_node_20=[$18], i_container_node_20=[$19], i_manager_id_node_20=[$20], i_product_name_node_20=[$21], _c22=[_UTF-16LE'hello'])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, item]])
         +- LogicalAggregate(group=[{0}])
            +- LogicalProject(cc_sq_ft_node_21=[$9])
               +- LogicalJoin(condition=[=($29, $55)], joinType=[inner])
                  :- LogicalProject(cc_call_center_sk_node_21=[$0], cc_call_center_id_node_21=[$1], cc_rec_start_date_node_21=[$2], cc_rec_end_date_node_21=[$3], cc_closed_date_sk_node_21=[$4], cc_open_date_sk_node_21=[$5], cc_name_node_21=[$6], cc_class_node_21=[$7], cc_employees_node_21=[$8], cc_sq_ft_node_21=[$9], cc_hours_node_21=[$10], cc_manager_node_21=[$11], cc_mkt_id_node_21=[$12], cc_mkt_class_node_21=[$13], cc_mkt_desc_node_21=[$14], cc_market_manager_node_21=[$15], cc_division_node_21=[$16], cc_division_name_node_21=[$17], cc_company_node_21=[$18], cc_company_name_node_21=[$19], cc_street_number_node_21=[$20], cc_street_name_node_21=[$21], cc_street_type_node_21=[$22], cc_suite_number_node_21=[$23], cc_city_node_21=[$24], cc_county_node_21=[$25], cc_state_node_21=[$26], cc_zip_node_21=[$27], cc_country_node_21=[$28], cc_gmt_offset_node_21=[$29], cc_tax_percentage_node_21=[$30])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
                  +- LogicalProject(web_site_sk_node_22=[$0], web_site_id_node_22=[$1], web_rec_start_date_node_22=[$2], web_rec_end_date_node_22=[$3], web_name_node_22=[$4], web_open_date_sk_node_22=[$5], web_close_date_sk_node_22=[$6], web_class_node_22=[$7], web_manager_node_22=[$8], web_mkt_id_node_22=[$9], web_mkt_class_node_22=[$10], web_mkt_desc_node_22=[$11], web_market_manager_node_22=[$12], web_company_id_node_22=[$13], web_company_name_node_22=[$14], web_street_number_node_22=[$15], web_street_name_node_22=[$16], web_street_type_node_22=[$17], web_suite_number_node_22=[$18], web_city_node_22=[$19], web_county_node_22=[$20], web_state_node_22=[$21], web_zip_node_22=[$22], web_country_node_22=[$23], web_gmt_offset_node_22=[$24], web_tax_percentage_node_22=[$25])
                     +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS ws_coupon_amt_node_18])
+- HashAggregate(isMerge=[false], groupBy=[ws_net_paid_inc_ship_node_18], select=[ws_net_paid_inc_ship_node_18, MIN(ws_coupon_amt_node_18) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_net_paid_inc_ship_node_18]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_container_node_20, hd_buy_potential_node_17)], select=[hd_demo_sk_node_17, hd_income_band_sk_node_17, hd_buy_potential_node_17, hd_dep_count_node_17, hd_vehicle_count_node_17, ws_sold_date_sk_node_18, ws_sold_time_sk_node_18, ws_ship_date_sk_node_18, ws_item_sk_node_18, ws_bill_customer_sk_node_18, ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk_node_18, ws_bill_addr_sk_node_18, ws_ship_customer_sk_node_18, ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk_node_18, ws_ship_addr_sk_node_18, ws_web_page_sk_node_18, ws_web_site_sk_node_18, ws_ship_mode_sk_node_18, ws_warehouse_sk_node_18, ws_promo_sk_node_18, ws_order_number_node_18, ws_quantity_node_18, ws_wholesale_cost_node_18, ws_list_price_node_18, ws_sales_price_node_18, ws_ext_discount_amt_node_18, ws_ext_sales_price_node_18, ws_ext_wholesale_cost_node_18, ws_ext_list_price_node_18, ws_ext_tax_node_18, ws_coupon_amt_node_18, ws_ext_ship_cost_node_18, ws_net_paid_node_18, ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax_node_18, ws_net_profit_node_18, _c39, bNDeT, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19, _c20, GLZpv, i_item_id_node_20, i_rec_start_date_node_20, i_rec_end_date_node_20, i_item_desc_node_20, i_current_price_node_20, i_wholesale_cost_node_20, i_brand_id_node_20, i_brand_node_20, i_class_id_node_20, i_class_node_20, i_category_id_node_20, i_category_node_20, i_manufact_id_node_20, i_manufact_node_20, i_size_node_20, i_formulation_node_20, i_color_node_20, i_units_node_20, i_container_node_20, i_manager_id_node_20, i_product_name_node_20, _c22, cc_sq_ft_node_21], build=[right])
         :- NestedLoopJoin(joinType=[InnerJoin], where=[=(hd_income_band_sk_node_17, sr_item_sk_node_19)], select=[hd_demo_sk_node_17, hd_income_band_sk_node_17, hd_buy_potential_node_17, hd_dep_count_node_17, hd_vehicle_count_node_17, ws_sold_date_sk_node_18, ws_sold_time_sk_node_18, ws_ship_date_sk_node_18, ws_item_sk_node_18, ws_bill_customer_sk_node_18, ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk_node_18, ws_bill_addr_sk_node_18, ws_ship_customer_sk_node_18, ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk_node_18, ws_ship_addr_sk_node_18, ws_web_page_sk_node_18, ws_web_site_sk_node_18, ws_ship_mode_sk_node_18, ws_warehouse_sk_node_18, ws_promo_sk_node_18, ws_order_number_node_18, ws_quantity_node_18, ws_wholesale_cost_node_18, ws_list_price_node_18, ws_sales_price_node_18, ws_ext_discount_amt_node_18, ws_ext_sales_price_node_18, ws_ext_wholesale_cost_node_18, ws_ext_list_price_node_18, ws_ext_tax_node_18, ws_coupon_amt_node_18, ws_ext_ship_cost_node_18, ws_net_paid_node_18, ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax_node_18, ws_net_profit_node_18, _c39, bNDeT, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19, _c20], build=[right])
         :  :- Sort(orderBy=[ws_list_price_node_18 ASC])
         :  :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_17, hd_income_band_sk AS hd_income_band_sk_node_17, hd_buy_potential AS hd_buy_potential_node_17, hd_dep_count AS hd_dep_count_node_17, hd_vehicle_count AS hd_vehicle_count_node_17, ws_sold_date_sk AS ws_sold_date_sk_node_18, ws_sold_time_sk AS ws_sold_time_sk_node_18, ws_ship_date_sk AS ws_ship_date_sk_node_18, ws_item_sk AS ws_item_sk_node_18, ws_bill_customer_sk AS ws_bill_customer_sk_node_18, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_18, ws_bill_addr_sk AS ws_bill_addr_sk_node_18, ws_ship_customer_sk AS ws_ship_customer_sk_node_18, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_18, ws_ship_addr_sk AS ws_ship_addr_sk_node_18, ws_web_page_sk AS ws_web_page_sk_node_18, ws_web_site_sk AS ws_web_site_sk_node_18, ws_ship_mode_sk AS ws_ship_mode_sk_node_18, ws_warehouse_sk AS ws_warehouse_sk_node_18, ws_promo_sk AS ws_promo_sk_node_18, ws_order_number AS ws_order_number_node_18, ws_quantity AS ws_quantity_node_18, ws_wholesale_cost AS ws_wholesale_cost_node_18, ws_list_price AS ws_list_price_node_18, ws_sales_price AS ws_sales_price_node_18, ws_ext_discount_amt AS ws_ext_discount_amt_node_18, ws_ext_sales_price AS ws_ext_sales_price_node_18, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_18, ws_ext_list_price AS ws_ext_list_price_node_18, ws_ext_tax AS ws_ext_tax_node_18, ws_coupon_amt AS ws_coupon_amt_node_18, ws_ext_ship_cost AS ws_ext_ship_cost_node_18, ws_net_paid AS ws_net_paid_node_18, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_18, ws_net_profit AS ws_net_profit_node_18, 'hello' AS _c39])
         :  :     +- Exchange(distribution=[single])
         :  :        +- HashJoin(joinType=[InnerJoin], where=[=(ws_bill_addr_sk, hd_dep_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], isBroadcast=[true], build=[left])
         :  :           :- Exchange(distribution=[broadcast])
         :  :           :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
         :  :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
         :  +- Exchange(distribution=[broadcast])
         :     +- Calc(select=[sr_returned_date_sk AS bNDeT, sr_return_time_sk AS sr_return_time_sk_node_19, sr_item_sk AS sr_item_sk_node_19, sr_customer_sk AS sr_customer_sk_node_19, sr_cdemo_sk AS sr_cdemo_sk_node_19, sr_hdemo_sk AS sr_hdemo_sk_node_19, sr_addr_sk AS sr_addr_sk_node_19, sr_store_sk AS sr_store_sk_node_19, sr_reason_sk AS sr_reason_sk_node_19, sr_ticket_number AS sr_ticket_number_node_19, sr_return_quantity AS sr_return_quantity_node_19, sr_return_amt AS sr_return_amt_node_19, sr_return_tax AS sr_return_tax_node_19, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_19, sr_fee AS sr_fee_node_19, sr_return_ship_cost AS sr_return_ship_cost_node_19, sr_refunded_cash AS sr_refunded_cash_node_19, sr_reversed_charge AS sr_reversed_charge_node_19, sr_store_credit AS sr_store_credit_node_19, sr_net_loss AS sr_net_loss_node_19, 'hello' AS _c20], where=[f0])
         :        +- PythonCalc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(sr_return_time_sk) AS f0])
         :           +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
         +- Exchange(distribution=[broadcast])
            +- NestedLoopJoin(joinType=[InnerJoin], where=[=(i_manufact_id_node_20, cc_sq_ft_node_21)], select=[GLZpv, i_item_id_node_20, i_rec_start_date_node_20, i_rec_end_date_node_20, i_item_desc_node_20, i_current_price_node_20, i_wholesale_cost_node_20, i_brand_id_node_20, i_brand_node_20, i_class_id_node_20, i_class_node_20, i_category_id_node_20, i_category_node_20, i_manufact_id_node_20, i_manufact_node_20, i_size_node_20, i_formulation_node_20, i_color_node_20, i_units_node_20, i_container_node_20, i_manager_id_node_20, i_product_name_node_20, _c22, cc_sq_ft_node_21], build=[right])
               :- Calc(select=[i_item_sk AS GLZpv, i_item_id AS i_item_id_node_20, i_rec_start_date AS i_rec_start_date_node_20, i_rec_end_date AS i_rec_end_date_node_20, i_item_desc AS i_item_desc_node_20, i_current_price AS i_current_price_node_20, i_wholesale_cost AS i_wholesale_cost_node_20, i_brand_id AS i_brand_id_node_20, i_brand AS i_brand_node_20, i_class_id AS i_class_id_node_20, i_class AS i_class_node_20, i_category_id AS i_category_id_node_20, i_category AS i_category_node_20, i_manufact_id AS i_manufact_id_node_20, i_manufact AS i_manufact_node_20, i_size AS i_size_node_20, i_formulation AS i_formulation_node_20, i_color AS i_color_node_20, i_units AS i_units_node_20, i_container AS i_container_node_20, i_manager_id AS i_manager_id_node_20, i_product_name AS i_product_name_node_20, 'hello' AS _c22])
               :  +- Limit(offset=[0], fetch=[8], global=[true])
               :     +- Exchange(distribution=[single])
               :        +- Limit(offset=[0], fetch=[8], global=[false])
               :           +- TableSourceScan(table=[[default_catalog, default_database, item, limit=[8]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
               +- Exchange(distribution=[broadcast])
                  +- SortAggregate(isMerge=[true], groupBy=[cc_sq_ft_node_21], select=[cc_sq_ft_node_21])
                     +- Sort(orderBy=[cc_sq_ft_node_21 ASC])
                        +- Exchange(distribution=[hash[cc_sq_ft_node_21]])
                           +- LocalSortAggregate(groupBy=[cc_sq_ft_node_21], select=[cc_sq_ft_node_21])
                              +- Sort(orderBy=[cc_sq_ft_node_21 ASC])
                                 +- Calc(select=[cc_sq_ft AS cc_sq_ft_node_21])
                                    +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cc_gmt_offset, web_gmt_offset)], select=[cc_sq_ft, cc_gmt_offset, web_gmt_offset], build=[left])
                                       :- Exchange(distribution=[broadcast])
                                       :  +- TableSourceScan(table=[[default_catalog, default_database, call_center, project=[cc_sq_ft, cc_gmt_offset], metadata=[]]], fields=[cc_sq_ft, cc_gmt_offset])
                                       +- TableSourceScan(table=[[default_catalog, default_database, web_site, project=[web_gmt_offset], metadata=[]]], fields=[web_gmt_offset])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS ws_coupon_amt_node_18])
+- HashAggregate(isMerge=[false], groupBy=[ws_net_paid_inc_ship_node_18], select=[ws_net_paid_inc_ship_node_18, MIN(ws_coupon_amt_node_18) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_net_paid_inc_ship_node_18]])
      +- MultipleInput(readOrder=[0,1,0], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(i_container_node_20 = hd_buy_potential_node_17)], select=[hd_demo_sk_node_17, hd_income_band_sk_node_17, hd_buy_potential_node_17, hd_dep_count_node_17, hd_vehicle_count_node_17, ws_sold_date_sk_node_18, ws_sold_time_sk_node_18, ws_ship_date_sk_node_18, ws_item_sk_node_18, ws_bill_customer_sk_node_18, ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk_node_18, ws_bill_addr_sk_node_18, ws_ship_customer_sk_node_18, ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk_node_18, ws_ship_addr_sk_node_18, ws_web_page_sk_node_18, ws_web_site_sk_node_18, ws_ship_mode_sk_node_18, ws_warehouse_sk_node_18, ws_promo_sk_node_18, ws_order_number_node_18, ws_quantity_node_18, ws_wholesale_cost_node_18, ws_list_price_node_18, ws_sales_price_node_18, ws_ext_discount_amt_node_18, ws_ext_sales_price_node_18, ws_ext_wholesale_cost_node_18, ws_ext_list_price_node_18, ws_ext_tax_node_18, ws_coupon_amt_node_18, ws_ext_ship_cost_node_18, ws_net_paid_node_18, ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax_node_18, ws_net_profit_node_18, _c39, bNDeT, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19, _c20, GLZpv, i_item_id_node_20, i_rec_start_date_node_20, i_rec_end_date_node_20, i_item_desc_node_20, i_current_price_node_20, i_wholesale_cost_node_20, i_brand_id_node_20, i_brand_node_20, i_class_id_node_20, i_class_node_20, i_category_id_node_20, i_category_node_20, i_manufact_id_node_20, i_manufact_node_20, i_size_node_20, i_formulation_node_20, i_color_node_20, i_units_node_20, i_container_node_20, i_manager_id_node_20, i_product_name_node_20, _c22, cc_sq_ft_node_21], build=[right])\
:- NestedLoopJoin(joinType=[InnerJoin], where=[(hd_income_band_sk_node_17 = sr_item_sk_node_19)], select=[hd_demo_sk_node_17, hd_income_band_sk_node_17, hd_buy_potential_node_17, hd_dep_count_node_17, hd_vehicle_count_node_17, ws_sold_date_sk_node_18, ws_sold_time_sk_node_18, ws_ship_date_sk_node_18, ws_item_sk_node_18, ws_bill_customer_sk_node_18, ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk_node_18, ws_bill_addr_sk_node_18, ws_ship_customer_sk_node_18, ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk_node_18, ws_ship_addr_sk_node_18, ws_web_page_sk_node_18, ws_web_site_sk_node_18, ws_ship_mode_sk_node_18, ws_warehouse_sk_node_18, ws_promo_sk_node_18, ws_order_number_node_18, ws_quantity_node_18, ws_wholesale_cost_node_18, ws_list_price_node_18, ws_sales_price_node_18, ws_ext_discount_amt_node_18, ws_ext_sales_price_node_18, ws_ext_wholesale_cost_node_18, ws_ext_list_price_node_18, ws_ext_tax_node_18, ws_coupon_amt_node_18, ws_ext_ship_cost_node_18, ws_net_paid_node_18, ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax_node_18, ws_net_profit_node_18, _c39, bNDeT, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19, _c20], build=[right])\
:  :- [#2] Sort(orderBy=[ws_list_price_node_18 ASC])\
:  +- [#3] Exchange(distribution=[broadcast])\
+- [#1] Exchange(distribution=[broadcast])\
])
         :- Exchange(distribution=[broadcast])
         :  +- NestedLoopJoin(joinType=[InnerJoin], where=[(i_manufact_id_node_20 = cc_sq_ft_node_21)], select=[GLZpv, i_item_id_node_20, i_rec_start_date_node_20, i_rec_end_date_node_20, i_item_desc_node_20, i_current_price_node_20, i_wholesale_cost_node_20, i_brand_id_node_20, i_brand_node_20, i_class_id_node_20, i_class_node_20, i_category_id_node_20, i_category_node_20, i_manufact_id_node_20, i_manufact_node_20, i_size_node_20, i_formulation_node_20, i_color_node_20, i_units_node_20, i_container_node_20, i_manager_id_node_20, i_product_name_node_20, _c22, cc_sq_ft_node_21], build=[right])
         :     :- Calc(select=[i_item_sk AS GLZpv, i_item_id AS i_item_id_node_20, i_rec_start_date AS i_rec_start_date_node_20, i_rec_end_date AS i_rec_end_date_node_20, i_item_desc AS i_item_desc_node_20, i_current_price AS i_current_price_node_20, i_wholesale_cost AS i_wholesale_cost_node_20, i_brand_id AS i_brand_id_node_20, i_brand AS i_brand_node_20, i_class_id AS i_class_id_node_20, i_class AS i_class_node_20, i_category_id AS i_category_id_node_20, i_category AS i_category_node_20, i_manufact_id AS i_manufact_id_node_20, i_manufact AS i_manufact_node_20, i_size AS i_size_node_20, i_formulation AS i_formulation_node_20, i_color AS i_color_node_20, i_units AS i_units_node_20, i_container AS i_container_node_20, i_manager_id AS i_manager_id_node_20, i_product_name AS i_product_name_node_20, 'hello' AS _c22])
         :     :  +- Limit(offset=[0], fetch=[8], global=[true])
         :     :     +- Exchange(distribution=[single])
         :     :        +- Limit(offset=[0], fetch=[8], global=[false])
         :     :           +- TableSourceScan(table=[[default_catalog, default_database, item, limit=[8]]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
         :     +- Exchange(distribution=[broadcast])
         :        +- SortAggregate(isMerge=[true], groupBy=[cc_sq_ft_node_21], select=[cc_sq_ft_node_21])
         :           +- Exchange(distribution=[forward])
         :              +- Sort(orderBy=[cc_sq_ft_node_21 ASC])
         :                 +- Exchange(distribution=[hash[cc_sq_ft_node_21]])
         :                    +- LocalSortAggregate(groupBy=[cc_sq_ft_node_21], select=[cc_sq_ft_node_21])
         :                       +- Exchange(distribution=[forward])
         :                          +- Sort(orderBy=[cc_sq_ft_node_21 ASC])
         :                             +- Calc(select=[cc_sq_ft AS cc_sq_ft_node_21])
         :                                +- MultipleInput(readOrder=[0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(cc_gmt_offset = web_gmt_offset)], select=[cc_sq_ft, cc_gmt_offset, web_gmt_offset], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_site, project=[web_gmt_offset], metadata=[]]], fields=[web_gmt_offset])\
])
         :                                   :- Exchange(distribution=[broadcast])
         :                                   :  +- TableSourceScan(table=[[default_catalog, default_database, call_center, project=[cc_sq_ft, cc_gmt_offset], metadata=[]]], fields=[cc_sq_ft, cc_gmt_offset])
         :                                   +- TableSourceScan(table=[[default_catalog, default_database, web_site, project=[web_gmt_offset], metadata=[]]], fields=[web_gmt_offset])
         :- Sort(orderBy=[ws_list_price_node_18 ASC])
         :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_17, hd_income_band_sk AS hd_income_band_sk_node_17, hd_buy_potential AS hd_buy_potential_node_17, hd_dep_count AS hd_dep_count_node_17, hd_vehicle_count AS hd_vehicle_count_node_17, ws_sold_date_sk AS ws_sold_date_sk_node_18, ws_sold_time_sk AS ws_sold_time_sk_node_18, ws_ship_date_sk AS ws_ship_date_sk_node_18, ws_item_sk AS ws_item_sk_node_18, ws_bill_customer_sk AS ws_bill_customer_sk_node_18, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_18, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_18, ws_bill_addr_sk AS ws_bill_addr_sk_node_18, ws_ship_customer_sk AS ws_ship_customer_sk_node_18, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_18, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_18, ws_ship_addr_sk AS ws_ship_addr_sk_node_18, ws_web_page_sk AS ws_web_page_sk_node_18, ws_web_site_sk AS ws_web_site_sk_node_18, ws_ship_mode_sk AS ws_ship_mode_sk_node_18, ws_warehouse_sk AS ws_warehouse_sk_node_18, ws_promo_sk AS ws_promo_sk_node_18, ws_order_number AS ws_order_number_node_18, ws_quantity AS ws_quantity_node_18, ws_wholesale_cost AS ws_wholesale_cost_node_18, ws_list_price AS ws_list_price_node_18, ws_sales_price AS ws_sales_price_node_18, ws_ext_discount_amt AS ws_ext_discount_amt_node_18, ws_ext_sales_price AS ws_ext_sales_price_node_18, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_18, ws_ext_list_price AS ws_ext_list_price_node_18, ws_ext_tax AS ws_ext_tax_node_18, ws_coupon_amt AS ws_coupon_amt_node_18, ws_ext_ship_cost AS ws_ext_ship_cost_node_18, ws_net_paid AS ws_net_paid_node_18, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_18, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_18, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_18, ws_net_profit AS ws_net_profit_node_18, 'hello' AS _c39])
         :     +- Exchange(distribution=[single])
         :        +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ws_bill_addr_sk = hd_dep_count)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])\
])
         :           :- Exchange(distribution=[broadcast])
         :           :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
         :           +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
         +- Exchange(distribution=[broadcast])
            +- Calc(select=[sr_returned_date_sk AS bNDeT, sr_return_time_sk AS sr_return_time_sk_node_19, sr_item_sk AS sr_item_sk_node_19, sr_customer_sk AS sr_customer_sk_node_19, sr_cdemo_sk AS sr_cdemo_sk_node_19, sr_hdemo_sk AS sr_hdemo_sk_node_19, sr_addr_sk AS sr_addr_sk_node_19, sr_store_sk AS sr_store_sk_node_19, sr_reason_sk AS sr_reason_sk_node_19, sr_ticket_number AS sr_ticket_number_node_19, sr_return_quantity AS sr_return_quantity_node_19, sr_return_amt AS sr_return_amt_node_19, sr_return_tax AS sr_return_tax_node_19, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_19, sr_fee AS sr_fee_node_19, sr_return_ship_cost AS sr_return_ship_cost_node_19, sr_refunded_cash AS sr_refunded_cash_node_19, sr_reversed_charge AS sr_reversed_charge_node_19, sr_store_credit AS sr_store_credit_node_19, sr_net_loss AS sr_net_loss_node_19, 'hello' AS _c20], where=[f0])
               +- PythonCalc(select=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(sr_return_time_sk) AS f0])
                  +- TableSourceScan(table=[[default_catalog, default_database, store_returns]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o282013736.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#570293332:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[25](input=RelSubset#570293330,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[25]), rel#570293329:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[25](input=RelSubset#570293328,groupBy=hd_income_band_sk, hd_buy_potential, ws_net_paid_inc_ship,select=hd_income_band_sk, hd_buy_potential, ws_net_paid_inc_ship, Partial_MIN(ws_coupon_amt) AS min$0)]
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:250)
\tat org.apache.calcite.plan.volcano.IterativeRuleDriver.drive(IterativeRuleDriver.java:59)
\tat org.apache.calcite.plan.volcano.VolcanoPlanner.findBestExp(VolcanoPlanner.java:523)
\tat org.apache.calcite.tools.Programs$RuleSetProgram.run(Programs.java:317)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkVolcanoProgram.optimize(FlinkVolcanoProgram.scala:62)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.$anonfun$optimize$1(FlinkChainedProgram.scala:59)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:196)
\tat scala.collection.TraversableOnce$folder$1.apply(TraversableOnce.scala:194)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableOnce.foldLeft(TraversableOnce.scala:199)
\tat scala.collection.TraversableOnce.foldLeft$(TraversableOnce.scala:192)
\tat scala.collection.AbstractTraversable.foldLeft(Traversable.scala:108)
\tat org.apache.flink.table.planner.plan.optimize.program.FlinkChainedProgram.optimize(FlinkChainedProgram.scala:55)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeTree(BatchCommonSubGraphBasedOptimizer.scala:93)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.optimizeBlock(BatchCommonSubGraphBasedOptimizer.scala:58)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.$anonfun$doOptimize$1$adapted(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat scala.collection.immutable.List.foreach(List.scala:431)
\tat org.apache.flink.table.planner.plan.optimize.BatchCommonSubGraphBasedOptimizer.doOptimize(BatchCommonSubGraphBasedOptimizer.scala:45)
\tat org.apache.flink.table.planner.plan.optimize.CommonSubGraphBasedOptimizer.optimize(CommonSubGraphBasedOptimizer.scala:87)
\tat org.apache.flink.table.planner.delegation.PlannerBase.optimize(PlannerBase.scala:390)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:625)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:149)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.explain(BatchPlanner.scala:49)
\tat org.apache.flink.table.api.internal.TableEnvironmentImpl.explainInternal(TableEnvironmentImpl.java:753)
\tat org.apache.flink.table.api.internal.TableImpl.explain(TableImpl.java:482)
\tat jdk.internal.reflect.GeneratedMethodAccessor32.invoke(Unknown Source)
\tat java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
\tat java.base/java.lang.reflect.Method.invoke(Method.java:566)
\tat org.apache.flink.api.python.shaded.py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:244)
\tat org.apache.flink.api.python.shaded.py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:374)
\tat org.apache.flink.api.python.shaded.py4j.Gateway.invoke(Gateway.java:282)
\tat org.apache.flink.api.python.shaded.py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
\tat org.apache.flink.api.python.shaded.py4j.commands.CallCommand.execute(CallCommand.java:79)
\tat org.apache.flink.api.python.shaded.py4j.GatewayConnection.run(GatewayConnection.java:238)
\tat java.base/java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.RuntimeException: Error occurred while applying rule FlinkExpandConversionRule
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.transformTo(VolcanoRuleCall.java:157)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:273)
\tat org.apache.calcite.plan.RelOptRuleCall.transformTo(RelOptRuleCall.java:288)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.satisfyTraitsBySelf(FlinkExpandConversionRule.scala:72)
\tat org.apache.flink.table.planner.plan.rules.physical.FlinkExpandConversionRule.onMatch(FlinkExpandConversionRule.scala:52)
\tat org.apache.calcite.plan.volcano.VolcanoRuleCall.onMatch(VolcanoRuleCall.java:223)
\t... 40 more
Caused by: java.lang.ArrayIndexOutOfBoundsException
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0