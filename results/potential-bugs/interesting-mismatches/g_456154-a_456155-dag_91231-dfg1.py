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
    return values.quantile(0.75)


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_12 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_10 = autonode_12.join(autonode_13, col('cr_refunded_addr_sk_node_12') == col('wp_access_date_sk_node_13'))
autonode_11 = autonode_14.order_by(col('hd_vehicle_count_node_14'))
autonode_8 = autonode_10.add_columns(lit("hello"))
autonode_9 = autonode_11.limit(78)
autonode_6 = autonode_8.order_by(col('wp_customer_sk_node_13'))
autonode_7 = autonode_9.alias('dh3Ph')
autonode_4 = autonode_6.filter(preloaded_udf_boolean(col('cr_refunded_hdemo_sk_node_12')))
autonode_5 = autonode_7.alias('XxORU')
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.add_columns(lit("hello"))
autonode_1 = autonode_2.join(autonode_3, col('hd_buy_potential_node_14') == col('wp_rec_start_date_node_13'))
sink = autonode_1.group_by(col('wp_type_node_13')).select(col('hd_buy_potential_node_14').max.alias('hd_buy_potential_node_14'))
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
LogicalProject(hd_buy_potential_node_14=[$1])
+- LogicalAggregate(group=[{36}], EXPR$0=[MAX($45)])
   +- LogicalJoin(condition=[=($45, $29)], joinType=[inner])
      :- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26], wp_web_page_sk_node_13=[$27], wp_web_page_id_node_13=[$28], wp_rec_start_date_node_13=[$29], wp_rec_end_date_node_13=[$30], wp_creation_date_sk_node_13=[$31], wp_access_date_sk_node_13=[$32], wp_autogen_flag_node_13=[$33], wp_customer_sk_node_13=[$34], wp_url_node_13=[$35], wp_type_node_13=[$36], wp_char_count_node_13=[$37], wp_link_count_node_13=[$38], wp_image_count_node_13=[$39], wp_max_ad_count_node_13=[$40], _c41=[$41], _c42=[_UTF-16LE'hello'])
      :  +- LogicalFilter(condition=[*org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*($5)])
      :     +- LogicalSort(sort0=[$34], dir0=[ASC])
      :        +- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26], wp_web_page_sk_node_13=[$27], wp_web_page_id_node_13=[$28], wp_rec_start_date_node_13=[$29], wp_rec_end_date_node_13=[$30], wp_creation_date_sk_node_13=[$31], wp_access_date_sk_node_13=[$32], wp_autogen_flag_node_13=[$33], wp_customer_sk_node_13=[$34], wp_url_node_13=[$35], wp_type_node_13=[$36], wp_char_count_node_13=[$37], wp_link_count_node_13=[$38], wp_image_count_node_13=[$39], wp_max_ad_count_node_13=[$40], _c41=[_UTF-16LE'hello'])
      :           +- LogicalJoin(condition=[=($6, $32)], joinType=[inner])
      :              :- LogicalProject(cr_returned_date_sk_node_12=[$0], cr_returned_time_sk_node_12=[$1], cr_item_sk_node_12=[$2], cr_refunded_customer_sk_node_12=[$3], cr_refunded_cdemo_sk_node_12=[$4], cr_refunded_hdemo_sk_node_12=[$5], cr_refunded_addr_sk_node_12=[$6], cr_returning_customer_sk_node_12=[$7], cr_returning_cdemo_sk_node_12=[$8], cr_returning_hdemo_sk_node_12=[$9], cr_returning_addr_sk_node_12=[$10], cr_call_center_sk_node_12=[$11], cr_catalog_page_sk_node_12=[$12], cr_ship_mode_sk_node_12=[$13], cr_warehouse_sk_node_12=[$14], cr_reason_sk_node_12=[$15], cr_order_number_node_12=[$16], cr_return_quantity_node_12=[$17], cr_return_amount_node_12=[$18], cr_return_tax_node_12=[$19], cr_return_amt_inc_tax_node_12=[$20], cr_fee_node_12=[$21], cr_return_ship_cost_node_12=[$22], cr_refunded_cash_node_12=[$23], cr_reversed_charge_node_12=[$24], cr_store_credit_node_12=[$25], cr_net_loss_node_12=[$26])
      :              :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      :              +- LogicalProject(wp_web_page_sk_node_13=[$0], wp_web_page_id_node_13=[$1], wp_rec_start_date_node_13=[$2], wp_rec_end_date_node_13=[$3], wp_creation_date_sk_node_13=[$4], wp_access_date_sk_node_13=[$5], wp_autogen_flag_node_13=[$6], wp_customer_sk_node_13=[$7], wp_url_node_13=[$8], wp_type_node_13=[$9], wp_char_count_node_13=[$10], wp_link_count_node_13=[$11], wp_image_count_node_13=[$12], wp_max_ad_count_node_13=[$13])
      :                 +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
      +- LogicalProject(XxORU=[AS(AS($0, _UTF-16LE'dh3Ph'), _UTF-16LE'XxORU')], hd_income_band_sk_node_14=[$1], hd_buy_potential_node_14=[$2], hd_dep_count_node_14=[$3], hd_vehicle_count_node_14=[$4], _c5=[_UTF-16LE'hello'])
         +- LogicalSort(sort0=[$4], dir0=[ASC], fetch=[78])
            +- LogicalProject(hd_demo_sk_node_14=[$0], hd_income_band_sk_node_14=[$1], hd_buy_potential_node_14=[$2], hd_dep_count_node_14=[$3], hd_vehicle_count_node_14=[$4])
               +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS hd_buy_potential_node_14])
+- SortAggregate(isMerge=[false], groupBy=[wp_type_node_13], select=[wp_type_node_13, MAX(hd_buy_potential_node_14) AS EXPR$0])
   +- Sort(orderBy=[wp_type_node_13 ASC])
      +- Exchange(distribution=[hash[wp_type_node_13]])
         +- HashJoin(joinType=[InnerJoin], where=[=(hd_buy_potential_node_14, wp_rec_start_date_node_13)], select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, wp_web_page_sk_node_13, wp_web_page_id_node_13, wp_rec_start_date_node_13, wp_rec_end_date_node_13, wp_creation_date_sk_node_13, wp_access_date_sk_node_13, wp_autogen_flag_node_13, wp_customer_sk_node_13, wp_url_node_13, wp_type_node_13, wp_char_count_node_13, wp_link_count_node_13, wp_image_count_node_13, wp_max_ad_count_node_13, _c41, _c42, XxORU, hd_income_band_sk_node_14, hd_buy_potential_node_14, hd_dep_count_node_14, hd_vehicle_count_node_14, _c5], isBroadcast=[true], build=[right])
            :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_12, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12, wp_web_page_sk AS wp_web_page_sk_node_13, wp_web_page_id AS wp_web_page_id_node_13, wp_rec_start_date AS wp_rec_start_date_node_13, wp_rec_end_date AS wp_rec_end_date_node_13, wp_creation_date_sk AS wp_creation_date_sk_node_13, wp_access_date_sk AS wp_access_date_sk_node_13, wp_autogen_flag AS wp_autogen_flag_node_13, wp_customer_sk AS wp_customer_sk_node_13, wp_url AS wp_url_node_13, wp_type AS wp_type_node_13, wp_char_count AS wp_char_count_node_13, wp_link_count AS wp_link_count_node_13, wp_image_count AS wp_image_count_node_13, wp_max_ad_count AS wp_max_ad_count_node_13, 'hello' AS _c41, 'hello' AS _c42], where=[f0])
            :  +- PythonCalc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cr_refunded_hdemo_sk) AS f0])
            :     +- Sort(orderBy=[wp_customer_sk ASC])
            :        +- Exchange(distribution=[single])
            :           +- HashJoin(joinType=[InnerJoin], where=[=(cr_refunded_addr_sk, wp_access_date_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])
            :              :- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
            :              +- Exchange(distribution=[broadcast])
            :                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
            +- Exchange(distribution=[broadcast])
               +- Calc(select=[hd_demo_sk AS XxORU, hd_income_band_sk AS hd_income_band_sk_node_14, hd_buy_potential AS hd_buy_potential_node_14, hd_dep_count AS hd_dep_count_node_14, hd_vehicle_count AS hd_vehicle_count_node_14, 'hello' AS _c5])
                  +- SortLimit(orderBy=[hd_vehicle_count ASC], offset=[0], fetch=[78], global=[true])
                     +- Exchange(distribution=[single])
                        +- SortLimit(orderBy=[hd_vehicle_count ASC], offset=[0], fetch=[78], global=[false])
                           +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS hd_buy_potential_node_14])
+- SortAggregate(isMerge=[false], groupBy=[wp_type_node_13], select=[wp_type_node_13, MAX(hd_buy_potential_node_14) AS EXPR$0])
   +- Exchange(distribution=[forward])
      +- Sort(orderBy=[wp_type_node_13 ASC])
         +- Exchange(distribution=[hash[wp_type_node_13]])
            +- HashJoin(joinType=[InnerJoin], where=[(hd_buy_potential_node_14 = wp_rec_start_date_node_13)], select=[cr_returned_date_sk_node_12, cr_returned_time_sk_node_12, cr_item_sk_node_12, cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk_node_12, cr_returning_customer_sk_node_12, cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk_node_12, cr_returning_addr_sk_node_12, cr_call_center_sk_node_12, cr_catalog_page_sk_node_12, cr_ship_mode_sk_node_12, cr_warehouse_sk_node_12, cr_reason_sk_node_12, cr_order_number_node_12, cr_return_quantity_node_12, cr_return_amount_node_12, cr_return_tax_node_12, cr_return_amt_inc_tax_node_12, cr_fee_node_12, cr_return_ship_cost_node_12, cr_refunded_cash_node_12, cr_reversed_charge_node_12, cr_store_credit_node_12, cr_net_loss_node_12, wp_web_page_sk_node_13, wp_web_page_id_node_13, wp_rec_start_date_node_13, wp_rec_end_date_node_13, wp_creation_date_sk_node_13, wp_access_date_sk_node_13, wp_autogen_flag_node_13, wp_customer_sk_node_13, wp_url_node_13, wp_type_node_13, wp_char_count_node_13, wp_link_count_node_13, wp_image_count_node_13, wp_max_ad_count_node_13, _c41, _c42, XxORU, hd_income_band_sk_node_14, hd_buy_potential_node_14, hd_dep_count_node_14, hd_vehicle_count_node_14, _c5], isBroadcast=[true], build=[right])
               :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_12, cr_returned_time_sk AS cr_returned_time_sk_node_12, cr_item_sk AS cr_item_sk_node_12, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_12, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_12, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_12, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_12, cr_returning_customer_sk AS cr_returning_customer_sk_node_12, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_12, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_12, cr_returning_addr_sk AS cr_returning_addr_sk_node_12, cr_call_center_sk AS cr_call_center_sk_node_12, cr_catalog_page_sk AS cr_catalog_page_sk_node_12, cr_ship_mode_sk AS cr_ship_mode_sk_node_12, cr_warehouse_sk AS cr_warehouse_sk_node_12, cr_reason_sk AS cr_reason_sk_node_12, cr_order_number AS cr_order_number_node_12, cr_return_quantity AS cr_return_quantity_node_12, cr_return_amount AS cr_return_amount_node_12, cr_return_tax AS cr_return_tax_node_12, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_12, cr_fee AS cr_fee_node_12, cr_return_ship_cost AS cr_return_ship_cost_node_12, cr_refunded_cash AS cr_refunded_cash_node_12, cr_reversed_charge AS cr_reversed_charge_node_12, cr_store_credit AS cr_store_credit_node_12, cr_net_loss AS cr_net_loss_node_12, wp_web_page_sk AS wp_web_page_sk_node_13, wp_web_page_id AS wp_web_page_id_node_13, wp_rec_start_date AS wp_rec_start_date_node_13, wp_rec_end_date AS wp_rec_end_date_node_13, wp_creation_date_sk AS wp_creation_date_sk_node_13, wp_access_date_sk AS wp_access_date_sk_node_13, wp_autogen_flag AS wp_autogen_flag_node_13, wp_customer_sk AS wp_customer_sk_node_13, wp_url AS wp_url_node_13, wp_type AS wp_type_node_13, wp_char_count AS wp_char_count_node_13, wp_link_count AS wp_link_count_node_13, wp_image_count AS wp_image_count_node_13, wp_max_ad_count AS wp_max_ad_count_node_13, 'hello' AS _c41, 'hello' AS _c42], where=[f0])
               :  +- PythonCalc(select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, *org.apache.flink.table.functions.python.PythonScalarFunction$2719bb62ba73d6c77acabda93ef50d3e*(cr_refunded_hdemo_sk) AS f0])
               :     +- Sort(orderBy=[wp_customer_sk ASC])
               :        +- Exchange(distribution=[single])
               :           +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(cr_refunded_addr_sk = wp_access_date_sk)], select=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])\
+- [#2] Exchange(distribution=[broadcast])\
])
               :              :- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
               :              +- Exchange(distribution=[broadcast])
               :                 +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
               +- Exchange(distribution=[broadcast])
                  +- Calc(select=[hd_demo_sk AS XxORU, hd_income_band_sk AS hd_income_band_sk_node_14, hd_buy_potential AS hd_buy_potential_node_14, hd_dep_count AS hd_dep_count_node_14, hd_vehicle_count AS hd_vehicle_count_node_14, 'hello' AS _c5])
                     +- SortLimit(orderBy=[hd_vehicle_count ASC], offset=[0], fetch=[78], global=[true])
                        +- Exchange(distribution=[single])
                           +- SortLimit(orderBy=[hd_vehicle_count ASC], offset=[0], fetch=[78], global=[false])
                              +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o248363576.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#502496754:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[34](input=RelSubset#502496752,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[34]), rel#502496751:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[34](input=RelSubset#502496750,groupBy=wp_rec_start_date_node_13, wp_type_node_13,select=wp_rec_start_date_node_13, wp_type_node_13)]
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