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
    return values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_sales").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_sales").get_schema().get_field_names()])
autonode_11 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_7 = autonode_9.join(autonode_10, col('cs_call_center_sk_node_9') == col('sm_ship_mode_sk_node_10'))
autonode_8 = autonode_11.distinct()
autonode_5 = autonode_7.alias('VP8cA')
autonode_6 = autonode_8.order_by(col('ws_bill_customer_sk_node_11'))
autonode_4 = autonode_5.join(autonode_6, col('ws_net_paid_node_11') == col('cs_sales_price_node_9'))
autonode_3 = autonode_4.group_by(col('ws_ship_addr_sk_node_11')).select(col('ws_ship_cdemo_sk_node_11').sum.alias('ws_ship_cdemo_sk_node_11'))
autonode_2 = autonode_3.select(col('ws_ship_cdemo_sk_node_11'))
autonode_1 = autonode_2.add_columns(lit("hello"))
sink = autonode_1.alias('oHErN')
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
LogicalProject(oHErN=[AS($1, _UTF-16LE'oHErN')], _c1=[_UTF-16LE'hello'])
+- LogicalAggregate(group=[{51}], EXPR$0=[SUM($49)])
   +- LogicalJoin(condition=[=($69, $21)], joinType=[inner])
      :- LogicalProject(VP8cA=[AS($0, _UTF-16LE'VP8cA')], cs_sold_time_sk_node_9=[$1], cs_ship_date_sk_node_9=[$2], cs_bill_customer_sk_node_9=[$3], cs_bill_cdemo_sk_node_9=[$4], cs_bill_hdemo_sk_node_9=[$5], cs_bill_addr_sk_node_9=[$6], cs_ship_customer_sk_node_9=[$7], cs_ship_cdemo_sk_node_9=[$8], cs_ship_hdemo_sk_node_9=[$9], cs_ship_addr_sk_node_9=[$10], cs_call_center_sk_node_9=[$11], cs_catalog_page_sk_node_9=[$12], cs_ship_mode_sk_node_9=[$13], cs_warehouse_sk_node_9=[$14], cs_item_sk_node_9=[$15], cs_promo_sk_node_9=[$16], cs_order_number_node_9=[$17], cs_quantity_node_9=[$18], cs_wholesale_cost_node_9=[$19], cs_list_price_node_9=[$20], cs_sales_price_node_9=[$21], cs_ext_discount_amt_node_9=[$22], cs_ext_sales_price_node_9=[$23], cs_ext_wholesale_cost_node_9=[$24], cs_ext_list_price_node_9=[$25], cs_ext_tax_node_9=[$26], cs_coupon_amt_node_9=[$27], cs_ext_ship_cost_node_9=[$28], cs_net_paid_node_9=[$29], cs_net_paid_inc_tax_node_9=[$30], cs_net_paid_inc_ship_node_9=[$31], cs_net_paid_inc_ship_tax_node_9=[$32], cs_net_profit_node_9=[$33], sm_ship_mode_sk_node_10=[$34], sm_ship_mode_id_node_10=[$35], sm_type_node_10=[$36], sm_code_node_10=[$37], sm_carrier_node_10=[$38], sm_contract_node_10=[$39])
      :  +- LogicalJoin(condition=[=($11, $34)], joinType=[inner])
      :     :- LogicalProject(cs_sold_date_sk_node_9=[$0], cs_sold_time_sk_node_9=[$1], cs_ship_date_sk_node_9=[$2], cs_bill_customer_sk_node_9=[$3], cs_bill_cdemo_sk_node_9=[$4], cs_bill_hdemo_sk_node_9=[$5], cs_bill_addr_sk_node_9=[$6], cs_ship_customer_sk_node_9=[$7], cs_ship_cdemo_sk_node_9=[$8], cs_ship_hdemo_sk_node_9=[$9], cs_ship_addr_sk_node_9=[$10], cs_call_center_sk_node_9=[$11], cs_catalog_page_sk_node_9=[$12], cs_ship_mode_sk_node_9=[$13], cs_warehouse_sk_node_9=[$14], cs_item_sk_node_9=[$15], cs_promo_sk_node_9=[$16], cs_order_number_node_9=[$17], cs_quantity_node_9=[$18], cs_wholesale_cost_node_9=[$19], cs_list_price_node_9=[$20], cs_sales_price_node_9=[$21], cs_ext_discount_amt_node_9=[$22], cs_ext_sales_price_node_9=[$23], cs_ext_wholesale_cost_node_9=[$24], cs_ext_list_price_node_9=[$25], cs_ext_tax_node_9=[$26], cs_coupon_amt_node_9=[$27], cs_ext_ship_cost_node_9=[$28], cs_net_paid_node_9=[$29], cs_net_paid_inc_tax_node_9=[$30], cs_net_paid_inc_ship_node_9=[$31], cs_net_paid_inc_ship_tax_node_9=[$32], cs_net_profit_node_9=[$33])
      :     :  +- LogicalTableScan(table=[[default_catalog, default_database, catalog_sales]])
      :     +- LogicalProject(sm_ship_mode_sk_node_10=[$0], sm_ship_mode_id_node_10=[$1], sm_type_node_10=[$2], sm_code_node_10=[$3], sm_carrier_node_10=[$4], sm_contract_node_10=[$5])
      :        +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
      +- LogicalSort(sort0=[$4], dir0=[ASC])
         +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
            +- LogicalProject(ws_sold_date_sk_node_11=[$0], ws_sold_time_sk_node_11=[$1], ws_ship_date_sk_node_11=[$2], ws_item_sk_node_11=[$3], ws_bill_customer_sk_node_11=[$4], ws_bill_cdemo_sk_node_11=[$5], ws_bill_hdemo_sk_node_11=[$6], ws_bill_addr_sk_node_11=[$7], ws_ship_customer_sk_node_11=[$8], ws_ship_cdemo_sk_node_11=[$9], ws_ship_hdemo_sk_node_11=[$10], ws_ship_addr_sk_node_11=[$11], ws_web_page_sk_node_11=[$12], ws_web_site_sk_node_11=[$13], ws_ship_mode_sk_node_11=[$14], ws_warehouse_sk_node_11=[$15], ws_promo_sk_node_11=[$16], ws_order_number_node_11=[$17], ws_quantity_node_11=[$18], ws_wholesale_cost_node_11=[$19], ws_list_price_node_11=[$20], ws_sales_price_node_11=[$21], ws_ext_discount_amt_node_11=[$22], ws_ext_sales_price_node_11=[$23], ws_ext_wholesale_cost_node_11=[$24], ws_ext_list_price_node_11=[$25], ws_ext_tax_node_11=[$26], ws_coupon_amt_node_11=[$27], ws_ext_ship_cost_node_11=[$28], ws_net_paid_node_11=[$29], ws_net_paid_inc_tax_node_11=[$30], ws_net_paid_inc_ship_node_11=[$31], ws_net_paid_inc_ship_tax_node_11=[$32], ws_net_profit_node_11=[$33])
               +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS oHErN, 'hello' AS _c1])
+- HashAggregate(isMerge=[false], groupBy=[ws_ship_addr_sk], select=[ws_ship_addr_sk, SUM(ws_ship_cdemo_sk) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ship_addr_sk]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ws_net_paid, cs_sales_price_node_9)], select=[VP8cA, cs_sold_time_sk_node_9, cs_ship_date_sk_node_9, cs_bill_customer_sk_node_9, cs_bill_cdemo_sk_node_9, cs_bill_hdemo_sk_node_9, cs_bill_addr_sk_node_9, cs_ship_customer_sk_node_9, cs_ship_cdemo_sk_node_9, cs_ship_hdemo_sk_node_9, cs_ship_addr_sk_node_9, cs_call_center_sk_node_9, cs_catalog_page_sk_node_9, cs_ship_mode_sk_node_9, cs_warehouse_sk_node_9, cs_item_sk_node_9, cs_promo_sk_node_9, cs_order_number_node_9, cs_quantity_node_9, cs_wholesale_cost_node_9, cs_list_price_node_9, cs_sales_price_node_9, cs_ext_discount_amt_node_9, cs_ext_sales_price_node_9, cs_ext_wholesale_cost_node_9, cs_ext_list_price_node_9, cs_ext_tax_node_9, cs_coupon_amt_node_9, cs_ext_ship_cost_node_9, cs_net_paid_node_9, cs_net_paid_inc_tax_node_9, cs_net_paid_inc_ship_node_9, cs_net_paid_inc_ship_tax_node_9, cs_net_profit_node_9, sm_ship_mode_sk_node_10, sm_ship_mode_id_node_10, sm_type_node_10, sm_code_node_10, sm_carrier_node_10, sm_contract_node_10, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cs_sold_date_sk AS VP8cA, cs_sold_time_sk AS cs_sold_time_sk_node_9, cs_ship_date_sk AS cs_ship_date_sk_node_9, cs_bill_customer_sk AS cs_bill_customer_sk_node_9, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_9, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_9, cs_bill_addr_sk AS cs_bill_addr_sk_node_9, cs_ship_customer_sk AS cs_ship_customer_sk_node_9, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_9, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_9, cs_ship_addr_sk AS cs_ship_addr_sk_node_9, cs_call_center_sk AS cs_call_center_sk_node_9, cs_catalog_page_sk AS cs_catalog_page_sk_node_9, cs_ship_mode_sk AS cs_ship_mode_sk_node_9, cs_warehouse_sk AS cs_warehouse_sk_node_9, cs_item_sk AS cs_item_sk_node_9, cs_promo_sk AS cs_promo_sk_node_9, cs_order_number AS cs_order_number_node_9, cs_quantity AS cs_quantity_node_9, cs_wholesale_cost AS cs_wholesale_cost_node_9, cs_list_price AS cs_list_price_node_9, cs_sales_price AS cs_sales_price_node_9, cs_ext_discount_amt AS cs_ext_discount_amt_node_9, cs_ext_sales_price AS cs_ext_sales_price_node_9, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_9, cs_ext_list_price AS cs_ext_list_price_node_9, cs_ext_tax AS cs_ext_tax_node_9, cs_coupon_amt AS cs_coupon_amt_node_9, cs_ext_ship_cost AS cs_ext_ship_cost_node_9, cs_net_paid AS cs_net_paid_node_9, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_9, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_9, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_9, cs_net_profit AS cs_net_profit_node_9, sm_ship_mode_sk AS sm_ship_mode_sk_node_10, sm_ship_mode_id AS sm_ship_mode_id_node_10, sm_type AS sm_type_node_10, sm_code AS sm_code_node_10, sm_carrier AS sm_carrier_node_10, sm_contract AS sm_contract_node_10])
         :     +- HashJoin(joinType=[InnerJoin], where=[=(cs_call_center_sk, sm_ship_mode_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
         :        :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         :        +- Exchange(distribution=[broadcast])
         :           +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
         +- Sort(orderBy=[ws_bill_customer_sk ASC])
            +- Exchange(distribution=[single])
               +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- Exchange(distribution=[hash[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit]])
                     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS oHErN, 'hello' AS _c1])
+- HashAggregate(isMerge=[false], groupBy=[ws_ship_addr_sk], select=[ws_ship_addr_sk, SUM(ws_ship_cdemo_sk) AS EXPR$0])
   +- Exchange(distribution=[hash[ws_ship_addr_sk]])
      +- NestedLoopJoin(joinType=[InnerJoin], where=[(ws_net_paid = cs_sales_price_node_9)], select=[VP8cA, cs_sold_time_sk_node_9, cs_ship_date_sk_node_9, cs_bill_customer_sk_node_9, cs_bill_cdemo_sk_node_9, cs_bill_hdemo_sk_node_9, cs_bill_addr_sk_node_9, cs_ship_customer_sk_node_9, cs_ship_cdemo_sk_node_9, cs_ship_hdemo_sk_node_9, cs_ship_addr_sk_node_9, cs_call_center_sk_node_9, cs_catalog_page_sk_node_9, cs_ship_mode_sk_node_9, cs_warehouse_sk_node_9, cs_item_sk_node_9, cs_promo_sk_node_9, cs_order_number_node_9, cs_quantity_node_9, cs_wholesale_cost_node_9, cs_list_price_node_9, cs_sales_price_node_9, cs_ext_discount_amt_node_9, cs_ext_sales_price_node_9, cs_ext_wholesale_cost_node_9, cs_ext_list_price_node_9, cs_ext_tax_node_9, cs_coupon_amt_node_9, cs_ext_ship_cost_node_9, cs_net_paid_node_9, cs_net_paid_inc_tax_node_9, cs_net_paid_inc_ship_node_9, cs_net_paid_inc_ship_tax_node_9, cs_net_profit_node_9, sm_ship_mode_sk_node_10, sm_ship_mode_id_node_10, sm_type_node_10, sm_code_node_10, sm_carrier_node_10, sm_contract_node_10, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[cs_sold_date_sk AS VP8cA, cs_sold_time_sk AS cs_sold_time_sk_node_9, cs_ship_date_sk AS cs_ship_date_sk_node_9, cs_bill_customer_sk AS cs_bill_customer_sk_node_9, cs_bill_cdemo_sk AS cs_bill_cdemo_sk_node_9, cs_bill_hdemo_sk AS cs_bill_hdemo_sk_node_9, cs_bill_addr_sk AS cs_bill_addr_sk_node_9, cs_ship_customer_sk AS cs_ship_customer_sk_node_9, cs_ship_cdemo_sk AS cs_ship_cdemo_sk_node_9, cs_ship_hdemo_sk AS cs_ship_hdemo_sk_node_9, cs_ship_addr_sk AS cs_ship_addr_sk_node_9, cs_call_center_sk AS cs_call_center_sk_node_9, cs_catalog_page_sk AS cs_catalog_page_sk_node_9, cs_ship_mode_sk AS cs_ship_mode_sk_node_9, cs_warehouse_sk AS cs_warehouse_sk_node_9, cs_item_sk AS cs_item_sk_node_9, cs_promo_sk AS cs_promo_sk_node_9, cs_order_number AS cs_order_number_node_9, cs_quantity AS cs_quantity_node_9, cs_wholesale_cost AS cs_wholesale_cost_node_9, cs_list_price AS cs_list_price_node_9, cs_sales_price AS cs_sales_price_node_9, cs_ext_discount_amt AS cs_ext_discount_amt_node_9, cs_ext_sales_price AS cs_ext_sales_price_node_9, cs_ext_wholesale_cost AS cs_ext_wholesale_cost_node_9, cs_ext_list_price AS cs_ext_list_price_node_9, cs_ext_tax AS cs_ext_tax_node_9, cs_coupon_amt AS cs_coupon_amt_node_9, cs_ext_ship_cost AS cs_ext_ship_cost_node_9, cs_net_paid AS cs_net_paid_node_9, cs_net_paid_inc_tax AS cs_net_paid_inc_tax_node_9, cs_net_paid_inc_ship AS cs_net_paid_inc_ship_node_9, cs_net_paid_inc_ship_tax AS cs_net_paid_inc_ship_tax_node_9, cs_net_profit AS cs_net_profit_node_9, sm_ship_mode_sk AS sm_ship_mode_sk_node_10, sm_ship_mode_id AS sm_ship_mode_id_node_10, sm_type AS sm_type_node_10, sm_code AS sm_code_node_10, sm_carrier AS sm_carrier_node_10, sm_contract AS sm_contract_node_10])
         :     +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(cs_call_center_sk = sm_ship_mode_sk)], select=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])\
+- [#2] Exchange(distribution=[broadcast])\
])
         :        :- TableSourceScan(table=[[default_catalog, default_database, catalog_sales]], fields=[cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk, cs_bill_customer_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_bill_addr_sk, cs_ship_customer_sk, cs_ship_cdemo_sk, cs_ship_hdemo_sk, cs_ship_addr_sk, cs_call_center_sk, cs_catalog_page_sk, cs_ship_mode_sk, cs_warehouse_sk, cs_item_sk, cs_promo_sk, cs_order_number, cs_quantity, cs_wholesale_cost, cs_list_price, cs_sales_price, cs_ext_discount_amt, cs_ext_sales_price, cs_ext_wholesale_cost, cs_ext_list_price, cs_ext_tax, cs_coupon_amt, cs_ext_ship_cost, cs_net_paid, cs_net_paid_inc_tax, cs_net_paid_inc_ship, cs_net_paid_inc_ship_tax, cs_net_profit])
         :        +- Exchange(distribution=[broadcast])
         :           +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
         +- Sort(orderBy=[ws_bill_customer_sk ASC])
            +- Exchange(distribution=[single])
               +- HashAggregate(isMerge=[false], groupBy=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                  +- Exchange(distribution=[hash[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit]])
                     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o289183026.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#583853986:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[4](input=RelSubset#583853984,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[4]), rel#583853983:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[4](input=RelSubset#583853982,groupBy=ws_ship_addr_sk, ws_net_paid,select=ws_ship_addr_sk, ws_net_paid, Partial_SUM(ws_ship_cdemo_sk) AS sum$0)]
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