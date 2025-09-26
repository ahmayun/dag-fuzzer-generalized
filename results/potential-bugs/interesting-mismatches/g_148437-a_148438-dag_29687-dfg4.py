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
    return values.max() - values.min()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_17 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_18 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_19 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_15 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_16 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_12 = autonode_17.alias('xI3tg')
autonode_13 = autonode_18.order_by(col('ws_web_page_sk_node_18'))
autonode_14 = autonode_19.add_columns(lit("hello"))
autonode_11 = autonode_15.join(autonode_16, col('wr_refunded_addr_sk_node_16') == col('hd_income_band_sk_node_15'))
autonode_9 = autonode_12.join(autonode_13, col('ws_quantity_node_18') == col('wr_returning_addr_sk_node_17'))
autonode_10 = autonode_14.add_columns(lit("hello"))
autonode_8 = autonode_11.order_by(col('wr_reason_sk_node_16'))
autonode_6 = autonode_9.filter(col('ws_ext_list_price_node_18') > 21.284019947052002)
autonode_7 = autonode_10.select(col('ws_net_paid_node_19'))
autonode_5 = autonode_8.filter(col('hd_income_band_sk_node_15') < 34)
autonode_4 = autonode_7.order_by(col('ws_net_paid_node_19'))
autonode_3 = autonode_5.join(autonode_6, col('ws_ext_wholesale_cost_node_18') == col('wr_return_ship_cost_node_16'))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_1 = autonode_3.group_by(col('ws_web_site_sk_node_18')).select(col('ws_ext_tax_node_18').sum.alias('ws_ext_tax_node_18'))
sink = autonode_1.join(autonode_2, col('ws_ext_tax_node_18') == col('ws_net_paid_node_19'))
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
LogicalJoin(condition=[=($0, $1)], joinType=[inner])
:- LogicalProject(ws_ext_tax_node_18=[$1])
:  +- LogicalAggregate(group=[{66}], EXPR$0=[SUM($79)])
:     +- LogicalJoin(condition=[=($77, $24)], joinType=[inner])
:        :- LogicalFilter(condition=[<($1, 34)])
:        :  +- LogicalSort(sort0=[$17], dir0=[ASC])
:        :     +- LogicalJoin(condition=[=($11, $1)], joinType=[inner])
:        :        :- LogicalProject(hd_demo_sk_node_15=[$0], hd_income_band_sk_node_15=[$1], hd_buy_potential_node_15=[$2], hd_dep_count_node_15=[$3], hd_vehicle_count_node_15=[$4])
:        :        :  +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
:        :        +- LogicalProject(wr_returned_date_sk_node_16=[$0], wr_returned_time_sk_node_16=[$1], wr_item_sk_node_16=[$2], wr_refunded_customer_sk_node_16=[$3], wr_refunded_cdemo_sk_node_16=[$4], wr_refunded_hdemo_sk_node_16=[$5], wr_refunded_addr_sk_node_16=[$6], wr_returning_customer_sk_node_16=[$7], wr_returning_cdemo_sk_node_16=[$8], wr_returning_hdemo_sk_node_16=[$9], wr_returning_addr_sk_node_16=[$10], wr_web_page_sk_node_16=[$11], wr_reason_sk_node_16=[$12], wr_order_number_node_16=[$13], wr_return_quantity_node_16=[$14], wr_return_amt_node_16=[$15], wr_return_tax_node_16=[$16], wr_return_amt_inc_tax_node_16=[$17], wr_fee_node_16=[$18], wr_return_ship_cost_node_16=[$19], wr_refunded_cash_node_16=[$20], wr_reversed_charge_node_16=[$21], wr_account_credit_node_16=[$22], wr_net_loss_node_16=[$23])
:        :           +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
:        +- LogicalFilter(condition=[>($49, 2.1284019947052002E1:DOUBLE)])
:           +- LogicalJoin(condition=[=($42, $10)], joinType=[inner])
:              :- LogicalProject(xI3tg=[AS($0, _UTF-16LE'xI3tg')], wr_returned_time_sk_node_17=[$1], wr_item_sk_node_17=[$2], wr_refunded_customer_sk_node_17=[$3], wr_refunded_cdemo_sk_node_17=[$4], wr_refunded_hdemo_sk_node_17=[$5], wr_refunded_addr_sk_node_17=[$6], wr_returning_customer_sk_node_17=[$7], wr_returning_cdemo_sk_node_17=[$8], wr_returning_hdemo_sk_node_17=[$9], wr_returning_addr_sk_node_17=[$10], wr_web_page_sk_node_17=[$11], wr_reason_sk_node_17=[$12], wr_order_number_node_17=[$13], wr_return_quantity_node_17=[$14], wr_return_amt_node_17=[$15], wr_return_tax_node_17=[$16], wr_return_amt_inc_tax_node_17=[$17], wr_fee_node_17=[$18], wr_return_ship_cost_node_17=[$19], wr_refunded_cash_node_17=[$20], wr_reversed_charge_node_17=[$21], wr_account_credit_node_17=[$22], wr_net_loss_node_17=[$23])
:              :  +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
:              +- LogicalSort(sort0=[$12], dir0=[ASC])
:                 +- LogicalProject(ws_sold_date_sk_node_18=[$0], ws_sold_time_sk_node_18=[$1], ws_ship_date_sk_node_18=[$2], ws_item_sk_node_18=[$3], ws_bill_customer_sk_node_18=[$4], ws_bill_cdemo_sk_node_18=[$5], ws_bill_hdemo_sk_node_18=[$6], ws_bill_addr_sk_node_18=[$7], ws_ship_customer_sk_node_18=[$8], ws_ship_cdemo_sk_node_18=[$9], ws_ship_hdemo_sk_node_18=[$10], ws_ship_addr_sk_node_18=[$11], ws_web_page_sk_node_18=[$12], ws_web_site_sk_node_18=[$13], ws_ship_mode_sk_node_18=[$14], ws_warehouse_sk_node_18=[$15], ws_promo_sk_node_18=[$16], ws_order_number_node_18=[$17], ws_quantity_node_18=[$18], ws_wholesale_cost_node_18=[$19], ws_list_price_node_18=[$20], ws_sales_price_node_18=[$21], ws_ext_discount_amt_node_18=[$22], ws_ext_sales_price_node_18=[$23], ws_ext_wholesale_cost_node_18=[$24], ws_ext_list_price_node_18=[$25], ws_ext_tax_node_18=[$26], ws_coupon_amt_node_18=[$27], ws_ext_ship_cost_node_18=[$28], ws_net_paid_node_18=[$29], ws_net_paid_inc_tax_node_18=[$30], ws_net_paid_inc_ship_node_18=[$31], ws_net_paid_inc_ship_tax_node_18=[$32], ws_net_profit_node_18=[$33])
:                    +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
+- LogicalProject(ws_net_paid_node_19=[$0], _c1=[_UTF-16LE'hello'])
   +- LogicalSort(sort0=[$0], dir0=[ASC])
      +- LogicalProject(ws_net_paid_node_19=[$29])
         +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])

== Optimized Physical Plan ==
Calc(select=[ws_ext_tax_node_18, ws_net_paid_node_19, 'hello' AS _c1])
+- HashJoin(joinType=[InnerJoin], where=[=(ws_ext_tax_node_18, ws_net_paid_node_190)], select=[ws_ext_tax_node_18, ws_net_paid_node_19, ws_net_paid_node_190], isBroadcast=[true], build=[left])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[EXPR$0 AS ws_ext_tax_node_18])
   :     +- HashAggregate(isMerge=[false], groupBy=[ws_web_site_sk], select=[ws_web_site_sk, SUM(ws_ext_tax) AS EXPR$0])
   :        +- Exchange(distribution=[hash[ws_web_site_sk]])
   :           +- HashJoin(joinType=[InnerJoin], where=[=(ws_ext_wholesale_cost, wr_return_ship_cost)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, xI3tg, wr_returned_time_sk_node_17, wr_item_sk_node_17, wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk_node_17, wr_returning_customer_sk_node_17, wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk_node_17, wr_returning_addr_sk_node_17, wr_web_page_sk_node_17, wr_reason_sk_node_17, wr_order_number_node_17, wr_return_quantity_node_17, wr_return_amt_node_17, wr_return_tax_node_17, wr_return_amt_inc_tax_node_17, wr_fee_node_17, wr_return_ship_cost_node_17, wr_refunded_cash_node_17, wr_reversed_charge_node_17, wr_account_credit_node_17, wr_net_loss_node_17, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
   :              :- Exchange(distribution=[hash[wr_return_ship_cost]])
   :              :  +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[<(hd_income_band_sk, 34)])
   :              :     +- Sort(orderBy=[wr_reason_sk ASC])
   :              :        +- Exchange(distribution=[single])
   :              :           +- HashJoin(joinType=[InnerJoin], where=[=(wr_refunded_addr_sk, hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
   :              :              :- Exchange(distribution=[broadcast])
   :              :              :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
   :              :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
   :              +- Exchange(distribution=[hash[ws_ext_wholesale_cost]])
   :                 +- HashJoin(joinType=[InnerJoin], where=[=(ws_quantity, wr_returning_addr_sk_node_17)], select=[xI3tg, wr_returned_time_sk_node_17, wr_item_sk_node_17, wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk_node_17, wr_returning_customer_sk_node_17, wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk_node_17, wr_returning_addr_sk_node_17, wr_web_page_sk_node_17, wr_reason_sk_node_17, wr_order_number_node_17, wr_return_quantity_node_17, wr_return_amt_node_17, wr_return_tax_node_17, wr_return_amt_inc_tax_node_17, wr_fee_node_17, wr_return_ship_cost_node_17, wr_refunded_cash_node_17, wr_reversed_charge_node_17, wr_account_credit_node_17, wr_net_loss_node_17, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
   :                    :- Exchange(distribution=[hash[wr_returning_addr_sk_node_17]])
   :                    :  +- Calc(select=[wr_returned_date_sk AS xI3tg, wr_returned_time_sk AS wr_returned_time_sk_node_17, wr_item_sk AS wr_item_sk_node_17, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_17, wr_returning_customer_sk AS wr_returning_customer_sk_node_17, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_17, wr_returning_addr_sk AS wr_returning_addr_sk_node_17, wr_web_page_sk AS wr_web_page_sk_node_17, wr_reason_sk AS wr_reason_sk_node_17, wr_order_number AS wr_order_number_node_17, wr_return_quantity AS wr_return_quantity_node_17, wr_return_amt AS wr_return_amt_node_17, wr_return_tax AS wr_return_tax_node_17, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_17, wr_fee AS wr_fee_node_17, wr_return_ship_cost AS wr_return_ship_cost_node_17, wr_refunded_cash AS wr_refunded_cash_node_17, wr_reversed_charge AS wr_reversed_charge_node_17, wr_account_credit AS wr_account_credit_node_17, wr_net_loss AS wr_net_loss_node_17])
   :                    :     +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
   :                    +- Exchange(distribution=[hash[ws_quantity]])
   :                       +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[>(ws_ext_list_price, 2.1284019947052002E1)])
   :                          +- Sort(orderBy=[ws_web_page_sk ASC])
   :                             +- Exchange(distribution=[single])
   :                                +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], metadata=[]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
   +- Calc(select=[ws_net_paid AS ws_net_paid_node_19, CAST(ws_net_paid AS DECIMAL(38, 2)) AS ws_net_paid_node_190])
      +- Sort(orderBy=[ws_net_paid ASC])
         +- Exchange(distribution=[single])
            +- Calc(select=[ws_net_paid])
               +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], metadata=[]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])

== Optimized Execution Plan ==
Calc(select=[ws_ext_tax_node_18, ws_net_paid_node_19, 'hello' AS _c1])
+- HashJoin(joinType=[InnerJoin], where=[(ws_ext_tax_node_18 = ws_net_paid_node_190)], select=[ws_ext_tax_node_18, ws_net_paid_node_19, ws_net_paid_node_190], isBroadcast=[true], build=[left])
   :- Exchange(distribution=[broadcast])
   :  +- Calc(select=[EXPR$0 AS ws_ext_tax_node_18])
   :     +- HashAggregate(isMerge=[false], groupBy=[ws_web_site_sk], select=[ws_web_site_sk, SUM(ws_ext_tax) AS EXPR$0])
   :        +- Exchange(distribution=[hash[ws_web_site_sk]])
   :           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ws_ext_wholesale_cost = wr_return_ship_cost)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss, xI3tg, wr_returned_time_sk_node_17, wr_item_sk_node_17, wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk_node_17, wr_returning_customer_sk_node_17, wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk_node_17, wr_returning_addr_sk_node_17, wr_web_page_sk_node_17, wr_reason_sk_node_17, wr_order_number_node_17, wr_return_quantity_node_17, wr_return_amt_node_17, wr_return_tax_node_17, wr_return_amt_inc_tax_node_17, wr_fee_node_17, wr_return_ship_cost_node_17, wr_refunded_cash_node_17, wr_reversed_charge_node_17, wr_account_credit_node_17, wr_net_loss_node_17, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
   :              :- Exchange(distribution=[hash[wr_return_ship_cost]])
   :              :  +- Calc(select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[(hd_income_band_sk < 34)])
   :              :     +- Sort(orderBy=[wr_reason_sk ASC])
   :              :        +- Exchange(distribution=[single])
   :              :           +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(wr_refunded_addr_sk = hd_income_band_sk)], select=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])\
])
   :              :              :- Exchange(distribution=[broadcast])
   :              :              :  +- TableSourceScan(table=[[default_catalog, default_database, household_demographics]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
   :              :              +- TableSourceScan(table=[[default_catalog, default_database, web_returns]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])(reuse_id=[1])
   :              +- Exchange(distribution=[hash[ws_ext_wholesale_cost]])
   :                 +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ws_quantity = wr_returning_addr_sk_node_17)], select=[xI3tg, wr_returned_time_sk_node_17, wr_item_sk_node_17, wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk_node_17, wr_returning_customer_sk_node_17, wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk_node_17, wr_returning_addr_sk_node_17, wr_web_page_sk_node_17, wr_reason_sk_node_17, wr_order_number_node_17, wr_return_quantity_node_17, wr_return_amt_node_17, wr_return_tax_node_17, wr_return_amt_inc_tax_node_17, wr_fee_node_17, wr_return_ship_cost_node_17, wr_refunded_cash_node_17, wr_reversed_charge_node_17, wr_account_credit_node_17, wr_net_loss_node_17, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], build=[left])
   :                    :- Exchange(distribution=[hash[wr_returning_addr_sk_node_17]])
   :                    :  +- Calc(select=[wr_returned_date_sk AS xI3tg, wr_returned_time_sk AS wr_returned_time_sk_node_17, wr_item_sk AS wr_item_sk_node_17, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_17, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_17, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_17, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_17, wr_returning_customer_sk AS wr_returning_customer_sk_node_17, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_17, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_17, wr_returning_addr_sk AS wr_returning_addr_sk_node_17, wr_web_page_sk AS wr_web_page_sk_node_17, wr_reason_sk AS wr_reason_sk_node_17, wr_order_number AS wr_order_number_node_17, wr_return_quantity AS wr_return_quantity_node_17, wr_return_amt AS wr_return_amt_node_17, wr_return_tax AS wr_return_tax_node_17, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_17, wr_fee AS wr_fee_node_17, wr_return_ship_cost AS wr_return_ship_cost_node_17, wr_refunded_cash AS wr_refunded_cash_node_17, wr_reversed_charge AS wr_reversed_charge_node_17, wr_account_credit AS wr_account_credit_node_17, wr_net_loss AS wr_net_loss_node_17])
   :                    :     +- Reused(reference_id=[1])
   :                    +- Exchange(distribution=[hash[ws_quantity]])
   :                       +- Calc(select=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], where=[(ws_ext_list_price > 2.1284019947052002E1)])
   :                          +- Sort(orderBy=[ws_web_page_sk ASC])
   :                             +- Exchange(distribution=[single])
   :                                +- TableSourceScan(table=[[default_catalog, default_database, web_sales, project=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], metadata=[]]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])(reuse_id=[2])
   +- Calc(select=[ws_net_paid AS ws_net_paid_node_19, CAST(ws_net_paid AS DECIMAL(38, 2)) AS ws_net_paid_node_190])
      +- Sort(orderBy=[ws_net_paid ASC])
         +- Exchange(distribution=[single])
            +- Calc(select=[ws_net_paid])
               +- Reused(reference_id=[2])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o80887298.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#163059294:AbstractConverter.BATCH_PHYSICAL.hash[0]true.[17](input=RelSubset#163059292,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0]true,sort=[17]), rel#163059291:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#163059290,groupBy=wr_return_ship_cost,select=wr_return_ship_cost, Partial_COUNT(*) AS count1$0)]
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