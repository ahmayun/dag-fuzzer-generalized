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


def preloaded_aggregation(values: pd.Series) -> int:
    return values.count()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_10 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_10") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_9 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_9") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_11 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_7 = autonode_10.add_columns(lit("hello"))
autonode_6 = autonode_9.group_by(col('cr_returned_time_sk_node_9')).select(call('preloaded_udf_agg', col('cr_returning_cdemo_sk_node_9')).alias('cr_returning_cdemo_sk_node_9'))
autonode_8 = autonode_11.add_columns(lit("hello"))
autonode_4 = autonode_6.group_by(col('cr_returning_cdemo_sk_node_9')).select(col('cr_returning_cdemo_sk_node_9').min.alias('cr_returning_cdemo_sk_node_9'))
autonode_5 = autonode_7.join(autonode_8, col('d_year_node_11') == col('ws_ship_mode_sk_node_10'))
autonode_2 = autonode_4.add_columns(lit("hello"))
autonode_3 = autonode_5.order_by(col('d_fy_quarter_seq_node_11'))
autonode_1 = autonode_2.join(autonode_3, col('cr_returning_cdemo_sk_node_9') == col('ws_bill_hdemo_sk_node_10'))
sink = autonode_1.group_by(col('d_qoy_node_11')).select(col('d_dow_node_11').max.alias('d_dow_node_11'))
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
LogicalProject(d_dow_node_11=[$1])
+- LogicalAggregate(group=[{47}], EXPR$0=[MAX($44)])
   +- LogicalJoin(condition=[=($0, $8)], joinType=[inner])
      :- LogicalProject(cr_returning_cdemo_sk_node_9=[$1], _c1=[_UTF-16LE'hello'])
      :  +- LogicalAggregate(group=[{0}], EXPR$0=[MIN($0)])
      :     +- LogicalProject(cr_returning_cdemo_sk_node_9=[$1])
      :        +- LogicalAggregate(group=[{0}], EXPR$0=[preloaded_udf_agg($1)])
      :           +- LogicalProject(cr_returned_time_sk_node_9=[$1], cr_returning_cdemo_sk_node_9=[$8])
      :              +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
      +- LogicalSort(sort0=[$47], dir0=[ASC])
         +- LogicalJoin(condition=[=($41, $14)], joinType=[inner])
            :- LogicalProject(ws_sold_date_sk_node_10=[$0], ws_sold_time_sk_node_10=[$1], ws_ship_date_sk_node_10=[$2], ws_item_sk_node_10=[$3], ws_bill_customer_sk_node_10=[$4], ws_bill_cdemo_sk_node_10=[$5], ws_bill_hdemo_sk_node_10=[$6], ws_bill_addr_sk_node_10=[$7], ws_ship_customer_sk_node_10=[$8], ws_ship_cdemo_sk_node_10=[$9], ws_ship_hdemo_sk_node_10=[$10], ws_ship_addr_sk_node_10=[$11], ws_web_page_sk_node_10=[$12], ws_web_site_sk_node_10=[$13], ws_ship_mode_sk_node_10=[$14], ws_warehouse_sk_node_10=[$15], ws_promo_sk_node_10=[$16], ws_order_number_node_10=[$17], ws_quantity_node_10=[$18], ws_wholesale_cost_node_10=[$19], ws_list_price_node_10=[$20], ws_sales_price_node_10=[$21], ws_ext_discount_amt_node_10=[$22], ws_ext_sales_price_node_10=[$23], ws_ext_wholesale_cost_node_10=[$24], ws_ext_list_price_node_10=[$25], ws_ext_tax_node_10=[$26], ws_coupon_amt_node_10=[$27], ws_ext_ship_cost_node_10=[$28], ws_net_paid_node_10=[$29], ws_net_paid_inc_tax_node_10=[$30], ws_net_paid_inc_ship_node_10=[$31], ws_net_paid_inc_ship_tax_node_10=[$32], ws_net_profit_node_10=[$33], _c34=[_UTF-16LE'hello'])
            :  +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
            +- LogicalProject(d_date_sk_node_11=[$0], d_date_id_node_11=[$1], d_date_node_11=[$2], d_month_seq_node_11=[$3], d_week_seq_node_11=[$4], d_quarter_seq_node_11=[$5], d_year_node_11=[$6], d_dow_node_11=[$7], d_moy_node_11=[$8], d_dom_node_11=[$9], d_qoy_node_11=[$10], d_fy_year_node_11=[$11], d_fy_quarter_seq_node_11=[$12], d_fy_week_seq_node_11=[$13], d_day_name_node_11=[$14], d_quarter_name_node_11=[$15], d_holiday_node_11=[$16], d_weekend_node_11=[$17], d_following_holiday_node_11=[$18], d_first_dom_node_11=[$19], d_last_dom_node_11=[$20], d_same_day_ly_node_11=[$21], d_same_day_lq_node_11=[$22], d_current_day_node_11=[$23], d_current_week_node_11=[$24], d_current_month_node_11=[$25], d_current_quarter_node_11=[$26], d_current_year_node_11=[$27], _c28=[_UTF-16LE'hello'])
               +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS d_dow_node_11])
+- HashAggregate(isMerge=[false], groupBy=[d_qoy_node_11], select=[d_qoy_node_11, MAX(d_dow_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[d_qoy_node_11]])
      +- HashJoin(joinType=[InnerJoin], where=[=(cr_returning_cdemo_sk_node_9, ws_bill_hdemo_sk_node_100)], select=[cr_returning_cdemo_sk_node_9, ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, ws_bill_hdemo_sk_node_100], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_9])
         :     +- HashAggregate(isMerge=[true], groupBy=[cr_returning_cdemo_sk_node_9], select=[cr_returning_cdemo_sk_node_9, Final_MIN(min$0) AS EXPR$0])
         :        +- Exchange(distribution=[hash[cr_returning_cdemo_sk_node_9]])
         :           +- LocalHashAggregate(groupBy=[cr_returning_cdemo_sk_node_9], select=[cr_returning_cdemo_sk_node_9, Partial_MIN(cr_returning_cdemo_sk_node_9) AS min$0])
         :              +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_9])
         :                 +- PythonGroupAggregate(groupBy=[cr_returned_time_sk], select=[cr_returned_time_sk, preloaded_udf_agg(cr_returning_cdemo_sk) AS EXPR$0])
         :                    +- Sort(orderBy=[cr_returned_time_sk ASC])
         :                       +- Exchange(distribution=[hash[cr_returned_time_sk]])
         :                          +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returned_time_sk, cr_returning_cdemo_sk], metadata=[]]], fields=[cr_returned_time_sk, cr_returning_cdemo_sk])
         +- Calc(select=[ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, CAST(ws_bill_hdemo_sk_node_10 AS BIGINT) AS ws_bill_hdemo_sk_node_100])
            +- Sort(orderBy=[d_fy_quarter_seq_node_11 ASC])
               +- Exchange(distribution=[single])
                  +- HashJoin(joinType=[InnerJoin], where=[=(d_year_node_11, ws_ship_mode_sk_node_10)], select=[ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, _c34, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28], build=[right])
                     :- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_10, ws_sold_time_sk AS ws_sold_time_sk_node_10, ws_ship_date_sk AS ws_ship_date_sk_node_10, ws_item_sk AS ws_item_sk_node_10, ws_bill_customer_sk AS ws_bill_customer_sk_node_10, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_10, ws_bill_addr_sk AS ws_bill_addr_sk_node_10, ws_ship_customer_sk AS ws_ship_customer_sk_node_10, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_10, ws_ship_addr_sk AS ws_ship_addr_sk_node_10, ws_web_page_sk AS ws_web_page_sk_node_10, ws_web_site_sk AS ws_web_site_sk_node_10, ws_ship_mode_sk AS ws_ship_mode_sk_node_10, ws_warehouse_sk AS ws_warehouse_sk_node_10, ws_promo_sk AS ws_promo_sk_node_10, ws_order_number AS ws_order_number_node_10, ws_quantity AS ws_quantity_node_10, ws_wholesale_cost AS ws_wholesale_cost_node_10, ws_list_price AS ws_list_price_node_10, ws_sales_price AS ws_sales_price_node_10, ws_ext_discount_amt AS ws_ext_discount_amt_node_10, ws_ext_sales_price AS ws_ext_sales_price_node_10, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_10, ws_ext_list_price AS ws_ext_list_price_node_10, ws_ext_tax AS ws_ext_tax_node_10, ws_coupon_amt AS ws_coupon_amt_node_10, ws_ext_ship_cost AS ws_ext_ship_cost_node_10, ws_net_paid AS ws_net_paid_node_10, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_10, ws_net_profit AS ws_net_profit_node_10, 'hello' AS _c34])
                     :  +- Exchange(distribution=[hash[ws_ship_mode_sk]])
                     :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- Calc(select=[d_date_sk AS d_date_sk_node_11, d_date_id AS d_date_id_node_11, d_date AS d_date_node_11, d_month_seq AS d_month_seq_node_11, d_week_seq AS d_week_seq_node_11, d_quarter_seq AS d_quarter_seq_node_11, d_year AS d_year_node_11, d_dow AS d_dow_node_11, d_moy AS d_moy_node_11, d_dom AS d_dom_node_11, d_qoy AS d_qoy_node_11, d_fy_year AS d_fy_year_node_11, d_fy_quarter_seq AS d_fy_quarter_seq_node_11, d_fy_week_seq AS d_fy_week_seq_node_11, d_day_name AS d_day_name_node_11, d_quarter_name AS d_quarter_name_node_11, d_holiday AS d_holiday_node_11, d_weekend AS d_weekend_node_11, d_following_holiday AS d_following_holiday_node_11, d_first_dom AS d_first_dom_node_11, d_last_dom AS d_last_dom_node_11, d_same_day_ly AS d_same_day_ly_node_11, d_same_day_lq AS d_same_day_lq_node_11, d_current_day AS d_current_day_node_11, d_current_week AS d_current_week_node_11, d_current_month AS d_current_month_node_11, d_current_quarter AS d_current_quarter_node_11, d_current_year AS d_current_year_node_11, 'hello' AS _c28])
                        +- Exchange(distribution=[hash[d_year]])
                           +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS d_dow_node_11])
+- HashAggregate(isMerge=[false], groupBy=[d_qoy_node_11], select=[d_qoy_node_11, MAX(d_dow_node_11) AS EXPR$0])
   +- Exchange(distribution=[hash[d_qoy_node_11]])
      +- HashJoin(joinType=[InnerJoin], where=[(cr_returning_cdemo_sk_node_9 = ws_bill_hdemo_sk_node_100)], select=[cr_returning_cdemo_sk_node_9, ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, ws_bill_hdemo_sk_node_100], isBroadcast=[true], build=[left])
         :- Exchange(distribution=[broadcast])
         :  +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_9])
         :     +- HashAggregate(isMerge=[true], groupBy=[cr_returning_cdemo_sk_node_9], select=[cr_returning_cdemo_sk_node_9, Final_MIN(min$0) AS EXPR$0])
         :        +- Exchange(distribution=[hash[cr_returning_cdemo_sk_node_9]])
         :           +- LocalHashAggregate(groupBy=[cr_returning_cdemo_sk_node_9], select=[cr_returning_cdemo_sk_node_9, Partial_MIN(cr_returning_cdemo_sk_node_9) AS min$0])
         :              +- Calc(select=[EXPR$0 AS cr_returning_cdemo_sk_node_9])
         :                 +- PythonGroupAggregate(groupBy=[cr_returned_time_sk], select=[cr_returned_time_sk, preloaded_udf_agg(cr_returning_cdemo_sk) AS EXPR$0])
         :                    +- Exchange(distribution=[keep_input_as_is[hash[cr_returned_time_sk]]])
         :                       +- Sort(orderBy=[cr_returned_time_sk ASC])
         :                          +- Exchange(distribution=[hash[cr_returned_time_sk]])
         :                             +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns, project=[cr_returned_time_sk, cr_returning_cdemo_sk], metadata=[]]], fields=[cr_returned_time_sk, cr_returning_cdemo_sk])
         +- Calc(select=[ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, CAST(ws_bill_hdemo_sk_node_10 AS BIGINT) AS ws_bill_hdemo_sk_node_100])
            +- Sort(orderBy=[d_fy_quarter_seq_node_11 ASC])
               +- Exchange(distribution=[single])
                  +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(d_year_node_11 = ws_ship_mode_sk_node_10)], select=[ws_sold_date_sk_node_10, ws_sold_time_sk_node_10, ws_ship_date_sk_node_10, ws_item_sk_node_10, ws_bill_customer_sk_node_10, ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk_node_10, ws_bill_addr_sk_node_10, ws_ship_customer_sk_node_10, ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk_node_10, ws_ship_addr_sk_node_10, ws_web_page_sk_node_10, ws_web_site_sk_node_10, ws_ship_mode_sk_node_10, ws_warehouse_sk_node_10, ws_promo_sk_node_10, ws_order_number_node_10, ws_quantity_node_10, ws_wholesale_cost_node_10, ws_list_price_node_10, ws_sales_price_node_10, ws_ext_discount_amt_node_10, ws_ext_sales_price_node_10, ws_ext_wholesale_cost_node_10, ws_ext_list_price_node_10, ws_ext_tax_node_10, ws_coupon_amt_node_10, ws_ext_ship_cost_node_10, ws_net_paid_node_10, ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax_node_10, ws_net_profit_node_10, _c34, d_date_sk_node_11, d_date_id_node_11, d_date_node_11, d_month_seq_node_11, d_week_seq_node_11, d_quarter_seq_node_11, d_year_node_11, d_dow_node_11, d_moy_node_11, d_dom_node_11, d_qoy_node_11, d_fy_year_node_11, d_fy_quarter_seq_node_11, d_fy_week_seq_node_11, d_day_name_node_11, d_quarter_name_node_11, d_holiday_node_11, d_weekend_node_11, d_following_holiday_node_11, d_first_dom_node_11, d_last_dom_node_11, d_same_day_ly_node_11, d_same_day_lq_node_11, d_current_day_node_11, d_current_week_node_11, d_current_month_node_11, d_current_quarter_node_11, d_current_year_node_11, _c28], build=[right])\
:- Calc(select=[ws_sold_date_sk AS ws_sold_date_sk_node_10, ws_sold_time_sk AS ws_sold_time_sk_node_10, ws_ship_date_sk AS ws_ship_date_sk_node_10, ws_item_sk AS ws_item_sk_node_10, ws_bill_customer_sk AS ws_bill_customer_sk_node_10, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_10, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_10, ws_bill_addr_sk AS ws_bill_addr_sk_node_10, ws_ship_customer_sk AS ws_ship_customer_sk_node_10, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_10, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_10, ws_ship_addr_sk AS ws_ship_addr_sk_node_10, ws_web_page_sk AS ws_web_page_sk_node_10, ws_web_site_sk AS ws_web_site_sk_node_10, ws_ship_mode_sk AS ws_ship_mode_sk_node_10, ws_warehouse_sk AS ws_warehouse_sk_node_10, ws_promo_sk AS ws_promo_sk_node_10, ws_order_number AS ws_order_number_node_10, ws_quantity AS ws_quantity_node_10, ws_wholesale_cost AS ws_wholesale_cost_node_10, ws_list_price AS ws_list_price_node_10, ws_sales_price AS ws_sales_price_node_10, ws_ext_discount_amt AS ws_ext_discount_amt_node_10, ws_ext_sales_price AS ws_ext_sales_price_node_10, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_10, ws_ext_list_price AS ws_ext_list_price_node_10, ws_ext_tax AS ws_ext_tax_node_10, ws_coupon_amt AS ws_coupon_amt_node_10, ws_ext_ship_cost AS ws_ext_ship_cost_node_10, ws_net_paid AS ws_net_paid_node_10, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_10, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_10, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_10, ws_net_profit AS ws_net_profit_node_10, 'hello' AS _c34])\
:  +- [#1] Exchange(distribution=[hash[ws_ship_mode_sk]])\
+- Calc(select=[d_date_sk AS d_date_sk_node_11, d_date_id AS d_date_id_node_11, d_date AS d_date_node_11, d_month_seq AS d_month_seq_node_11, d_week_seq AS d_week_seq_node_11, d_quarter_seq AS d_quarter_seq_node_11, d_year AS d_year_node_11, d_dow AS d_dow_node_11, d_moy AS d_moy_node_11, d_dom AS d_dom_node_11, d_qoy AS d_qoy_node_11, d_fy_year AS d_fy_year_node_11, d_fy_quarter_seq AS d_fy_quarter_seq_node_11, d_fy_week_seq AS d_fy_week_seq_node_11, d_day_name AS d_day_name_node_11, d_quarter_name AS d_quarter_name_node_11, d_holiday AS d_holiday_node_11, d_weekend AS d_weekend_node_11, d_following_holiday AS d_following_holiday_node_11, d_first_dom AS d_first_dom_node_11, d_last_dom AS d_last_dom_node_11, d_same_day_ly AS d_same_day_ly_node_11, d_same_day_lq AS d_same_day_lq_node_11, d_current_day AS d_current_day_node_11, d_current_week AS d_current_week_node_11, d_current_month AS d_current_month_node_11, d_current_quarter AS d_current_quarter_node_11, d_current_year AS d_current_year_node_11, 'hello' AS _c28])\
   +- [#2] Exchange(distribution=[hash[d_year]])\
])
                     :- Exchange(distribution=[hash[ws_ship_mode_sk]])
                     :  +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
                     +- Exchange(distribution=[hash[d_year]])
                        +- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o186689116.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#377267523:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[46](input=RelSubset#377267521,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[46]), rel#377267520:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[46](input=RelSubset#377267519,groupBy=d_qoy_node_11, ws_bill_hdemo_sk_node_100,select=d_qoy_node_11, ws_bill_hdemo_sk_node_100, Partial_MAX(d_dow_node_11) AS max$0)]
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