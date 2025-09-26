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
    return values.skew()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_25 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_25") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_24 = table_env.from_path("catalog_returns").select(*[col(column_name).alias(f"{column_name}_node_24") for column_name in table_env.from_path("catalog_returns").get_schema().get_field_names()])
autonode_21 = table_env.from_path("household_demographics").select(*[col(column_name).alias(f"{column_name}_node_21") for column_name in table_env.from_path("household_demographics").get_schema().get_field_names()])
autonode_23 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_23") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_22 = table_env.from_path("web_returns").select(*[col(column_name).alias(f"{column_name}_node_22") for column_name in table_env.from_path("web_returns").get_schema().get_field_names()])
autonode_20 = autonode_25.filter(col('c_last_review_date_node_25').char_length < 5)
autonode_19 = autonode_24.order_by(col('cr_refunded_cdemo_sk_node_24'))
autonode_16 = autonode_21.add_columns(lit("hello"))
autonode_18 = autonode_23.alias('loHfH')
autonode_17 = autonode_22.filter(col('wr_net_loss_node_22') <= 47.41256237030029)
autonode_15 = autonode_20.group_by(col('c_last_review_date_node_25')).select(call('preloaded_udf_agg', col('c_first_shipto_date_sk_node_25')).alias('c_first_shipto_date_sk_node_25'))
autonode_14 = autonode_19.order_by(col('cr_return_quantity_node_24'))
autonode_11 = autonode_16.filter(col('hd_buy_potential_node_21').char_length <= 5)
autonode_13 = autonode_18.distinct()
autonode_12 = autonode_17.order_by(col('wr_refunded_cash_node_22'))
autonode_10 = autonode_14.join(autonode_15, col('c_first_shipto_date_sk_node_25') == col('cr_refunded_customer_sk_node_24'))
autonode_9 = autonode_13.filter(col('ss_cdemo_sk_node_23') <= 0)
autonode_8 = autonode_11.join(autonode_12, col('wr_refunded_cdemo_sk_node_22') == col('hd_income_band_sk_node_21'))
autonode_7 = autonode_10.group_by(col('cr_item_sk_node_24')).select(col('cr_item_sk_node_24').min.alias('cr_item_sk_node_24'))
autonode_6 = autonode_9.add_columns(lit("hello"))
autonode_5 = autonode_8.order_by(col('hd_buy_potential_node_21'))
autonode_4 = autonode_6.join(autonode_7, col('cr_item_sk_node_24') == col('ss_addr_sk_node_23'))
autonode_3 = autonode_5.limit(76)
autonode_2 = autonode_4.filter(col('ss_customer_sk_node_23') < 49)
autonode_1 = autonode_3.alias('cgcXF')
sink = autonode_1.join(autonode_2, col('ss_ext_sales_price_node_23') == col('wr_reversed_charge_node_22'))
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
LogicalJoin(condition=[=($45, $27)], joinType=[inner])
:- LogicalProject(cgcXF=[AS($0, _UTF-16LE'cgcXF')], hd_income_band_sk_node_21=[$1], hd_buy_potential_node_21=[$2], hd_dep_count_node_21=[$3], hd_vehicle_count_node_21=[$4], _c5=[$5], wr_returned_date_sk_node_22=[$6], wr_returned_time_sk_node_22=[$7], wr_item_sk_node_22=[$8], wr_refunded_customer_sk_node_22=[$9], wr_refunded_cdemo_sk_node_22=[$10], wr_refunded_hdemo_sk_node_22=[$11], wr_refunded_addr_sk_node_22=[$12], wr_returning_customer_sk_node_22=[$13], wr_returning_cdemo_sk_node_22=[$14], wr_returning_hdemo_sk_node_22=[$15], wr_returning_addr_sk_node_22=[$16], wr_web_page_sk_node_22=[$17], wr_reason_sk_node_22=[$18], wr_order_number_node_22=[$19], wr_return_quantity_node_22=[$20], wr_return_amt_node_22=[$21], wr_return_tax_node_22=[$22], wr_return_amt_inc_tax_node_22=[$23], wr_fee_node_22=[$24], wr_return_ship_cost_node_22=[$25], wr_refunded_cash_node_22=[$26], wr_reversed_charge_node_22=[$27], wr_account_credit_node_22=[$28], wr_net_loss_node_22=[$29])
:  +- LogicalSort(sort0=[$2], dir0=[ASC], fetch=[76])
:     +- LogicalJoin(condition=[=($10, $1)], joinType=[inner])
:        :- LogicalFilter(condition=[<=(CHAR_LENGTH($2), 5)])
:        :  +- LogicalProject(hd_demo_sk_node_21=[$0], hd_income_band_sk_node_21=[$1], hd_buy_potential_node_21=[$2], hd_dep_count_node_21=[$3], hd_vehicle_count_node_21=[$4], _c5=[_UTF-16LE'hello'])
:        :     +- LogicalTableScan(table=[[default_catalog, default_database, household_demographics]])
:        +- LogicalSort(sort0=[$20], dir0=[ASC])
:           +- LogicalFilter(condition=[<=($23, 4.741256237030029E1:DOUBLE)])
:              +- LogicalProject(wr_returned_date_sk_node_22=[$0], wr_returned_time_sk_node_22=[$1], wr_item_sk_node_22=[$2], wr_refunded_customer_sk_node_22=[$3], wr_refunded_cdemo_sk_node_22=[$4], wr_refunded_hdemo_sk_node_22=[$5], wr_refunded_addr_sk_node_22=[$6], wr_returning_customer_sk_node_22=[$7], wr_returning_cdemo_sk_node_22=[$8], wr_returning_hdemo_sk_node_22=[$9], wr_returning_addr_sk_node_22=[$10], wr_web_page_sk_node_22=[$11], wr_reason_sk_node_22=[$12], wr_order_number_node_22=[$13], wr_return_quantity_node_22=[$14], wr_return_amt_node_22=[$15], wr_return_tax_node_22=[$16], wr_return_amt_inc_tax_node_22=[$17], wr_fee_node_22=[$18], wr_return_ship_cost_node_22=[$19], wr_refunded_cash_node_22=[$20], wr_reversed_charge_node_22=[$21], wr_account_credit_node_22=[$22], wr_net_loss_node_22=[$23])
:                 +- LogicalTableScan(table=[[default_catalog, default_database, web_returns]])
+- LogicalFilter(condition=[<($3, 49)])
   +- LogicalJoin(condition=[=($24, $6)], joinType=[inner])
      :- LogicalProject(loHfH=[$0], ss_sold_time_sk_node_23=[$1], ss_item_sk_node_23=[$2], ss_customer_sk_node_23=[$3], ss_cdemo_sk_node_23=[$4], ss_hdemo_sk_node_23=[$5], ss_addr_sk_node_23=[$6], ss_store_sk_node_23=[$7], ss_promo_sk_node_23=[$8], ss_ticket_number_node_23=[$9], ss_quantity_node_23=[$10], ss_wholesale_cost_node_23=[$11], ss_list_price_node_23=[$12], ss_sales_price_node_23=[$13], ss_ext_discount_amt_node_23=[$14], ss_ext_sales_price_node_23=[$15], ss_ext_wholesale_cost_node_23=[$16], ss_ext_list_price_node_23=[$17], ss_ext_tax_node_23=[$18], ss_coupon_amt_node_23=[$19], ss_net_paid_node_23=[$20], ss_net_paid_inc_tax_node_23=[$21], ss_net_profit_node_23=[$22], _c23=[_UTF-16LE'hello'])
      :  +- LogicalFilter(condition=[<=($4, 0)])
      :     +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22}])
      :        +- LogicalProject(loHfH=[AS($0, _UTF-16LE'loHfH')], ss_sold_time_sk_node_23=[$1], ss_item_sk_node_23=[$2], ss_customer_sk_node_23=[$3], ss_cdemo_sk_node_23=[$4], ss_hdemo_sk_node_23=[$5], ss_addr_sk_node_23=[$6], ss_store_sk_node_23=[$7], ss_promo_sk_node_23=[$8], ss_ticket_number_node_23=[$9], ss_quantity_node_23=[$10], ss_wholesale_cost_node_23=[$11], ss_list_price_node_23=[$12], ss_sales_price_node_23=[$13], ss_ext_discount_amt_node_23=[$14], ss_ext_sales_price_node_23=[$15], ss_ext_wholesale_cost_node_23=[$16], ss_ext_list_price_node_23=[$17], ss_ext_tax_node_23=[$18], ss_coupon_amt_node_23=[$19], ss_net_paid_node_23=[$20], ss_net_paid_inc_tax_node_23=[$21], ss_net_profit_node_23=[$22])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      +- LogicalProject(cr_item_sk_node_24=[$1])
         +- LogicalAggregate(group=[{2}], EXPR$0=[MIN($2)])
            +- LogicalJoin(condition=[=($27, $3)], joinType=[inner])
               :- LogicalSort(sort0=[$17], dir0=[ASC])
               :  +- LogicalSort(sort0=[$4], dir0=[ASC])
               :     +- LogicalProject(cr_returned_date_sk_node_24=[$0], cr_returned_time_sk_node_24=[$1], cr_item_sk_node_24=[$2], cr_refunded_customer_sk_node_24=[$3], cr_refunded_cdemo_sk_node_24=[$4], cr_refunded_hdemo_sk_node_24=[$5], cr_refunded_addr_sk_node_24=[$6], cr_returning_customer_sk_node_24=[$7], cr_returning_cdemo_sk_node_24=[$8], cr_returning_hdemo_sk_node_24=[$9], cr_returning_addr_sk_node_24=[$10], cr_call_center_sk_node_24=[$11], cr_catalog_page_sk_node_24=[$12], cr_ship_mode_sk_node_24=[$13], cr_warehouse_sk_node_24=[$14], cr_reason_sk_node_24=[$15], cr_order_number_node_24=[$16], cr_return_quantity_node_24=[$17], cr_return_amount_node_24=[$18], cr_return_tax_node_24=[$19], cr_return_amt_inc_tax_node_24=[$20], cr_fee_node_24=[$21], cr_return_ship_cost_node_24=[$22], cr_refunded_cash_node_24=[$23], cr_reversed_charge_node_24=[$24], cr_store_credit_node_24=[$25], cr_net_loss_node_24=[$26])
               :        +- LogicalTableScan(table=[[default_catalog, default_database, catalog_returns]])
               +- LogicalProject(c_first_shipto_date_sk_node_25=[$1])
                  +- LogicalAggregate(group=[{17}], EXPR$0=[preloaded_udf_agg($5)])
                     +- LogicalFilter(condition=[<(CHAR_LENGTH($17), 5)])
                        +- LogicalProject(c_customer_sk_node_25=[$0], c_customer_id_node_25=[$1], c_current_cdemo_sk_node_25=[$2], c_current_hdemo_sk_node_25=[$3], c_current_addr_sk_node_25=[$4], c_first_shipto_date_sk_node_25=[$5], c_first_sales_date_sk_node_25=[$6], c_salutation_node_25=[$7], c_first_name_node_25=[$8], c_last_name_node_25=[$9], c_preferred_cust_flag_node_25=[$10], c_birth_day_node_25=[$11], c_birth_month_node_25=[$12], c_birth_year_node_25=[$13], c_birth_country_node_25=[$14], c_login_node_25=[$15], c_email_address_node_25=[$16], c_last_review_date_node_25=[$17])
                           +- LogicalTableScan(table=[[default_catalog, default_database, customer]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(ss_ext_sales_price_node_23, wr_reversed_charge_node_22)], select=[cgcXF, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, _c5, wr_returned_date_sk_node_22, wr_returned_time_sk_node_22, wr_item_sk_node_22, wr_refunded_customer_sk_node_22, wr_refunded_cdemo_sk_node_22, wr_refunded_hdemo_sk_node_22, wr_refunded_addr_sk_node_22, wr_returning_customer_sk_node_22, wr_returning_cdemo_sk_node_22, wr_returning_hdemo_sk_node_22, wr_returning_addr_sk_node_22, wr_web_page_sk_node_22, wr_reason_sk_node_22, wr_order_number_node_22, wr_return_quantity_node_22, wr_return_amt_node_22, wr_return_tax_node_22, wr_return_amt_inc_tax_node_22, wr_fee_node_22, wr_return_ship_cost_node_22, wr_refunded_cash_node_22, wr_reversed_charge_node_22, wr_account_credit_node_22, wr_net_loss_node_22, loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, _c23, cr_item_sk_node_24], isBroadcast=[true], build=[left])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[hd_demo_sk_node_21 AS cgcXF, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, 'hello' AS _c5, wr_returned_date_sk AS wr_returned_date_sk_node_22, wr_returned_time_sk AS wr_returned_time_sk_node_22, wr_item_sk AS wr_item_sk_node_22, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_22, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_22, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_22, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_22, wr_returning_customer_sk AS wr_returning_customer_sk_node_22, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_22, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_22, wr_returning_addr_sk AS wr_returning_addr_sk_node_22, wr_web_page_sk AS wr_web_page_sk_node_22, wr_reason_sk AS wr_reason_sk_node_22, wr_order_number AS wr_order_number_node_22, wr_return_quantity AS wr_return_quantity_node_22, wr_return_amt AS wr_return_amt_node_22, wr_return_tax AS wr_return_tax_node_22, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_22, wr_fee AS wr_fee_node_22, wr_return_ship_cost AS wr_return_ship_cost_node_22, wr_refunded_cash AS wr_refunded_cash_node_22, wr_reversed_charge AS wr_reversed_charge_node_22, wr_account_credit AS wr_account_credit_node_22, wr_net_loss AS wr_net_loss_node_22])
:     +- SortLimit(orderBy=[hd_buy_potential_node_21 ASC], offset=[0], fetch=[76], global=[true])
:        +- Exchange(distribution=[single])
:           +- SortLimit(orderBy=[hd_buy_potential_node_21 ASC], offset=[0], fetch=[76], global=[false])
:              +- HashJoin(joinType=[InnerJoin], where=[=(wr_refunded_cdemo_sk, hd_income_band_sk_node_21)], select=[hd_demo_sk_node_21, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, _c5, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
:                 :- Exchange(distribution=[broadcast])
:                 :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_21, hd_income_band_sk AS hd_income_band_sk_node_21, hd_buy_potential AS hd_buy_potential_node_21, hd_dep_count AS hd_dep_count_node_21, hd_vehicle_count AS hd_vehicle_count_node_21, 'hello' AS _c5], where=[<=(CHAR_LENGTH(hd_buy_potential), 5)])
:                 :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[<=(CHAR_LENGTH(hd_buy_potential), 5)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
:                 +- Sort(orderBy=[wr_refunded_cash ASC])
:                    +- Exchange(distribution=[single])
:                       +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[<=(wr_net_loss, 4.741256237030029E1)])
:                          +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_net_loss, 4.741256237030029E1:DOUBLE)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
+- HashJoin(joinType=[InnerJoin], where=[=(cr_item_sk_node_24, ss_addr_sk_node_23)], select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, _c23, cr_item_sk_node_24], isBroadcast=[true], build=[right])
   :- Calc(select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, 'hello' AS _c23])
   :  +- HashAggregate(isMerge=[false], groupBy=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23], select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23])
   :     +- Exchange(distribution=[hash[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23]])
   :        +- Calc(select=[ss_sold_date_sk AS loHfH, ss_sold_time_sk AS ss_sold_time_sk_node_23, ss_item_sk AS ss_item_sk_node_23, ss_customer_sk AS ss_customer_sk_node_23, ss_cdemo_sk AS ss_cdemo_sk_node_23, ss_hdemo_sk AS ss_hdemo_sk_node_23, ss_addr_sk AS ss_addr_sk_node_23, ss_store_sk AS ss_store_sk_node_23, ss_promo_sk AS ss_promo_sk_node_23, ss_ticket_number AS ss_ticket_number_node_23, ss_quantity AS ss_quantity_node_23, ss_wholesale_cost AS ss_wholesale_cost_node_23, ss_list_price AS ss_list_price_node_23, ss_sales_price AS ss_sales_price_node_23, ss_ext_discount_amt AS ss_ext_discount_amt_node_23, ss_ext_sales_price AS ss_ext_sales_price_node_23, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_23, ss_ext_list_price AS ss_ext_list_price_node_23, ss_ext_tax AS ss_ext_tax_node_23, ss_coupon_amt AS ss_coupon_amt_node_23, ss_net_paid AS ss_net_paid_node_23, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_23, ss_net_profit AS ss_net_profit_node_23], where=[AND(<=(ss_cdemo_sk, 0), <(ss_customer_sk, 49))])
   :           +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[and(<=(ss_cdemo_sk, 0), <(ss_customer_sk, 49))]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])
   +- Exchange(distribution=[broadcast])
      +- Calc(select=[EXPR$0 AS cr_item_sk_node_24])
         +- HashAggregate(isMerge=[true], groupBy=[cr_item_sk_node_24], select=[cr_item_sk_node_24, Final_MIN(min$0) AS EXPR$0])
            +- Exchange(distribution=[hash[cr_item_sk_node_24]])
               +- LocalHashAggregate(groupBy=[cr_item_sk_node_24], select=[cr_item_sk_node_24, Partial_MIN(cr_item_sk_node_24) AS min$0])
                  +- Calc(select=[cr_returned_date_sk_node_24, cr_returned_time_sk_node_24, cr_item_sk_node_24, cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk_node_24, cr_returning_customer_sk_node_24, cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk_node_24, cr_returning_addr_sk_node_24, cr_call_center_sk_node_24, cr_catalog_page_sk_node_24, cr_ship_mode_sk_node_24, cr_warehouse_sk_node_24, cr_reason_sk_node_24, cr_order_number_node_24, cr_return_quantity_node_24, cr_return_amount_node_24, cr_return_tax_node_24, cr_return_amt_inc_tax_node_24, cr_fee_node_24, cr_return_ship_cost_node_24, cr_refunded_cash_node_24, cr_reversed_charge_node_24, cr_store_credit_node_24, cr_net_loss_node_24, c_first_shipto_date_sk_node_25])
                     +- HashJoin(joinType=[InnerJoin], where=[=(c_first_shipto_date_sk_node_25, cr_refunded_customer_sk_node_240)], select=[cr_returned_date_sk_node_24, cr_returned_time_sk_node_24, cr_item_sk_node_24, cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk_node_24, cr_returning_customer_sk_node_24, cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk_node_24, cr_returning_addr_sk_node_24, cr_call_center_sk_node_24, cr_catalog_page_sk_node_24, cr_ship_mode_sk_node_24, cr_warehouse_sk_node_24, cr_reason_sk_node_24, cr_order_number_node_24, cr_return_quantity_node_24, cr_return_amount_node_24, cr_return_tax_node_24, cr_return_amt_inc_tax_node_24, cr_fee_node_24, cr_return_ship_cost_node_24, cr_refunded_cash_node_24, cr_reversed_charge_node_24, cr_store_credit_node_24, cr_net_loss_node_24, cr_refunded_customer_sk_node_240, c_first_shipto_date_sk_node_25], isBroadcast=[true], build=[right])
                        :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_24, cr_returned_time_sk AS cr_returned_time_sk_node_24, cr_item_sk AS cr_item_sk_node_24, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_24, cr_returning_customer_sk AS cr_returning_customer_sk_node_24, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_24, cr_returning_addr_sk AS cr_returning_addr_sk_node_24, cr_call_center_sk AS cr_call_center_sk_node_24, cr_catalog_page_sk AS cr_catalog_page_sk_node_24, cr_ship_mode_sk AS cr_ship_mode_sk_node_24, cr_warehouse_sk AS cr_warehouse_sk_node_24, cr_reason_sk AS cr_reason_sk_node_24, cr_order_number AS cr_order_number_node_24, cr_return_quantity AS cr_return_quantity_node_24, cr_return_amount AS cr_return_amount_node_24, cr_return_tax AS cr_return_tax_node_24, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_24, cr_fee AS cr_fee_node_24, cr_return_ship_cost AS cr_return_ship_cost_node_24, cr_refunded_cash AS cr_refunded_cash_node_24, cr_reversed_charge AS cr_reversed_charge_node_24, cr_store_credit AS cr_store_credit_node_24, cr_net_loss AS cr_net_loss_node_24, CAST(cr_refunded_customer_sk AS BIGINT) AS cr_refunded_customer_sk_node_240])
                        :  +- Sort(orderBy=[cr_return_quantity ASC])
                        :     +- Sort(orderBy=[cr_refunded_cdemo_sk ASC])
                        :        +- Exchange(distribution=[single])
                        :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
                        +- Exchange(distribution=[broadcast])
                           +- Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_25])
                              +- PythonGroupAggregate(groupBy=[c_last_review_date], select=[c_last_review_date, preloaded_udf_agg(c_first_shipto_date_sk) AS EXPR$0])
                                 +- Sort(orderBy=[c_last_review_date ASC])
                                    +- Exchange(distribution=[hash[c_last_review_date]])
                                       +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[<(CHAR_LENGTH(c_last_review_date), 5)])
                                          +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[<(CHAR_LENGTH(c_last_review_date), 5)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ss_ext_sales_price_node_23 = wr_reversed_charge_node_22)], select=[cgcXF, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, _c5, wr_returned_date_sk_node_22, wr_returned_time_sk_node_22, wr_item_sk_node_22, wr_refunded_customer_sk_node_22, wr_refunded_cdemo_sk_node_22, wr_refunded_hdemo_sk_node_22, wr_refunded_addr_sk_node_22, wr_returning_customer_sk_node_22, wr_returning_cdemo_sk_node_22, wr_returning_hdemo_sk_node_22, wr_returning_addr_sk_node_22, wr_web_page_sk_node_22, wr_reason_sk_node_22, wr_order_number_node_22, wr_return_quantity_node_22, wr_return_amt_node_22, wr_return_tax_node_22, wr_return_amt_inc_tax_node_22, wr_fee_node_22, wr_return_ship_cost_node_22, wr_refunded_cash_node_22, wr_reversed_charge_node_22, wr_account_credit_node_22, wr_net_loss_node_22, loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, _c23, cr_item_sk_node_24], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- HashJoin(joinType=[InnerJoin], where=[(cr_item_sk_node_24 = ss_addr_sk_node_23)], select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, _c23, cr_item_sk_node_24], isBroadcast=[true], build=[right])\
   :- Calc(select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23, 'hello' AS _c23])\
   :  +- HashAggregate(isMerge=[false], groupBy=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23], select=[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23])\
   :     +- [#3] Exchange(distribution=[hash[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23]])\
   +- [#2] Exchange(distribution=[broadcast])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[hd_demo_sk_node_21 AS cgcXF, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, 'hello' AS _c5, wr_returned_date_sk AS wr_returned_date_sk_node_22, wr_returned_time_sk AS wr_returned_time_sk_node_22, wr_item_sk AS wr_item_sk_node_22, wr_refunded_customer_sk AS wr_refunded_customer_sk_node_22, wr_refunded_cdemo_sk AS wr_refunded_cdemo_sk_node_22, wr_refunded_hdemo_sk AS wr_refunded_hdemo_sk_node_22, wr_refunded_addr_sk AS wr_refunded_addr_sk_node_22, wr_returning_customer_sk AS wr_returning_customer_sk_node_22, wr_returning_cdemo_sk AS wr_returning_cdemo_sk_node_22, wr_returning_hdemo_sk AS wr_returning_hdemo_sk_node_22, wr_returning_addr_sk AS wr_returning_addr_sk_node_22, wr_web_page_sk AS wr_web_page_sk_node_22, wr_reason_sk AS wr_reason_sk_node_22, wr_order_number AS wr_order_number_node_22, wr_return_quantity AS wr_return_quantity_node_22, wr_return_amt AS wr_return_amt_node_22, wr_return_tax AS wr_return_tax_node_22, wr_return_amt_inc_tax AS wr_return_amt_inc_tax_node_22, wr_fee AS wr_fee_node_22, wr_return_ship_cost AS wr_return_ship_cost_node_22, wr_refunded_cash AS wr_refunded_cash_node_22, wr_reversed_charge AS wr_reversed_charge_node_22, wr_account_credit AS wr_account_credit_node_22, wr_net_loss AS wr_net_loss_node_22])
:     +- SortLimit(orderBy=[hd_buy_potential_node_21 ASC], offset=[0], fetch=[76], global=[true])
:        +- Exchange(distribution=[single])
:           +- SortLimit(orderBy=[hd_buy_potential_node_21 ASC], offset=[0], fetch=[76], global=[false])
:              +- HashJoin(joinType=[InnerJoin], where=[(wr_refunded_cdemo_sk = hd_income_band_sk_node_21)], select=[hd_demo_sk_node_21, hd_income_band_sk_node_21, hd_buy_potential_node_21, hd_dep_count_node_21, hd_vehicle_count_node_21, _c5, wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], isBroadcast=[true], build=[left])
:                 :- Exchange(distribution=[broadcast])
:                 :  +- Calc(select=[hd_demo_sk AS hd_demo_sk_node_21, hd_income_band_sk AS hd_income_band_sk_node_21, hd_buy_potential AS hd_buy_potential_node_21, hd_dep_count AS hd_dep_count_node_21, hd_vehicle_count AS hd_vehicle_count_node_21, 'hello' AS _c5], where=[(CHAR_LENGTH(hd_buy_potential) <= 5)])
:                 :     +- TableSourceScan(table=[[default_catalog, default_database, household_demographics, filter=[<=(CHAR_LENGTH(hd_buy_potential), 5)]]], fields=[hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count])
:                 +- Sort(orderBy=[wr_refunded_cash ASC])
:                    +- Exchange(distribution=[single])
:                       +- Calc(select=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss], where=[(wr_net_loss <= 4.741256237030029E1)])
:                          +- TableSourceScan(table=[[default_catalog, default_database, web_returns, filter=[<=(wr_net_loss, 4.741256237030029E1:DOUBLE)]]], fields=[wr_returned_date_sk, wr_returned_time_sk, wr_item_sk, wr_refunded_customer_sk, wr_refunded_cdemo_sk, wr_refunded_hdemo_sk, wr_refunded_addr_sk, wr_returning_customer_sk, wr_returning_cdemo_sk, wr_returning_hdemo_sk, wr_returning_addr_sk, wr_web_page_sk, wr_reason_sk, wr_order_number, wr_return_quantity, wr_return_amt, wr_return_tax, wr_return_amt_inc_tax, wr_fee, wr_return_ship_cost, wr_refunded_cash, wr_reversed_charge, wr_account_credit, wr_net_loss])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS cr_item_sk_node_24])
:     +- HashAggregate(isMerge=[true], groupBy=[cr_item_sk_node_24], select=[cr_item_sk_node_24, Final_MIN(min$0) AS EXPR$0])
:        +- Exchange(distribution=[hash[cr_item_sk_node_24]])
:           +- LocalHashAggregate(groupBy=[cr_item_sk_node_24], select=[cr_item_sk_node_24, Partial_MIN(cr_item_sk_node_24) AS min$0])
:              +- Calc(select=[cr_returned_date_sk_node_24, cr_returned_time_sk_node_24, cr_item_sk_node_24, cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk_node_24, cr_returning_customer_sk_node_24, cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk_node_24, cr_returning_addr_sk_node_24, cr_call_center_sk_node_24, cr_catalog_page_sk_node_24, cr_ship_mode_sk_node_24, cr_warehouse_sk_node_24, cr_reason_sk_node_24, cr_order_number_node_24, cr_return_quantity_node_24, cr_return_amount_node_24, cr_return_tax_node_24, cr_return_amt_inc_tax_node_24, cr_fee_node_24, cr_return_ship_cost_node_24, cr_refunded_cash_node_24, cr_reversed_charge_node_24, cr_store_credit_node_24, cr_net_loss_node_24, c_first_shipto_date_sk_node_25])
:                 +- HashJoin(joinType=[InnerJoin], where=[(c_first_shipto_date_sk_node_25 = cr_refunded_customer_sk_node_240)], select=[cr_returned_date_sk_node_24, cr_returned_time_sk_node_24, cr_item_sk_node_24, cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk_node_24, cr_returning_customer_sk_node_24, cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk_node_24, cr_returning_addr_sk_node_24, cr_call_center_sk_node_24, cr_catalog_page_sk_node_24, cr_ship_mode_sk_node_24, cr_warehouse_sk_node_24, cr_reason_sk_node_24, cr_order_number_node_24, cr_return_quantity_node_24, cr_return_amount_node_24, cr_return_tax_node_24, cr_return_amt_inc_tax_node_24, cr_fee_node_24, cr_return_ship_cost_node_24, cr_refunded_cash_node_24, cr_reversed_charge_node_24, cr_store_credit_node_24, cr_net_loss_node_24, cr_refunded_customer_sk_node_240, c_first_shipto_date_sk_node_25], isBroadcast=[true], build=[right])
:                    :- Calc(select=[cr_returned_date_sk AS cr_returned_date_sk_node_24, cr_returned_time_sk AS cr_returned_time_sk_node_24, cr_item_sk AS cr_item_sk_node_24, cr_refunded_customer_sk AS cr_refunded_customer_sk_node_24, cr_refunded_cdemo_sk AS cr_refunded_cdemo_sk_node_24, cr_refunded_hdemo_sk AS cr_refunded_hdemo_sk_node_24, cr_refunded_addr_sk AS cr_refunded_addr_sk_node_24, cr_returning_customer_sk AS cr_returning_customer_sk_node_24, cr_returning_cdemo_sk AS cr_returning_cdemo_sk_node_24, cr_returning_hdemo_sk AS cr_returning_hdemo_sk_node_24, cr_returning_addr_sk AS cr_returning_addr_sk_node_24, cr_call_center_sk AS cr_call_center_sk_node_24, cr_catalog_page_sk AS cr_catalog_page_sk_node_24, cr_ship_mode_sk AS cr_ship_mode_sk_node_24, cr_warehouse_sk AS cr_warehouse_sk_node_24, cr_reason_sk AS cr_reason_sk_node_24, cr_order_number AS cr_order_number_node_24, cr_return_quantity AS cr_return_quantity_node_24, cr_return_amount AS cr_return_amount_node_24, cr_return_tax AS cr_return_tax_node_24, cr_return_amt_inc_tax AS cr_return_amt_inc_tax_node_24, cr_fee AS cr_fee_node_24, cr_return_ship_cost AS cr_return_ship_cost_node_24, cr_refunded_cash AS cr_refunded_cash_node_24, cr_reversed_charge AS cr_reversed_charge_node_24, cr_store_credit AS cr_store_credit_node_24, cr_net_loss AS cr_net_loss_node_24, CAST(cr_refunded_customer_sk AS BIGINT) AS cr_refunded_customer_sk_node_240])
:                    :  +- Sort(orderBy=[cr_return_quantity ASC])
:                    :     +- Sort(orderBy=[cr_refunded_cdemo_sk ASC])
:                    :        +- Exchange(distribution=[single])
:                    :           +- TableSourceScan(table=[[default_catalog, default_database, catalog_returns]], fields=[cr_returned_date_sk, cr_returned_time_sk, cr_item_sk, cr_refunded_customer_sk, cr_refunded_cdemo_sk, cr_refunded_hdemo_sk, cr_refunded_addr_sk, cr_returning_customer_sk, cr_returning_cdemo_sk, cr_returning_hdemo_sk, cr_returning_addr_sk, cr_call_center_sk, cr_catalog_page_sk, cr_ship_mode_sk, cr_warehouse_sk, cr_reason_sk, cr_order_number, cr_return_quantity, cr_return_amount, cr_return_tax, cr_return_amt_inc_tax, cr_fee, cr_return_ship_cost, cr_refunded_cash, cr_reversed_charge, cr_store_credit, cr_net_loss])
:                    +- Exchange(distribution=[broadcast])
:                       +- Calc(select=[EXPR$0 AS c_first_shipto_date_sk_node_25])
:                          +- PythonGroupAggregate(groupBy=[c_last_review_date], select=[c_last_review_date, preloaded_udf_agg(c_first_shipto_date_sk) AS EXPR$0])
:                             +- Exchange(distribution=[keep_input_as_is[hash[c_last_review_date]]])
:                                +- Sort(orderBy=[c_last_review_date ASC])
:                                   +- Exchange(distribution=[hash[c_last_review_date]])
:                                      +- Calc(select=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], where=[(CHAR_LENGTH(c_last_review_date) < 5)])
:                                         +- TableSourceScan(table=[[default_catalog, default_database, customer, filter=[<(CHAR_LENGTH(c_last_review_date), 5)]]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
+- Exchange(distribution=[hash[loHfH, ss_sold_time_sk_node_23, ss_item_sk_node_23, ss_customer_sk_node_23, ss_cdemo_sk_node_23, ss_hdemo_sk_node_23, ss_addr_sk_node_23, ss_store_sk_node_23, ss_promo_sk_node_23, ss_ticket_number_node_23, ss_quantity_node_23, ss_wholesale_cost_node_23, ss_list_price_node_23, ss_sales_price_node_23, ss_ext_discount_amt_node_23, ss_ext_sales_price_node_23, ss_ext_wholesale_cost_node_23, ss_ext_list_price_node_23, ss_ext_tax_node_23, ss_coupon_amt_node_23, ss_net_paid_node_23, ss_net_paid_inc_tax_node_23, ss_net_profit_node_23]])
   +- Calc(select=[ss_sold_date_sk AS loHfH, ss_sold_time_sk AS ss_sold_time_sk_node_23, ss_item_sk AS ss_item_sk_node_23, ss_customer_sk AS ss_customer_sk_node_23, ss_cdemo_sk AS ss_cdemo_sk_node_23, ss_hdemo_sk AS ss_hdemo_sk_node_23, ss_addr_sk AS ss_addr_sk_node_23, ss_store_sk AS ss_store_sk_node_23, ss_promo_sk AS ss_promo_sk_node_23, ss_ticket_number AS ss_ticket_number_node_23, ss_quantity AS ss_quantity_node_23, ss_wholesale_cost AS ss_wholesale_cost_node_23, ss_list_price AS ss_list_price_node_23, ss_sales_price AS ss_sales_price_node_23, ss_ext_discount_amt AS ss_ext_discount_amt_node_23, ss_ext_sales_price AS ss_ext_sales_price_node_23, ss_ext_wholesale_cost AS ss_ext_wholesale_cost_node_23, ss_ext_list_price AS ss_ext_list_price_node_23, ss_ext_tax AS ss_ext_tax_node_23, ss_coupon_amt AS ss_coupon_amt_node_23, ss_net_paid AS ss_net_paid_node_23, ss_net_paid_inc_tax AS ss_net_paid_inc_tax_node_23, ss_net_profit AS ss_net_profit_node_23], where=[((ss_cdemo_sk <= 0) AND (ss_customer_sk < 49))])
      +- TableSourceScan(table=[[default_catalog, default_database, store_sales, filter=[and(<=(ss_cdemo_sk, 0), <(ss_customer_sk, 49))]]], fields=[ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk, ss_cdemo_sk, ss_hdemo_sk, ss_addr_sk, ss_store_sk, ss_promo_sk, ss_ticket_number, ss_quantity, ss_wholesale_cost, ss_list_price, ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price, ss_ext_wholesale_cost, ss_ext_list_price, ss_ext_tax, ss_coupon_amt, ss_net_paid, ss_net_paid_inc_tax, ss_net_profit])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o36949605.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#73339068:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[17](input=RelSubset#73339066,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[17]), rel#73339065:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[17](input=RelSubset#73339064,groupBy=cr_item_sk_node_24, cr_refunded_customer_sk_node_240,select=cr_item_sk_node_24, cr_refunded_customer_sk_node_240, Partial_MIN(cr_item_sk_node_24) AS min$0)]
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