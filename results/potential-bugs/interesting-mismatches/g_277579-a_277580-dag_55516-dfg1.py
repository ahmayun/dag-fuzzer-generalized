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
    return values.sum()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("store_sales").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("store_sales").get_schema().get_field_names()])
autonode_17 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer").get_schema().get_field_names()])
autonode_15 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_16 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_9 = autonode_13.group_by(col('ss_store_sk_node_13')).select(col('ss_ticket_number_node_13').max.alias('ss_ticket_number_node_13'))
autonode_10 = autonode_14.order_by(col('c_birth_month_node_14'))
autonode_11 = autonode_15.add_columns(lit("hello"))
autonode_12 = autonode_16.join(autonode_17, col('p_start_date_sk_node_17') == col('ib_lower_bound_node_16'))
autonode_7 = autonode_9.join(autonode_10, col('ss_ticket_number_node_13') == col('c_customer_sk_node_14'))
autonode_8 = autonode_11.join(autonode_12, col('p_cost_node_17') == col('p_cost_node_15'))
autonode_5 = autonode_7.group_by(col('c_preferred_cust_flag_node_14')).select(col('c_birth_day_node_14').min.alias('c_birth_day_node_14'))
autonode_6 = autonode_8.order_by(col('p_channel_press_node_17'))
autonode_3 = autonode_5.limit(27)
autonode_4 = autonode_6.add_columns(lit("hello"))
autonode_2 = autonode_3.join(autonode_4, col('p_end_date_sk_node_17') == col('c_birth_day_node_14'))
autonode_1 = autonode_2.group_by(col('p_channel_email_node_15')).select(call('preloaded_udf_agg', col('p_promo_name_node_17')).alias('p_promo_name_node_17'))
sink = autonode_1.select(col('p_promo_name_node_17'))
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
LogicalProject(p_promo_name_node_17=[$1])
+- LogicalAggregate(group=[{10}], EXPR$0=[preloaded_udf_agg($31)])
   +- LogicalJoin(condition=[=($27, $0)], joinType=[inner])
      :- LogicalSort(fetch=[27])
      :  +- LogicalProject(c_birth_day_node_14=[$1])
      :     +- LogicalAggregate(group=[{11}], EXPR$0=[MIN($12)])
      :        +- LogicalJoin(condition=[=($0, $1)], joinType=[inner])
      :           :- LogicalProject(ss_ticket_number_node_13=[$1])
      :           :  +- LogicalAggregate(group=[{0}], EXPR$0=[MAX($1)])
      :           :     +- LogicalProject(ss_store_sk_node_13=[$7], ss_ticket_number_node_13=[$9])
      :           :        +- LogicalTableScan(table=[[default_catalog, default_database, store_sales]])
      :           +- LogicalSort(sort0=[$12], dir0=[ASC])
      :              +- LogicalProject(c_customer_sk_node_14=[$0], c_customer_id_node_14=[$1], c_current_cdemo_sk_node_14=[$2], c_current_hdemo_sk_node_14=[$3], c_current_addr_sk_node_14=[$4], c_first_shipto_date_sk_node_14=[$5], c_first_sales_date_sk_node_14=[$6], c_salutation_node_14=[$7], c_first_name_node_14=[$8], c_last_name_node_14=[$9], c_preferred_cust_flag_node_14=[$10], c_birth_day_node_14=[$11], c_birth_month_node_14=[$12], c_birth_year_node_14=[$13], c_birth_country_node_14=[$14], c_login_node_14=[$15], c_email_address_node_14=[$16], c_last_review_date_node_14=[$17])
      :                 +- LogicalTableScan(table=[[default_catalog, default_database, customer]])
      +- LogicalProject(p_promo_sk_node_15=[$0], p_promo_id_node_15=[$1], p_start_date_sk_node_15=[$2], p_end_date_sk_node_15=[$3], p_item_sk_node_15=[$4], p_cost_node_15=[$5], p_response_target_node_15=[$6], p_promo_name_node_15=[$7], p_channel_dmail_node_15=[$8], p_channel_email_node_15=[$9], p_channel_catalog_node_15=[$10], p_channel_tv_node_15=[$11], p_channel_radio_node_15=[$12], p_channel_press_node_15=[$13], p_channel_event_node_15=[$14], p_channel_demo_node_15=[$15], p_channel_details_node_15=[$16], p_purpose_node_15=[$17], p_discount_active_node_15=[$18], _c19=[$19], ib_income_band_sk_node_16=[$20], ib_lower_bound_node_16=[$21], ib_upper_bound_node_16=[$22], p_promo_sk_node_17=[$23], p_promo_id_node_17=[$24], p_start_date_sk_node_17=[$25], p_end_date_sk_node_17=[$26], p_item_sk_node_17=[$27], p_cost_node_17=[$28], p_response_target_node_17=[$29], p_promo_name_node_17=[$30], p_channel_dmail_node_17=[$31], p_channel_email_node_17=[$32], p_channel_catalog_node_17=[$33], p_channel_tv_node_17=[$34], p_channel_radio_node_17=[$35], p_channel_press_node_17=[$36], p_channel_event_node_17=[$37], p_channel_demo_node_17=[$38], p_channel_details_node_17=[$39], p_purpose_node_17=[$40], p_discount_active_node_17=[$41], _c42=[_UTF-16LE'hello'])
         +- LogicalSort(sort0=[$36], dir0=[ASC])
            +- LogicalJoin(condition=[=($28, $5)], joinType=[inner])
               :- LogicalProject(p_promo_sk_node_15=[$0], p_promo_id_node_15=[$1], p_start_date_sk_node_15=[$2], p_end_date_sk_node_15=[$3], p_item_sk_node_15=[$4], p_cost_node_15=[$5], p_response_target_node_15=[$6], p_promo_name_node_15=[$7], p_channel_dmail_node_15=[$8], p_channel_email_node_15=[$9], p_channel_catalog_node_15=[$10], p_channel_tv_node_15=[$11], p_channel_radio_node_15=[$12], p_channel_press_node_15=[$13], p_channel_event_node_15=[$14], p_channel_demo_node_15=[$15], p_channel_details_node_15=[$16], p_purpose_node_15=[$17], p_discount_active_node_15=[$18], _c19=[_UTF-16LE'hello'])
               :  +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
               +- LogicalJoin(condition=[=($5, $1)], joinType=[inner])
                  :- LogicalProject(ib_income_band_sk_node_16=[$0], ib_lower_bound_node_16=[$1], ib_upper_bound_node_16=[$2])
                  :  +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
                  +- LogicalProject(p_promo_sk_node_17=[$0], p_promo_id_node_17=[$1], p_start_date_sk_node_17=[$2], p_end_date_sk_node_17=[$3], p_item_sk_node_17=[$4], p_cost_node_17=[$5], p_response_target_node_17=[$6], p_promo_name_node_17=[$7], p_channel_dmail_node_17=[$8], p_channel_email_node_17=[$9], p_channel_catalog_node_17=[$10], p_channel_tv_node_17=[$11], p_channel_radio_node_17=[$12], p_channel_press_node_17=[$13], p_channel_event_node_17=[$14], p_channel_demo_node_17=[$15], p_channel_details_node_17=[$16], p_purpose_node_17=[$17], p_discount_active_node_17=[$18])
                     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_promo_name_node_17])
+- PythonGroupAggregate(groupBy=[p_channel_email_node_15], select=[p_channel_email_node_15, preloaded_udf_agg(p_promo_name_node_17) AS EXPR$0])
   +- Sort(orderBy=[p_channel_email_node_15 ASC])
      +- Exchange(distribution=[hash[p_channel_email_node_15]])
         +- HashJoin(joinType=[InnerJoin], where=[=(p_end_date_sk_node_17, c_birth_day_node_14)], select=[c_birth_day_node_14, p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, _c19, ib_income_band_sk_node_16, ib_lower_bound_node_16, ib_upper_bound_node_16, p_promo_sk_node_17, p_promo_id_node_17, p_start_date_sk_node_17, p_end_date_sk_node_17, p_item_sk_node_17, p_cost_node_17, p_response_target_node_17, p_promo_name_node_17, p_channel_dmail_node_17, p_channel_email_node_17, p_channel_catalog_node_17, p_channel_tv_node_17, p_channel_radio_node_17, p_channel_press_node_17, p_channel_event_node_17, p_channel_demo_node_17, p_channel_details_node_17, p_purpose_node_17, p_discount_active_node_17, _c42], isBroadcast=[true], build=[left])
            :- Exchange(distribution=[broadcast])
            :  +- Calc(select=[EXPR$0 AS c_birth_day_node_14])
            :     +- Limit(offset=[0], fetch=[27], global=[true])
            :        +- Exchange(distribution=[single])
            :           +- Limit(offset=[0], fetch=[27], global=[false])
            :              +- HashAggregate(isMerge=[true], groupBy=[c_preferred_cust_flag], select=[c_preferred_cust_flag, Final_MIN(min$0) AS EXPR$0])
            :                 +- Exchange(distribution=[hash[c_preferred_cust_flag]])
            :                    +- LocalHashAggregate(groupBy=[c_preferred_cust_flag], select=[c_preferred_cust_flag, Partial_MIN(c_birth_day) AS min$0])
            :                       +- NestedLoopJoin(joinType=[InnerJoin], where=[=(ss_ticket_number_node_13, c_customer_sk)], select=[ss_ticket_number_node_13, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
            :                          :- Exchange(distribution=[broadcast])
            :                          :  +- Calc(select=[EXPR$0 AS ss_ticket_number_node_13])
            :                          :     +- HashAggregate(isMerge=[true], groupBy=[ss_store_sk], select=[ss_store_sk, Final_MAX(max$0) AS EXPR$0])
            :                          :        +- Exchange(distribution=[hash[ss_store_sk]])
            :                          :           +- LocalHashAggregate(groupBy=[ss_store_sk], select=[ss_store_sk, Partial_MAX(ss_ticket_number) AS max$0])
            :                          :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_store_sk, ss_ticket_number], metadata=[]]], fields=[ss_store_sk, ss_ticket_number])
            :                          +- Sort(orderBy=[c_birth_month ASC])
            :                             +- Exchange(distribution=[single])
            :                                +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
            +- Calc(select=[p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, 'hello' AS _c19, ib_income_band_sk AS ib_income_band_sk_node_16, ib_lower_bound AS ib_lower_bound_node_16, ib_upper_bound AS ib_upper_bound_node_16, p_promo_sk AS p_promo_sk_node_17, p_promo_id AS p_promo_id_node_17, p_start_date_sk AS p_start_date_sk_node_17, p_end_date_sk AS p_end_date_sk_node_17, p_item_sk AS p_item_sk_node_17, p_cost AS p_cost_node_17, p_response_target AS p_response_target_node_17, p_promo_name AS p_promo_name_node_17, p_channel_dmail AS p_channel_dmail_node_17, p_channel_email AS p_channel_email_node_17, p_channel_catalog AS p_channel_catalog_node_17, p_channel_tv AS p_channel_tv_node_17, p_channel_radio AS p_channel_radio_node_17, p_channel_press AS p_channel_press_node_17, p_channel_event AS p_channel_event_node_17, p_channel_demo AS p_channel_demo_node_17, p_channel_details AS p_channel_details_node_17, p_purpose AS p_purpose_node_17, p_discount_active AS p_discount_active_node_17, 'hello' AS _c42])
               +- Sort(orderBy=[p_channel_press ASC])
                  +- Exchange(distribution=[single])
                     +- HashJoin(joinType=[InnerJoin], where=[=(p_cost, p_cost_node_15)], select=[p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, _c19, ib_income_band_sk, ib_lower_bound, ib_upper_bound, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])
                        :- Calc(select=[p_promo_sk AS p_promo_sk_node_15, p_promo_id AS p_promo_id_node_15, p_start_date_sk AS p_start_date_sk_node_15, p_end_date_sk AS p_end_date_sk_node_15, p_item_sk AS p_item_sk_node_15, p_cost AS p_cost_node_15, p_response_target AS p_response_target_node_15, p_promo_name AS p_promo_name_node_15, p_channel_dmail AS p_channel_dmail_node_15, p_channel_email AS p_channel_email_node_15, p_channel_catalog AS p_channel_catalog_node_15, p_channel_tv AS p_channel_tv_node_15, p_channel_radio AS p_channel_radio_node_15, p_channel_press AS p_channel_press_node_15, p_channel_event AS p_channel_event_node_15, p_channel_demo AS p_channel_demo_node_15, p_channel_details AS p_channel_details_node_15, p_purpose AS p_purpose_node_15, p_discount_active AS p_discount_active_node_15, 'hello' AS _c19])
                        :  +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                        +- Exchange(distribution=[broadcast])
                           +- HashJoin(joinType=[InnerJoin], where=[=(p_start_date_sk, ib_lower_bound)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[left])
                              :- Exchange(distribution=[broadcast])
                              :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
                              +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_promo_name_node_17])
+- PythonGroupAggregate(groupBy=[p_channel_email_node_15], select=[p_channel_email_node_15, preloaded_udf_agg(p_promo_name_node_17) AS EXPR$0])
   +- Exchange(distribution=[keep_input_as_is[hash[p_channel_email_node_15]]])
      +- Sort(orderBy=[p_channel_email_node_15 ASC])
         +- Exchange(distribution=[hash[p_channel_email_node_15]])
            +- HashJoin(joinType=[InnerJoin], where=[(p_end_date_sk_node_17 = c_birth_day_node_14)], select=[c_birth_day_node_14, p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, _c19, ib_income_band_sk_node_16, ib_lower_bound_node_16, ib_upper_bound_node_16, p_promo_sk_node_17, p_promo_id_node_17, p_start_date_sk_node_17, p_end_date_sk_node_17, p_item_sk_node_17, p_cost_node_17, p_response_target_node_17, p_promo_name_node_17, p_channel_dmail_node_17, p_channel_email_node_17, p_channel_catalog_node_17, p_channel_tv_node_17, p_channel_radio_node_17, p_channel_press_node_17, p_channel_event_node_17, p_channel_demo_node_17, p_channel_details_node_17, p_purpose_node_17, p_discount_active_node_17, _c42], isBroadcast=[true], build=[left])
               :- Exchange(distribution=[broadcast])
               :  +- Calc(select=[EXPR$0 AS c_birth_day_node_14])
               :     +- Limit(offset=[0], fetch=[27], global=[true])
               :        +- Exchange(distribution=[single])
               :           +- Limit(offset=[0], fetch=[27], global=[false])
               :              +- HashAggregate(isMerge=[true], groupBy=[c_preferred_cust_flag], select=[c_preferred_cust_flag, Final_MIN(min$0) AS EXPR$0])
               :                 +- Exchange(distribution=[hash[c_preferred_cust_flag]])
               :                    +- LocalHashAggregate(groupBy=[c_preferred_cust_flag], select=[c_preferred_cust_flag, Partial_MIN(c_birth_day) AS min$0])
               :                       +- NestedLoopJoin(joinType=[InnerJoin], where=[(ss_ticket_number_node_13 = c_customer_sk)], select=[ss_ticket_number_node_13, c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date], build=[left])
               :                          :- Exchange(distribution=[broadcast])
               :                          :  +- Calc(select=[EXPR$0 AS ss_ticket_number_node_13])
               :                          :     +- HashAggregate(isMerge=[true], groupBy=[ss_store_sk], select=[ss_store_sk, Final_MAX(max$0) AS EXPR$0])
               :                          :        +- Exchange(distribution=[hash[ss_store_sk]])
               :                          :           +- LocalHashAggregate(groupBy=[ss_store_sk], select=[ss_store_sk, Partial_MAX(ss_ticket_number) AS max$0])
               :                          :              +- TableSourceScan(table=[[default_catalog, default_database, store_sales, project=[ss_store_sk, ss_ticket_number], metadata=[]]], fields=[ss_store_sk, ss_ticket_number])
               :                          +- Sort(orderBy=[c_birth_month ASC])
               :                             +- Exchange(distribution=[single])
               :                                +- TableSourceScan(table=[[default_catalog, default_database, customer]], fields=[c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk, c_current_addr_sk, c_first_shipto_date_sk, c_first_sales_date_sk, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date])
               +- Calc(select=[p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, 'hello' AS _c19, ib_income_band_sk AS ib_income_band_sk_node_16, ib_lower_bound AS ib_lower_bound_node_16, ib_upper_bound AS ib_upper_bound_node_16, p_promo_sk AS p_promo_sk_node_17, p_promo_id AS p_promo_id_node_17, p_start_date_sk AS p_start_date_sk_node_17, p_end_date_sk AS p_end_date_sk_node_17, p_item_sk AS p_item_sk_node_17, p_cost AS p_cost_node_17, p_response_target AS p_response_target_node_17, p_promo_name AS p_promo_name_node_17, p_channel_dmail AS p_channel_dmail_node_17, p_channel_email AS p_channel_email_node_17, p_channel_catalog AS p_channel_catalog_node_17, p_channel_tv AS p_channel_tv_node_17, p_channel_radio AS p_channel_radio_node_17, p_channel_press AS p_channel_press_node_17, p_channel_event AS p_channel_event_node_17, p_channel_demo AS p_channel_demo_node_17, p_channel_details AS p_channel_details_node_17, p_purpose AS p_purpose_node_17, p_discount_active AS p_discount_active_node_17, 'hello' AS _c42])
                  +- Sort(orderBy=[p_channel_press ASC])
                     +- Exchange(distribution=[single])
                        +- HashJoin(joinType=[InnerJoin], where=[(p_cost = p_cost_node_15)], select=[p_promo_sk_node_15, p_promo_id_node_15, p_start_date_sk_node_15, p_end_date_sk_node_15, p_item_sk_node_15, p_cost_node_15, p_response_target_node_15, p_promo_name_node_15, p_channel_dmail_node_15, p_channel_email_node_15, p_channel_catalog_node_15, p_channel_tv_node_15, p_channel_radio_node_15, p_channel_press_node_15, p_channel_event_node_15, p_channel_demo_node_15, p_channel_details_node_15, p_purpose_node_15, p_discount_active_node_15, _c19, ib_income_band_sk, ib_lower_bound, ib_upper_bound, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[right])
                           :- Exchange(distribution=[any], shuffle_mode=[BATCH])
                           :  +- Calc(select=[p_promo_sk AS p_promo_sk_node_15, p_promo_id AS p_promo_id_node_15, p_start_date_sk AS p_start_date_sk_node_15, p_end_date_sk AS p_end_date_sk_node_15, p_item_sk AS p_item_sk_node_15, p_cost AS p_cost_node_15, p_response_target AS p_response_target_node_15, p_promo_name AS p_promo_name_node_15, p_channel_dmail AS p_channel_dmail_node_15, p_channel_email AS p_channel_email_node_15, p_channel_catalog AS p_channel_catalog_node_15, p_channel_tv AS p_channel_tv_node_15, p_channel_radio AS p_channel_radio_node_15, p_channel_press AS p_channel_press_node_15, p_channel_event AS p_channel_event_node_15, p_channel_demo AS p_channel_demo_node_15, p_channel_details AS p_channel_details_node_15, p_purpose AS p_purpose_node_15, p_discount_active AS p_discount_active_node_15, 'hello' AS _c19])
                           :     +- TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])(reuse_id=[1])
                           +- Exchange(distribution=[broadcast])
                              +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(p_start_date_sk = ib_lower_bound)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, promotion]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])\
])
                                 :- Exchange(distribution=[broadcast])
                                 :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
                                 +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o151099073.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#305191803:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[12](input=RelSubset#305191801,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[12]), rel#305191800:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[12](input=RelSubset#305191799,groupBy=c_customer_sk, c_preferred_cust_flag,select=c_customer_sk, c_preferred_cust_flag, Partial_MIN(c_birth_day) AS min$0)]
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