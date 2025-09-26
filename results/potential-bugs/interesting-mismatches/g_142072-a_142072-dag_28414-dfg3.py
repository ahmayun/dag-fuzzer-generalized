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
    return values.std()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_12 = table_env.from_path("ship_mode").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("ship_mode").get_schema().get_field_names()])
autonode_17 = table_env.from_path("inventory").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("inventory").get_schema().get_field_names()])
autonode_11 = table_env.from_path("date_dim").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("date_dim").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_15 = table_env.from_path("call_center").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("call_center").get_schema().get_field_names()])
autonode_16 = table_env.from_path("time_dim").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("time_dim").get_schema().get_field_names()])
autonode_10 = autonode_17.select(col('inv_quantity_on_hand_node_17'))
autonode_6 = autonode_11.join(autonode_12, col('sm_type_node_12') == col('d_current_week_node_11'))
autonode_7 = autonode_13.join(autonode_14, col('cd_purchase_estimate_node_14') == col('wp_image_count_node_13'))
autonode_8 = autonode_15.distinct()
autonode_9 = autonode_16.add_columns(lit("hello"))
autonode_3 = autonode_6.distinct()
autonode_4 = autonode_7.join(autonode_8, col('cd_education_status_node_14') == col('cc_division_name_node_15'))
autonode_5 = autonode_9.join(autonode_10, col('t_hour_node_16') == col('inv_quantity_on_hand_node_17'))
autonode_1 = autonode_3.join(autonode_4, col('d_date_sk_node_11') == col('wp_access_date_sk_node_13'))
autonode_2 = autonode_5.add_columns(lit("hello"))
sink = autonode_1.join(autonode_2, col('sm_ship_mode_sk_node_12') == col('inv_quantity_on_hand_node_17'))
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
LogicalJoin(condition=[=($28, $99)], joinType=[inner])
:- LogicalJoin(condition=[=($0, $39)], joinType=[inner])
:  :- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33}])
:  :  +- LogicalJoin(condition=[=($30, $24)], joinType=[inner])
:  :     :- LogicalProject(d_date_sk_node_11=[$0], d_date_id_node_11=[$1], d_date_node_11=[$2], d_month_seq_node_11=[$3], d_week_seq_node_11=[$4], d_quarter_seq_node_11=[$5], d_year_node_11=[$6], d_dow_node_11=[$7], d_moy_node_11=[$8], d_dom_node_11=[$9], d_qoy_node_11=[$10], d_fy_year_node_11=[$11], d_fy_quarter_seq_node_11=[$12], d_fy_week_seq_node_11=[$13], d_day_name_node_11=[$14], d_quarter_name_node_11=[$15], d_holiday_node_11=[$16], d_weekend_node_11=[$17], d_following_holiday_node_11=[$18], d_first_dom_node_11=[$19], d_last_dom_node_11=[$20], d_same_day_ly_node_11=[$21], d_same_day_lq_node_11=[$22], d_current_day_node_11=[$23], d_current_week_node_11=[$24], d_current_month_node_11=[$25], d_current_quarter_node_11=[$26], d_current_year_node_11=[$27])
:  :     :  +- LogicalTableScan(table=[[default_catalog, default_database, date_dim]])
:  :     +- LogicalProject(sm_ship_mode_sk_node_12=[$0], sm_ship_mode_id_node_12=[$1], sm_type_node_12=[$2], sm_code_node_12=[$3], sm_carrier_node_12=[$4], sm_contract_node_12=[$5])
:  :        +- LogicalTableScan(table=[[default_catalog, default_database, ship_mode]])
:  +- LogicalJoin(condition=[=($17, $40)], joinType=[inner])
:     :- LogicalJoin(condition=[=($18, $12)], joinType=[inner])
:     :  :- LogicalProject(wp_web_page_sk_node_13=[$0], wp_web_page_id_node_13=[$1], wp_rec_start_date_node_13=[$2], wp_rec_end_date_node_13=[$3], wp_creation_date_sk_node_13=[$4], wp_access_date_sk_node_13=[$5], wp_autogen_flag_node_13=[$6], wp_customer_sk_node_13=[$7], wp_url_node_13=[$8], wp_type_node_13=[$9], wp_char_count_node_13=[$10], wp_link_count_node_13=[$11], wp_image_count_node_13=[$12], wp_max_ad_count_node_13=[$13])
:     :  :  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])
:     :  +- LogicalProject(cd_demo_sk_node_14=[$0], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8])
:     :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
:     +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30}])
:        +- LogicalProject(cc_call_center_sk_node_15=[$0], cc_call_center_id_node_15=[$1], cc_rec_start_date_node_15=[$2], cc_rec_end_date_node_15=[$3], cc_closed_date_sk_node_15=[$4], cc_open_date_sk_node_15=[$5], cc_name_node_15=[$6], cc_class_node_15=[$7], cc_employees_node_15=[$8], cc_sq_ft_node_15=[$9], cc_hours_node_15=[$10], cc_manager_node_15=[$11], cc_mkt_id_node_15=[$12], cc_mkt_class_node_15=[$13], cc_mkt_desc_node_15=[$14], cc_market_manager_node_15=[$15], cc_division_node_15=[$16], cc_division_name_node_15=[$17], cc_company_node_15=[$18], cc_company_name_node_15=[$19], cc_street_number_node_15=[$20], cc_street_name_node_15=[$21], cc_street_type_node_15=[$22], cc_suite_number_node_15=[$23], cc_city_node_15=[$24], cc_county_node_15=[$25], cc_state_node_15=[$26], cc_zip_node_15=[$27], cc_country_node_15=[$28], cc_gmt_offset_node_15=[$29], cc_tax_percentage_node_15=[$30])
:           +- LogicalTableScan(table=[[default_catalog, default_database, call_center]])
+- LogicalProject(t_time_sk_node_16=[$0], t_time_id_node_16=[$1], t_time_node_16=[$2], t_hour_node_16=[$3], t_minute_node_16=[$4], t_second_node_16=[$5], t_am_pm_node_16=[$6], t_shift_node_16=[$7], t_sub_shift_node_16=[$8], t_meal_time_node_16=[$9], _c10=[$10], inv_quantity_on_hand_node_17=[$11], _c12=[_UTF-16LE'hello'])
   +- LogicalJoin(condition=[=($3, $11)], joinType=[inner])
      :- LogicalProject(t_time_sk_node_16=[$0], t_time_id_node_16=[$1], t_time_node_16=[$2], t_hour_node_16=[$3], t_minute_node_16=[$4], t_second_node_16=[$5], t_am_pm_node_16=[$6], t_shift_node_16=[$7], t_sub_shift_node_16=[$8], t_meal_time_node_16=[$9], _c10=[_UTF-16LE'hello'])
      :  +- LogicalTableScan(table=[[default_catalog, default_database, time_dim]])
      +- LogicalProject(inv_quantity_on_hand_node_17=[$3])
         +- LogicalTableScan(table=[[default_catalog, default_database, inventory]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(sm_ship_mode_sk, inv_quantity_on_hand_node_17)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, t_time_sk_node_16, t_time_id_node_16, t_time_node_16, t_hour_node_16, t_minute_node_16, t_second_node_16, t_am_pm_node_16, t_shift_node_16, t_sub_shift_node_16, t_meal_time_node_16, _c10, inv_quantity_on_hand_node_17, _c12], build=[left])
:- Exchange(distribution=[hash[sm_ship_mode_sk]])
:  +- HashJoin(joinType=[InnerJoin], where=[=(d_date_sk, wp_access_date_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], build=[left])
:     :- Exchange(distribution=[hash[d_date_sk]])
:     :  +- HashAggregate(isMerge=[false], groupBy=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:     :     +- Exchange(distribution=[hash[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
:     :        +- HashJoin(joinType=[InnerJoin], where=[=(sm_type, d_current_week)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])
:     :           :- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
:     :           +- Exchange(distribution=[broadcast])
:     :              +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:     +- Exchange(distribution=[hash[wp_access_date_sk]])
:        +- NestedLoopJoin(joinType=[InnerJoin], where=[=(cd_education_status, cc_division_name)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], build=[right])
:           :- HashJoin(joinType=[InnerJoin], where=[=(cd_purchase_estimate, wp_image_count)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])
:           :  :- Exchange(distribution=[broadcast])
:           :  :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
:           :  +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
:           +- Exchange(distribution=[broadcast])
:              +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
:                 +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC])
:                    +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage]])
:                       +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
+- Calc(select=[t_time_sk AS t_time_sk_node_16, t_time_id AS t_time_id_node_16, t_time AS t_time_node_16, t_hour AS t_hour_node_16, t_minute AS t_minute_node_16, t_second AS t_second_node_16, t_am_pm AS t_am_pm_node_16, t_shift AS t_shift_node_16, t_sub_shift AS t_sub_shift_node_16, t_meal_time AS t_meal_time_node_16, 'hello' AS _c10, inv_quantity_on_hand AS inv_quantity_on_hand_node_17, 'hello' AS _c12])
   +- HashJoin(joinType=[InnerJoin], where=[=(t_hour, inv_quantity_on_hand)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, inv_quantity_on_hand], build=[left])
      :- Exchange(distribution=[hash[t_hour]])
      :  +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
      +- Exchange(distribution=[hash[inv_quantity_on_hand]])
         +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_quantity_on_hand], metadata=[]]], fields=[inv_quantity_on_hand])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sm_ship_mode_sk = inv_quantity_on_hand_node_17)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage, t_time_sk_node_16, t_time_id_node_16, t_time_node_16, t_hour_node_16, t_minute_node_16, t_second_node_16, t_am_pm_node_16, t_shift_node_16, t_sub_shift_node_16, t_meal_time_node_16, _c10, inv_quantity_on_hand_node_17, _c12], build=[left])\
:- [#1] Exchange(distribution=[hash[sm_ship_mode_sk]])\
+- Calc(select=[t_time_sk AS t_time_sk_node_16, t_time_id AS t_time_id_node_16, t_time AS t_time_node_16, t_hour AS t_hour_node_16, t_minute AS t_minute_node_16, t_second AS t_second_node_16, t_am_pm AS t_am_pm_node_16, t_shift AS t_shift_node_16, t_sub_shift AS t_sub_shift_node_16, t_meal_time AS t_meal_time_node_16, 'hello' AS _c10, inv_quantity_on_hand AS inv_quantity_on_hand_node_17, 'hello' AS _c12])\
   +- HashJoin(joinType=[InnerJoin], where=[(t_hour = inv_quantity_on_hand)], select=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time, inv_quantity_on_hand], build=[left])\
      :- [#2] Exchange(distribution=[hash[t_hour]])\
      +- [#3] Exchange(distribution=[hash[inv_quantity_on_hand]])\
])
:- Exchange(distribution=[hash[sm_ship_mode_sk]])
:  +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(d_date_sk = wp_access_date_sk)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract, wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], build=[left])
:     :- Exchange(distribution=[hash[d_date_sk]])
:     :  +- HashAggregate(isMerge=[false], groupBy=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:     :     +- Exchange(distribution=[hash[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract]])
:     :        +- MultipleInput(readOrder=[1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(sm_type = d_current_week)], select=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year, sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract], isBroadcast=[true], build=[right])\
:- [#1] TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])\
+- [#2] Exchange(distribution=[broadcast])\
])
:     :           :- TableSourceScan(table=[[default_catalog, default_database, date_dim]], fields=[d_date_sk, d_date_id, d_date, d_month_seq, d_week_seq, d_quarter_seq, d_year, d_dow, d_moy, d_dom, d_qoy, d_fy_year, d_fy_quarter_seq, d_fy_week_seq, d_day_name, d_quarter_name, d_holiday, d_weekend, d_following_holiday, d_first_dom, d_last_dom, d_same_day_ly, d_same_day_lq, d_current_day, d_current_week, d_current_month, d_current_quarter, d_current_year])
:     :           +- Exchange(distribution=[broadcast])
:     :              +- TableSourceScan(table=[[default_catalog, default_database, ship_mode]], fields=[sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract])
:     +- Exchange(distribution=[hash[wp_access_date_sk]])
:        +- MultipleInput(readOrder=[0,0,1], members=[\
NestedLoopJoin(joinType=[InnerJoin], where=[(cd_education_status = cc_division_name)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count, cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], build=[right])\
:- HashJoin(joinType=[InnerJoin], where=[(cd_purchase_estimate = wp_image_count)], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], isBroadcast=[true], build=[left])\
:  :- [#2] Exchange(distribution=[broadcast])\
:  +- [#3] TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])\
+- [#1] Exchange(distribution=[broadcast])\
])
:           :- Exchange(distribution=[broadcast])
:           :  +- SortAggregate(isMerge=[false], groupBy=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage], select=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
:           :     +- Exchange(distribution=[forward])
:           :        +- Sort(orderBy=[cc_call_center_sk ASC, cc_call_center_id ASC, cc_rec_start_date ASC, cc_rec_end_date ASC, cc_closed_date_sk ASC, cc_open_date_sk ASC, cc_name ASC, cc_class ASC, cc_employees ASC, cc_sq_ft ASC, cc_hours ASC, cc_manager ASC, cc_mkt_id ASC, cc_mkt_class ASC, cc_mkt_desc ASC, cc_market_manager ASC, cc_division ASC, cc_division_name ASC, cc_company ASC, cc_company_name ASC, cc_street_number ASC, cc_street_name ASC, cc_street_type ASC, cc_suite_number ASC, cc_city ASC, cc_county ASC, cc_state ASC, cc_zip ASC, cc_country ASC, cc_gmt_offset ASC, cc_tax_percentage ASC])
:           :           +- Exchange(distribution=[hash[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage]])
:           :              +- TableSourceScan(table=[[default_catalog, default_database, call_center]], fields=[cc_call_center_sk, cc_call_center_id, cc_rec_start_date, cc_rec_end_date, cc_closed_date_sk, cc_open_date_sk, cc_name, cc_class, cc_employees, cc_sq_ft, cc_hours, cc_manager, cc_mkt_id, cc_mkt_class, cc_mkt_desc, cc_market_manager, cc_division, cc_division_name, cc_company, cc_company_name, cc_street_number, cc_street_name, cc_street_type, cc_suite_number, cc_city, cc_county, cc_state, cc_zip, cc_country, cc_gmt_offset, cc_tax_percentage])
:           :- Exchange(distribution=[broadcast])
:           :  +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
:           +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
:- Exchange(distribution=[hash[t_hour]])
:  +- TableSourceScan(table=[[default_catalog, default_database, time_dim]], fields=[t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time])
+- Exchange(distribution=[hash[inv_quantity_on_hand]])
   +- TableSourceScan(table=[[default_catalog, default_database, inventory, project=[inv_quantity_on_hand], metadata=[]]], fields=[inv_quantity_on_hand])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "IndexOutOfBoundsException",
      "error_message": "An error occurred while calling o77465955.explain.
: java.lang.IndexOutOfBoundsException: Index 11 out of bounds for length 11
\tat java.base/jdk.internal.util.Preconditions.outOfBounds(Preconditions.java:64)
\tat java.base/jdk.internal.util.Preconditions.outOfBoundsCheckIndex(Preconditions.java:70)
\tat java.base/jdk.internal.util.Preconditions.checkIndex(Preconditions.java:248)
\tat java.base/java.util.Objects.checkIndex(Objects.java:374)
\tat java.base/java.util.ArrayList.get(ArrayList.java:459)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.lambda$createHashPartitioner$2(BatchExecExchange.java:241)
\tat java.base/java.util.stream.IntPipeline$1$1.accept(IntPipeline.java:180)
\tat java.base/java.util.Spliterators$IntArraySpliterator.forEachRemaining(Spliterators.java:1032)
\tat java.base/java.util.Spliterator$OfInt.forEachRemaining(Spliterator.java:699)
\tat java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:484)
\tat java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:474)
\tat java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:550)
\tat java.base/java.util.stream.AbstractPipeline.evaluateToArrayNode(AbstractPipeline.java:260)
\tat java.base/java.util.stream.ReferencePipeline.toArray(ReferencePipeline.java:517)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.createHashPartitioner(BatchExecExchange.java:242)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:209)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc.translateToPlanInternal(CommonExecCalc.java:94)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange.translateToPlanInternal(BatchExecExchange.java:161)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecEdge.translateToPlan(ExecEdge.java:262)
\tat org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecHashJoin.translateToPlanInternal(BatchExecHashJoin.java:193)
\tat org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase.translateToPlan(ExecNodeBase.java:168)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.$anonfun$translateToPlan$1(BatchPlanner.scala:95)
\tat scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
\tat scala.collection.Iterator.foreach(Iterator.scala:943)
\tat scala.collection.Iterator.foreach$(Iterator.scala:943)
\tat scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
\tat scala.collection.IterableLike.foreach(IterableLike.scala:74)
\tat scala.collection.IterableLike.foreach$(IterableLike.scala:73)
\tat scala.collection.AbstractIterable.foreach(Iterable.scala:56)
\tat scala.collection.TraversableLike.map(TraversableLike.scala:286)
\tat scala.collection.TraversableLike.map$(TraversableLike.scala:279)
\tat scala.collection.AbstractTraversable.map(Traversable.scala:108)
\tat org.apache.flink.table.planner.delegation.BatchPlanner.translateToPlan(BatchPlanner.scala:94)
\tat org.apache.flink.table.planner.delegation.PlannerBase.getExplainGraphs(PlannerBase.scala:627)
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
",
      "stdout": "",
      "stderr": ""
    }
  }
}
"""



//Optimizer Branch Coverage: 0