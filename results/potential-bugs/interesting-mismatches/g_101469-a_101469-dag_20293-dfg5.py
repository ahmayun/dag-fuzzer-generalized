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
    return values.mean()


try:
    table_env.drop_temporary_function("preloaded_udf_agg")
except:
    pass

preloaded_udf_agg = udaf(preloaded_aggregation, result_type=DataTypes.BIGINT(), func_type="pandas")

table_env.create_temporary_function("preloaded_udf_agg", preloaded_udf_agg)

autonode_13 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_12 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_12") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_11 = table_env.from_path("item").select(*[col(column_name).alias(f"{column_name}_node_11") for column_name in table_env.from_path("item").get_schema().get_field_names()])
autonode_14 = table_env.from_path("promotion").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("promotion").get_schema().get_field_names()])
autonode_15 = table_env.from_path("web_page").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("web_page").get_schema().get_field_names()])
autonode_8 = autonode_13.order_by(col('p_purpose_node_13'))
autonode_7 = autonode_11.join(autonode_12, col('cd_gender_node_12') == col('i_color_node_11'))
autonode_9 = autonode_14.select(col('p_start_date_sk_node_14'))
autonode_10 = autonode_15.distinct()
autonode_4 = autonode_7.order_by(col('i_formulation_node_11'))
autonode_5 = autonode_8.join(autonode_9, col('p_start_date_sk_node_14') == col('p_end_date_sk_node_13'))
autonode_6 = autonode_10.alias('Axptz')
autonode_2 = autonode_4.filter(col('i_manager_id_node_11') >= -7)
autonode_3 = autonode_5.join(autonode_6, col('wp_web_page_id_node_15') == col('p_channel_radio_node_13'))
autonode_1 = autonode_2.join(autonode_3, col('p_cost_node_13') == col('i_wholesale_cost_node_11'))
sink = autonode_1.group_by(col('cd_dep_employed_count_node_12')).select(col('p_end_date_sk_node_13').min.alias('p_end_date_sk_node_13'))
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
LogicalProject(p_end_date_sk_node_13=[$1])
+- LogicalAggregate(group=[{29}], EXPR$0=[MIN($34)])
   +- LogicalJoin(condition=[=($36, $6)], joinType=[inner])
      :- LogicalFilter(condition=[>=($20, -7)])
      :  +- LogicalSort(sort0=[$16], dir0=[ASC])
      :     +- LogicalJoin(condition=[=($23, $17)], joinType=[inner])
      :        :- LogicalProject(i_item_sk_node_11=[$0], i_item_id_node_11=[$1], i_rec_start_date_node_11=[$2], i_rec_end_date_node_11=[$3], i_item_desc_node_11=[$4], i_current_price_node_11=[$5], i_wholesale_cost_node_11=[$6], i_brand_id_node_11=[$7], i_brand_node_11=[$8], i_class_id_node_11=[$9], i_class_node_11=[$10], i_category_id_node_11=[$11], i_category_node_11=[$12], i_manufact_id_node_11=[$13], i_manufact_node_11=[$14], i_size_node_11=[$15], i_formulation_node_11=[$16], i_color_node_11=[$17], i_units_node_11=[$18], i_container_node_11=[$19], i_manager_id_node_11=[$20], i_product_name_node_11=[$21])
      :        :  +- LogicalTableScan(table=[[default_catalog, default_database, item]])
      :        +- LogicalProject(cd_demo_sk_node_12=[$0], cd_gender_node_12=[$1], cd_marital_status_node_12=[$2], cd_education_status_node_12=[$3], cd_purchase_estimate_node_12=[$4], cd_credit_rating_node_12=[$5], cd_dep_count_node_12=[$6], cd_dep_employed_count_node_12=[$7], cd_dep_college_count_node_12=[$8])
      :           +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
      +- LogicalJoin(condition=[=($21, $12)], joinType=[inner])
         :- LogicalJoin(condition=[=($19, $3)], joinType=[inner])
         :  :- LogicalSort(sort0=[$17], dir0=[ASC])
         :  :  +- LogicalProject(p_promo_sk_node_13=[$0], p_promo_id_node_13=[$1], p_start_date_sk_node_13=[$2], p_end_date_sk_node_13=[$3], p_item_sk_node_13=[$4], p_cost_node_13=[$5], p_response_target_node_13=[$6], p_promo_name_node_13=[$7], p_channel_dmail_node_13=[$8], p_channel_email_node_13=[$9], p_channel_catalog_node_13=[$10], p_channel_tv_node_13=[$11], p_channel_radio_node_13=[$12], p_channel_press_node_13=[$13], p_channel_event_node_13=[$14], p_channel_demo_node_13=[$15], p_channel_details_node_13=[$16], p_purpose_node_13=[$17], p_discount_active_node_13=[$18])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         :  +- LogicalProject(p_start_date_sk_node_14=[$2])
         :     +- LogicalTableScan(table=[[default_catalog, default_database, promotion]])
         +- LogicalProject(Axptz=[AS($0, _UTF-16LE'Axptz')], wp_web_page_id_node_15=[$1], wp_rec_start_date_node_15=[$2], wp_rec_end_date_node_15=[$3], wp_creation_date_sk_node_15=[$4], wp_access_date_sk_node_15=[$5], wp_autogen_flag_node_15=[$6], wp_customer_sk_node_15=[$7], wp_url_node_15=[$8], wp_type_node_15=[$9], wp_char_count_node_15=[$10], wp_link_count_node_15=[$11], wp_image_count_node_15=[$12], wp_max_ad_count_node_15=[$13])
            +- LogicalAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}])
               +- LogicalProject(wp_web_page_sk_node_15=[$0], wp_web_page_id_node_15=[$1], wp_rec_start_date_node_15=[$2], wp_rec_end_date_node_15=[$3], wp_creation_date_sk_node_15=[$4], wp_access_date_sk_node_15=[$5], wp_autogen_flag_node_15=[$6], wp_customer_sk_node_15=[$7], wp_url_node_15=[$8], wp_type_node_15=[$9], wp_char_count_node_15=[$10], wp_link_count_node_15=[$11], wp_image_count_node_15=[$12], wp_max_ad_count_node_15=[$13])
                  +- LogicalTableScan(table=[[default_catalog, default_database, web_page]])

== Optimized Physical Plan ==
Calc(select=[EXPR$0 AS p_end_date_sk_node_13])
+- HashAggregate(isMerge=[false], groupBy=[cd_dep_employed_count_node_12], select=[cd_dep_employed_count_node_12, MIN(p_end_date_sk_node_13) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_dep_employed_count_node_12]])
      +- Calc(select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, cd_demo_sk_node_12, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12, p_promo_sk AS p_promo_sk_node_13, p_promo_id AS p_promo_id_node_13, p_start_date_sk AS p_start_date_sk_node_13, p_end_date_sk AS p_end_date_sk_node_13, p_item_sk AS p_item_sk_node_13, p_cost AS p_cost_node_13, p_response_target AS p_response_target_node_13, p_promo_name AS p_promo_name_node_13, p_channel_dmail AS p_channel_dmail_node_13, p_channel_email AS p_channel_email_node_13, p_channel_catalog AS p_channel_catalog_node_13, p_channel_tv AS p_channel_tv_node_13, p_channel_radio AS p_channel_radio_node_13, p_channel_press AS p_channel_press_node_13, p_channel_event AS p_channel_event_node_13, p_channel_demo AS p_channel_demo_node_13, p_channel_details AS p_channel_details_node_13, p_purpose AS p_purpose_node_13, p_discount_active AS p_discount_active_node_13, p_start_date_sk0 AS p_start_date_sk_node_14, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15])
         +- HashJoin(joinType=[InnerJoin], where=[=(p_cost, i_wholesale_cost_node_110)], select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, cd_demo_sk_node_12, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12, i_wholesale_cost_node_110, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15], isBroadcast=[true], build=[right])
            :- Calc(select=[i_item_sk AS i_item_sk_node_11, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11, cd_demo_sk AS cd_demo_sk_node_12, cd_gender AS cd_gender_node_12, cd_marital_status AS cd_marital_status_node_12, cd_education_status AS cd_education_status_node_12, cd_purchase_estimate AS cd_purchase_estimate_node_12, cd_credit_rating AS cd_credit_rating_node_12, cd_dep_count AS cd_dep_count_node_12, cd_dep_employed_count AS cd_dep_employed_count_node_12, cd_dep_college_count AS cd_dep_college_count_node_12, CAST(i_wholesale_cost AS DECIMAL(15, 2)) AS i_wholesale_cost_node_110], where=[>=(i_manager_id, -7)])
            :  +- Sort(orderBy=[i_formulation ASC])
            :     +- Exchange(distribution=[single])
            :        +- HashJoin(joinType=[InnerJoin], where=[=(cd_gender, i_color)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
            :           :- Exchange(distribution=[hash[i_color]])
            :           :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            :           +- Exchange(distribution=[hash[cd_gender]])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- Exchange(distribution=[broadcast])
               +- HashJoin(joinType=[InnerJoin], where=[=(wp_web_page_id_node_15, p_channel_radio)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15], isBroadcast=[true], build=[right])
                  :- HashJoin(joinType=[InnerJoin], where=[=(p_start_date_sk0, p_end_date_sk)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0], isBroadcast=[true], build=[right])
                  :  :- Sort(orderBy=[p_purpose ASC])
                  :  :  +- Exchange(distribution=[single])
                  :  :     +- TableSourceScan(table=[[default_catalog, default_database, promotion, project=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], metadata=[]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  :  +- Exchange(distribution=[broadcast])
                  :     +- Calc(select=[p_start_date_sk])
                  :        +- TableSourceScan(table=[[default_catalog, default_database, promotion, project=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], metadata=[]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[wp_web_page_sk AS Axptz, wp_web_page_id AS wp_web_page_id_node_15, wp_rec_start_date AS wp_rec_start_date_node_15, wp_rec_end_date AS wp_rec_end_date_node_15, wp_creation_date_sk AS wp_creation_date_sk_node_15, wp_access_date_sk AS wp_access_date_sk_node_15, wp_autogen_flag AS wp_autogen_flag_node_15, wp_customer_sk AS wp_customer_sk_node_15, wp_url AS wp_url_node_15, wp_type AS wp_type_node_15, wp_char_count AS wp_char_count_node_15, wp_link_count AS wp_link_count_node_15, wp_image_count AS wp_image_count_node_15, wp_max_ad_count AS wp_max_ad_count_node_15])
                        +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                           +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
                              +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])

== Optimized Execution Plan ==
Calc(select=[EXPR$0 AS p_end_date_sk_node_13])
+- HashAggregate(isMerge=[false], groupBy=[cd_dep_employed_count_node_12], select=[cd_dep_employed_count_node_12, MIN(p_end_date_sk_node_13) AS EXPR$0])
   +- Exchange(distribution=[hash[cd_dep_employed_count_node_12]])
      +- Calc(select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, cd_demo_sk_node_12, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12, p_promo_sk AS p_promo_sk_node_13, p_promo_id AS p_promo_id_node_13, p_start_date_sk AS p_start_date_sk_node_13, p_end_date_sk AS p_end_date_sk_node_13, p_item_sk AS p_item_sk_node_13, p_cost AS p_cost_node_13, p_response_target AS p_response_target_node_13, p_promo_name AS p_promo_name_node_13, p_channel_dmail AS p_channel_dmail_node_13, p_channel_email AS p_channel_email_node_13, p_channel_catalog AS p_channel_catalog_node_13, p_channel_tv AS p_channel_tv_node_13, p_channel_radio AS p_channel_radio_node_13, p_channel_press AS p_channel_press_node_13, p_channel_event AS p_channel_event_node_13, p_channel_demo AS p_channel_demo_node_13, p_channel_details AS p_channel_details_node_13, p_purpose AS p_purpose_node_13, p_discount_active AS p_discount_active_node_13, p_start_date_sk0 AS p_start_date_sk_node_14, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15])
         +- HashJoin(joinType=[InnerJoin], where=[(p_cost = i_wholesale_cost_node_110)], select=[i_item_sk_node_11, i_item_id_node_11, i_rec_start_date_node_11, i_rec_end_date_node_11, i_item_desc_node_11, i_current_price_node_11, i_wholesale_cost_node_11, i_brand_id_node_11, i_brand_node_11, i_class_id_node_11, i_class_node_11, i_category_id_node_11, i_category_node_11, i_manufact_id_node_11, i_manufact_node_11, i_size_node_11, i_formulation_node_11, i_color_node_11, i_units_node_11, i_container_node_11, i_manager_id_node_11, i_product_name_node_11, cd_demo_sk_node_12, cd_gender_node_12, cd_marital_status_node_12, cd_education_status_node_12, cd_purchase_estimate_node_12, cd_credit_rating_node_12, cd_dep_count_node_12, cd_dep_employed_count_node_12, cd_dep_college_count_node_12, i_wholesale_cost_node_110, p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15], isBroadcast=[true], build=[right])
            :- Calc(select=[i_item_sk AS i_item_sk_node_11, i_item_id AS i_item_id_node_11, i_rec_start_date AS i_rec_start_date_node_11, i_rec_end_date AS i_rec_end_date_node_11, i_item_desc AS i_item_desc_node_11, i_current_price AS i_current_price_node_11, i_wholesale_cost AS i_wholesale_cost_node_11, i_brand_id AS i_brand_id_node_11, i_brand AS i_brand_node_11, i_class_id AS i_class_id_node_11, i_class AS i_class_node_11, i_category_id AS i_category_id_node_11, i_category AS i_category_node_11, i_manufact_id AS i_manufact_id_node_11, i_manufact AS i_manufact_node_11, i_size AS i_size_node_11, i_formulation AS i_formulation_node_11, i_color AS i_color_node_11, i_units AS i_units_node_11, i_container AS i_container_node_11, i_manager_id AS i_manager_id_node_11, i_product_name AS i_product_name_node_11, cd_demo_sk AS cd_demo_sk_node_12, cd_gender AS cd_gender_node_12, cd_marital_status AS cd_marital_status_node_12, cd_education_status AS cd_education_status_node_12, cd_purchase_estimate AS cd_purchase_estimate_node_12, cd_credit_rating AS cd_credit_rating_node_12, cd_dep_count AS cd_dep_count_node_12, cd_dep_employed_count AS cd_dep_employed_count_node_12, cd_dep_college_count AS cd_dep_college_count_node_12, CAST(i_wholesale_cost AS DECIMAL(15, 2)) AS i_wholesale_cost_node_110], where=[(i_manager_id >= -7)])
            :  +- Sort(orderBy=[i_formulation ASC])
            :     +- Exchange(distribution=[single])
            :        +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(cd_gender = i_color)], select=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name, cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count], build=[left])
            :           :- Exchange(distribution=[hash[i_color]])
            :           :  +- TableSourceScan(table=[[default_catalog, default_database, item]], fields=[i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc, i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id, i_class, i_category_id, i_category, i_manufact_id, i_manufact, i_size, i_formulation, i_color, i_units, i_container, i_manager_id, i_product_name])
            :           +- Exchange(distribution=[hash[cd_gender]])
            :              +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics]], fields=[cd_demo_sk, cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count])
            +- Exchange(distribution=[broadcast])
               +- MultipleInput(readOrder=[0,1,0], members=[\
HashJoin(joinType=[InnerJoin], where=[(wp_web_page_id_node_15 = p_channel_radio)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0, Axptz, wp_web_page_id_node_15, wp_rec_start_date_node_15, wp_rec_end_date_node_15, wp_creation_date_sk_node_15, wp_access_date_sk_node_15, wp_autogen_flag_node_15, wp_customer_sk_node_15, wp_url_node_15, wp_type_node_15, wp_char_count_node_15, wp_link_count_node_15, wp_image_count_node_15, wp_max_ad_count_node_15], isBroadcast=[true], build=[right])\
:- HashJoin(joinType=[InnerJoin], where=[(p_start_date_sk0 = p_end_date_sk)], select=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active, p_start_date_sk0], isBroadcast=[true], build=[right])\
:  :- [#2] Sort(orderBy=[p_purpose ASC])\
:  +- [#3] Exchange(distribution=[broadcast])\
+- [#1] Exchange(distribution=[broadcast])\
])
                  :- Exchange(distribution=[broadcast])
                  :  +- Calc(select=[wp_web_page_sk AS Axptz, wp_web_page_id AS wp_web_page_id_node_15, wp_rec_start_date AS wp_rec_start_date_node_15, wp_rec_end_date AS wp_rec_end_date_node_15, wp_creation_date_sk AS wp_creation_date_sk_node_15, wp_access_date_sk AS wp_access_date_sk_node_15, wp_autogen_flag AS wp_autogen_flag_node_15, wp_customer_sk AS wp_customer_sk_node_15, wp_url AS wp_url_node_15, wp_type AS wp_type_node_15, wp_char_count AS wp_char_count_node_15, wp_link_count AS wp_link_count_node_15, wp_image_count AS wp_image_count_node_15, wp_max_ad_count AS wp_max_ad_count_node_15])
                  :     +- HashAggregate(isMerge=[false], groupBy=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count], select=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                  :        +- Exchange(distribution=[hash[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count]])
                  :           +- TableSourceScan(table=[[default_catalog, default_database, web_page]], fields=[wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count])
                  :- Sort(orderBy=[p_purpose ASC])
                  :  +- Exchange(distribution=[single])
                  :     +- TableSourceScan(table=[[default_catalog, default_database, promotion, project=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active], metadata=[]]], fields=[p_promo_sk, p_promo_id, p_start_date_sk, p_end_date_sk, p_item_sk, p_cost, p_response_target, p_promo_name, p_channel_dmail, p_channel_email, p_channel_catalog, p_channel_tv, p_channel_radio, p_channel_press, p_channel_event, p_channel_demo, p_channel_details, p_purpose, p_discount_active])(reuse_id=[1])
                  +- Exchange(distribution=[broadcast])
                     +- Calc(select=[p_start_date_sk])
                        +- Reused(reference_id=[1])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o55342969.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#110932901:AbstractConverter.BATCH_PHYSICAL.hash[0, 1]true.[16](input=RelSubset#110932899,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1]true,sort=[16]), rel#110932898:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[16](input=RelSubset#110932897,groupBy=cd_dep_employed_count_node_12, i_wholesale_cost_node_110,select=cd_dep_employed_count_node_12, i_wholesale_cost_node_110)]
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