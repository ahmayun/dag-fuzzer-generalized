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

autonode_13 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_13") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_17 = table_env.from_path("web_sales").select(*[col(column_name).alias(f"{column_name}_node_17") for column_name in table_env.from_path("web_sales").get_schema().get_field_names()])
autonode_18 = table_env.from_path("customer_address").select(*[col(column_name).alias(f"{column_name}_node_18") for column_name in table_env.from_path("customer_address").get_schema().get_field_names()])
autonode_19 = table_env.from_path("store_returns").select(*[col(column_name).alias(f"{column_name}_node_19") for column_name in table_env.from_path("store_returns").get_schema().get_field_names()])
autonode_14 = table_env.from_path("customer_demographics").select(*[col(column_name).alias(f"{column_name}_node_14") for column_name in table_env.from_path("customer_demographics").get_schema().get_field_names()])
autonode_15 = table_env.from_path("web_site").select(*[col(column_name).alias(f"{column_name}_node_15") for column_name in table_env.from_path("web_site").get_schema().get_field_names()])
autonode_16 = table_env.from_path("income_band").select(*[col(column_name).alias(f"{column_name}_node_16") for column_name in table_env.from_path("income_band").get_schema().get_field_names()])
autonode_11 = autonode_18.order_by(col('ca_street_name_node_18'))
autonode_12 = autonode_19.filter(col('sr_return_tax_node_19') >= 46.58842086791992)
autonode_8 = autonode_13.join(autonode_14, col('cd_credit_rating_node_14') == col('cd_gender_node_13'))
autonode_9 = autonode_15.alias('vhEPo')
autonode_10 = autonode_16.join(autonode_17, col('ib_upper_bound_node_16') == col('ws_bill_addr_sk_node_17'))
autonode_7 = autonode_12.alias('99yHS')
autonode_5 = autonode_8.join(autonode_9, col('web_manager_node_15') == col('cd_marital_status_node_13'))
autonode_6 = autonode_10.join(autonode_11, col('ca_gmt_offset_node_18') == col('ws_ext_sales_price_node_17'))
autonode_3 = autonode_5.filter(col('cd_demo_sk_node_13') <= -30)
autonode_4 = autonode_6.join(autonode_7, col('ca_address_sk_node_18') == col('sr_ticket_number_node_19'))
autonode_1 = autonode_3.select(col('cd_dep_employed_count_node_14'))
autonode_2 = autonode_4.group_by(col('ca_suite_number_node_18')).select(col('sr_addr_sk_node_19').min.alias('sr_addr_sk_node_19'))
sink = autonode_1.join(autonode_2, col('sr_addr_sk_node_19') == col('cd_dep_employed_count_node_14'))
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
LogicalJoin(condition=[=($1, $0)], joinType=[inner])
:- LogicalProject(cd_dep_employed_count_node_14=[$16])
:  +- LogicalFilter(condition=[<=($0, -30)])
:     +- LogicalJoin(condition=[=($26, $2)], joinType=[inner])
:        :- LogicalJoin(condition=[=($14, $1)], joinType=[inner])
:        :  :- LogicalProject(cd_demo_sk_node_13=[$0], cd_gender_node_13=[$1], cd_marital_status_node_13=[$2], cd_education_status_node_13=[$3], cd_purchase_estimate_node_13=[$4], cd_credit_rating_node_13=[$5], cd_dep_count_node_13=[$6], cd_dep_employed_count_node_13=[$7], cd_dep_college_count_node_13=[$8])
:        :  :  +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
:        :  +- LogicalProject(cd_demo_sk_node_14=[$0], cd_gender_node_14=[$1], cd_marital_status_node_14=[$2], cd_education_status_node_14=[$3], cd_purchase_estimate_node_14=[$4], cd_credit_rating_node_14=[$5], cd_dep_count_node_14=[$6], cd_dep_employed_count_node_14=[$7], cd_dep_college_count_node_14=[$8])
:        :     +- LogicalTableScan(table=[[default_catalog, default_database, customer_demographics]])
:        +- LogicalProject(vhEPo=[AS($0, _UTF-16LE'vhEPo')], web_site_id_node_15=[$1], web_rec_start_date_node_15=[$2], web_rec_end_date_node_15=[$3], web_name_node_15=[$4], web_open_date_sk_node_15=[$5], web_close_date_sk_node_15=[$6], web_class_node_15=[$7], web_manager_node_15=[$8], web_mkt_id_node_15=[$9], web_mkt_class_node_15=[$10], web_mkt_desc_node_15=[$11], web_market_manager_node_15=[$12], web_company_id_node_15=[$13], web_company_name_node_15=[$14], web_street_number_node_15=[$15], web_street_name_node_15=[$16], web_street_type_node_15=[$17], web_suite_number_node_15=[$18], web_city_node_15=[$19], web_county_node_15=[$20], web_state_node_15=[$21], web_zip_node_15=[$22], web_country_node_15=[$23], web_gmt_offset_node_15=[$24], web_tax_percentage_node_15=[$25])
:           +- LogicalTableScan(table=[[default_catalog, default_database, web_site]])
+- LogicalProject(sr_addr_sk_node_19=[$1])
   +- LogicalAggregate(group=[{42}], EXPR$0=[MIN($56)])
      +- LogicalJoin(condition=[=($37, $59)], joinType=[inner])
         :- LogicalJoin(condition=[=($48, $26)], joinType=[inner])
         :  :- LogicalJoin(condition=[=($2, $10)], joinType=[inner])
         :  :  :- LogicalProject(ib_income_band_sk_node_16=[$0], ib_lower_bound_node_16=[$1], ib_upper_bound_node_16=[$2])
         :  :  :  +- LogicalTableScan(table=[[default_catalog, default_database, income_band]])
         :  :  +- LogicalProject(ws_sold_date_sk_node_17=[$0], ws_sold_time_sk_node_17=[$1], ws_ship_date_sk_node_17=[$2], ws_item_sk_node_17=[$3], ws_bill_customer_sk_node_17=[$4], ws_bill_cdemo_sk_node_17=[$5], ws_bill_hdemo_sk_node_17=[$6], ws_bill_addr_sk_node_17=[$7], ws_ship_customer_sk_node_17=[$8], ws_ship_cdemo_sk_node_17=[$9], ws_ship_hdemo_sk_node_17=[$10], ws_ship_addr_sk_node_17=[$11], ws_web_page_sk_node_17=[$12], ws_web_site_sk_node_17=[$13], ws_ship_mode_sk_node_17=[$14], ws_warehouse_sk_node_17=[$15], ws_promo_sk_node_17=[$16], ws_order_number_node_17=[$17], ws_quantity_node_17=[$18], ws_wholesale_cost_node_17=[$19], ws_list_price_node_17=[$20], ws_sales_price_node_17=[$21], ws_ext_discount_amt_node_17=[$22], ws_ext_sales_price_node_17=[$23], ws_ext_wholesale_cost_node_17=[$24], ws_ext_list_price_node_17=[$25], ws_ext_tax_node_17=[$26], ws_coupon_amt_node_17=[$27], ws_ext_ship_cost_node_17=[$28], ws_net_paid_node_17=[$29], ws_net_paid_inc_tax_node_17=[$30], ws_net_paid_inc_ship_node_17=[$31], ws_net_paid_inc_ship_tax_node_17=[$32], ws_net_profit_node_17=[$33])
         :  :     +- LogicalTableScan(table=[[default_catalog, default_database, web_sales]])
         :  +- LogicalSort(sort0=[$3], dir0=[ASC])
         :     +- LogicalProject(ca_address_sk_node_18=[$0], ca_address_id_node_18=[$1], ca_street_number_node_18=[$2], ca_street_name_node_18=[$3], ca_street_type_node_18=[$4], ca_suite_number_node_18=[$5], ca_city_node_18=[$6], ca_county_node_18=[$7], ca_state_node_18=[$8], ca_zip_node_18=[$9], ca_country_node_18=[$10], ca_gmt_offset_node_18=[$11], ca_location_type_node_18=[$12])
         :        +- LogicalTableScan(table=[[default_catalog, default_database, customer_address]])
         +- LogicalProject(99yHS=[AS($0, _UTF-16LE'99yHS')], sr_return_time_sk_node_19=[$1], sr_item_sk_node_19=[$2], sr_customer_sk_node_19=[$3], sr_cdemo_sk_node_19=[$4], sr_hdemo_sk_node_19=[$5], sr_addr_sk_node_19=[$6], sr_store_sk_node_19=[$7], sr_reason_sk_node_19=[$8], sr_ticket_number_node_19=[$9], sr_return_quantity_node_19=[$10], sr_return_amt_node_19=[$11], sr_return_tax_node_19=[$12], sr_return_amt_inc_tax_node_19=[$13], sr_fee_node_19=[$14], sr_return_ship_cost_node_19=[$15], sr_refunded_cash_node_19=[$16], sr_reversed_charge_node_19=[$17], sr_store_credit_node_19=[$18], sr_net_loss_node_19=[$19])
            +- LogicalFilter(condition=[>=($12, 4.658842086791992E1:DOUBLE)])
               +- LogicalProject(sr_returned_date_sk_node_19=[$0], sr_return_time_sk_node_19=[$1], sr_item_sk_node_19=[$2], sr_customer_sk_node_19=[$3], sr_cdemo_sk_node_19=[$4], sr_hdemo_sk_node_19=[$5], sr_addr_sk_node_19=[$6], sr_store_sk_node_19=[$7], sr_reason_sk_node_19=[$8], sr_ticket_number_node_19=[$9], sr_return_quantity_node_19=[$10], sr_return_amt_node_19=[$11], sr_return_tax_node_19=[$12], sr_return_amt_inc_tax_node_19=[$13], sr_fee_node_19=[$14], sr_return_ship_cost_node_19=[$15], sr_refunded_cash_node_19=[$16], sr_reversed_charge_node_19=[$17], sr_store_credit_node_19=[$18], sr_net_loss_node_19=[$19])
                  +- LogicalTableScan(table=[[default_catalog, default_database, store_returns]])

== Optimized Physical Plan ==
HashJoin(joinType=[InnerJoin], where=[=(sr_addr_sk_node_19, cd_dep_employed_count_node_14)], select=[cd_dep_employed_count_node_14, sr_addr_sk_node_19], isBroadcast=[true], build=[right])
:- Calc(select=[cd_dep_employed_count_node_130 AS cd_dep_employed_count_node_14])
:  +- HashJoin(joinType=[InnerJoin], where=[=(web_manager, cd_marital_status_node_13)], select=[cd_marital_status_node_13, cd_dep_employed_count_node_130, web_manager], isBroadcast=[true], build=[right])
:     :- Calc(select=[cd_marital_status AS cd_marital_status_node_13, cd_dep_employed_count AS cd_dep_employed_count_node_130])
:     :  +- HashJoin(joinType=[InnerJoin], where=[=(cd_credit_rating, cd_gender)], select=[cd_gender, cd_marital_status, cd_credit_rating, cd_dep_employed_count], build=[left])
:     :     :- Exchange(distribution=[hash[cd_gender]])
:     :     :  +- Calc(select=[cd_gender, cd_marital_status], where=[<=(cd_demo_sk, -30)])
:     :     :     +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[<=(cd_demo_sk, -30)], project=[cd_demo_sk, cd_gender, cd_marital_status], metadata=[]]], fields=[cd_demo_sk, cd_gender, cd_marital_status])
:     :     +- Exchange(distribution=[hash[cd_credit_rating]])
:     :        +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_credit_rating, cd_dep_employed_count], metadata=[]]], fields=[cd_credit_rating, cd_dep_employed_count])
:     +- Exchange(distribution=[broadcast])
:        +- TableSourceScan(table=[[default_catalog, default_database, web_site, project=[web_manager], metadata=[]]], fields=[web_manager])
+- Exchange(distribution=[broadcast])
   +- Calc(select=[EXPR$0 AS sr_addr_sk_node_19])
      +- HashAggregate(isMerge=[false], groupBy=[ca_suite_number_node_18], select=[ca_suite_number_node_18, MIN(sr_addr_sk_node_19) AS EXPR$0])
         +- Exchange(distribution=[hash[ca_suite_number_node_18]])
            +- HashJoin(joinType=[InnerJoin], where=[=(ca_address_sk_node_18, sr_ticket_number_node_19)], select=[ib_income_band_sk_node_16, ib_lower_bound_node_16, ib_upper_bound_node_16, ws_sold_date_sk_node_17, ws_sold_time_sk_node_17, ws_ship_date_sk_node_17, ws_item_sk_node_17, ws_bill_customer_sk_node_17, ws_bill_cdemo_sk_node_17, ws_bill_hdemo_sk_node_17, ws_bill_addr_sk_node_17, ws_ship_customer_sk_node_17, ws_ship_cdemo_sk_node_17, ws_ship_hdemo_sk_node_17, ws_ship_addr_sk_node_17, ws_web_page_sk_node_17, ws_web_site_sk_node_17, ws_ship_mode_sk_node_17, ws_warehouse_sk_node_17, ws_promo_sk_node_17, ws_order_number_node_17, ws_quantity_node_17, ws_wholesale_cost_node_17, ws_list_price_node_17, ws_sales_price_node_17, ws_ext_discount_amt_node_17, ws_ext_sales_price_node_17, ws_ext_wholesale_cost_node_17, ws_ext_list_price_node_17, ws_ext_tax_node_17, ws_coupon_amt_node_17, ws_ext_ship_cost_node_17, ws_net_paid_node_17, ws_net_paid_inc_tax_node_17, ws_net_paid_inc_ship_node_17, ws_net_paid_inc_ship_tax_node_17, ws_net_profit_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, 99yHS, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19], build=[left])
               :- Exchange(distribution=[hash[ca_address_sk_node_18]])
               :  +- Calc(select=[ib_income_band_sk AS ib_income_band_sk_node_16, ib_lower_bound AS ib_lower_bound_node_16, ib_upper_bound AS ib_upper_bound_node_16, ws_sold_date_sk AS ws_sold_date_sk_node_17, ws_sold_time_sk AS ws_sold_time_sk_node_17, ws_ship_date_sk AS ws_ship_date_sk_node_17, ws_item_sk AS ws_item_sk_node_17, ws_bill_customer_sk AS ws_bill_customer_sk_node_17, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_17, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_17, ws_bill_addr_sk AS ws_bill_addr_sk_node_17, ws_ship_customer_sk AS ws_ship_customer_sk_node_17, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_17, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_17, ws_ship_addr_sk AS ws_ship_addr_sk_node_17, ws_web_page_sk AS ws_web_page_sk_node_17, ws_web_site_sk AS ws_web_site_sk_node_17, ws_ship_mode_sk AS ws_ship_mode_sk_node_17, ws_warehouse_sk AS ws_warehouse_sk_node_17, ws_promo_sk AS ws_promo_sk_node_17, ws_order_number AS ws_order_number_node_17, ws_quantity AS ws_quantity_node_17, ws_wholesale_cost AS ws_wholesale_cost_node_17, ws_list_price AS ws_list_price_node_17, ws_sales_price AS ws_sales_price_node_17, ws_ext_discount_amt AS ws_ext_discount_amt_node_17, ws_ext_sales_price AS ws_ext_sales_price_node_17, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_17, ws_ext_list_price AS ws_ext_list_price_node_17, ws_ext_tax AS ws_ext_tax_node_17, ws_coupon_amt AS ws_coupon_amt_node_17, ws_ext_ship_cost AS ws_ext_ship_cost_node_17, ws_net_paid AS ws_net_paid_node_17, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_17, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_17, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_17, ws_net_profit AS ws_net_profit_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18])
               :     +- HashJoin(joinType=[InnerJoin], where=[=(ca_gmt_offset_node_180, ws_ext_sales_price)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, ca_gmt_offset_node_180], build=[right])
               :        :- Exchange(distribution=[hash[ws_ext_sales_price]])
               :        :  +- HashJoin(joinType=[InnerJoin], where=[=(ib_upper_bound, ws_bill_addr_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], isBroadcast=[true], build=[left])
               :        :     :- Exchange(distribution=[broadcast])
               :        :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
               :        :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
               :        +- Exchange(distribution=[hash[ca_gmt_offset_node_180]])
               :           +- Calc(select=[ca_address_sk AS ca_address_sk_node_18, ca_address_id AS ca_address_id_node_18, ca_street_number AS ca_street_number_node_18, ca_street_name AS ca_street_name_node_18, ca_street_type AS ca_street_type_node_18, ca_suite_number AS ca_suite_number_node_18, ca_city AS ca_city_node_18, ca_county AS ca_county_node_18, ca_state AS ca_state_node_18, ca_zip AS ca_zip_node_18, ca_country AS ca_country_node_18, ca_gmt_offset AS ca_gmt_offset_node_18, ca_location_type AS ca_location_type_node_18, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_180])
               :              +- Sort(orderBy=[ca_street_name ASC])
               :                 +- Exchange(distribution=[single])
               :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
               +- Exchange(distribution=[hash[sr_ticket_number_node_19]])
                  +- Calc(select=[sr_returned_date_sk AS 99yHS, sr_return_time_sk AS sr_return_time_sk_node_19, sr_item_sk AS sr_item_sk_node_19, sr_customer_sk AS sr_customer_sk_node_19, sr_cdemo_sk AS sr_cdemo_sk_node_19, sr_hdemo_sk AS sr_hdemo_sk_node_19, sr_addr_sk AS sr_addr_sk_node_19, sr_store_sk AS sr_store_sk_node_19, sr_reason_sk AS sr_reason_sk_node_19, sr_ticket_number AS sr_ticket_number_node_19, sr_return_quantity AS sr_return_quantity_node_19, sr_return_amt AS sr_return_amt_node_19, sr_return_tax AS sr_return_tax_node_19, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_19, sr_fee AS sr_fee_node_19, sr_return_ship_cost AS sr_return_ship_cost_node_19, sr_refunded_cash AS sr_refunded_cash_node_19, sr_reversed_charge AS sr_reversed_charge_node_19, sr_store_credit AS sr_store_credit_node_19, sr_net_loss AS sr_net_loss_node_19], where=[>=(sr_return_tax, 4.658842086791992E1)])
                     +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[>=(sr_return_tax, 4.658842086791992E1:DOUBLE)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])

== Optimized Execution Plan ==
MultipleInput(readOrder=[0,0,0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(sr_addr_sk_node_19 = cd_dep_employed_count_node_14)], select=[cd_dep_employed_count_node_14, sr_addr_sk_node_19], isBroadcast=[true], build=[right])\
:- Calc(select=[cd_dep_employed_count_node_130 AS cd_dep_employed_count_node_14])\
:  +- HashJoin(joinType=[InnerJoin], where=[(web_manager = cd_marital_status_node_13)], select=[cd_marital_status_node_13, cd_dep_employed_count_node_130, web_manager], isBroadcast=[true], build=[right])\
:     :- Calc(select=[cd_marital_status AS cd_marital_status_node_13, cd_dep_employed_count AS cd_dep_employed_count_node_130])\
:     :  +- HashJoin(joinType=[InnerJoin], where=[(cd_credit_rating = cd_gender)], select=[cd_gender, cd_marital_status, cd_credit_rating, cd_dep_employed_count], build=[left])\
:     :     :- [#3] Exchange(distribution=[hash[cd_gender]])\
:     :     +- [#4] Exchange(distribution=[hash[cd_credit_rating]])\
:     +- [#2] Exchange(distribution=[broadcast])\
+- [#1] Exchange(distribution=[broadcast])\
])
:- Exchange(distribution=[broadcast])
:  +- Calc(select=[EXPR$0 AS sr_addr_sk_node_19])
:     +- HashAggregate(isMerge=[false], groupBy=[ca_suite_number_node_18], select=[ca_suite_number_node_18, MIN(sr_addr_sk_node_19) AS EXPR$0])
:        +- Exchange(distribution=[hash[ca_suite_number_node_18]])
:           +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_address_sk_node_18 = sr_ticket_number_node_19)], select=[ib_income_band_sk_node_16, ib_lower_bound_node_16, ib_upper_bound_node_16, ws_sold_date_sk_node_17, ws_sold_time_sk_node_17, ws_ship_date_sk_node_17, ws_item_sk_node_17, ws_bill_customer_sk_node_17, ws_bill_cdemo_sk_node_17, ws_bill_hdemo_sk_node_17, ws_bill_addr_sk_node_17, ws_ship_customer_sk_node_17, ws_ship_cdemo_sk_node_17, ws_ship_hdemo_sk_node_17, ws_ship_addr_sk_node_17, ws_web_page_sk_node_17, ws_web_site_sk_node_17, ws_ship_mode_sk_node_17, ws_warehouse_sk_node_17, ws_promo_sk_node_17, ws_order_number_node_17, ws_quantity_node_17, ws_wholesale_cost_node_17, ws_list_price_node_17, ws_sales_price_node_17, ws_ext_discount_amt_node_17, ws_ext_sales_price_node_17, ws_ext_wholesale_cost_node_17, ws_ext_list_price_node_17, ws_ext_tax_node_17, ws_coupon_amt_node_17, ws_ext_ship_cost_node_17, ws_net_paid_node_17, ws_net_paid_inc_tax_node_17, ws_net_paid_inc_ship_node_17, ws_net_paid_inc_ship_tax_node_17, ws_net_profit_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, 99yHS, sr_return_time_sk_node_19, sr_item_sk_node_19, sr_customer_sk_node_19, sr_cdemo_sk_node_19, sr_hdemo_sk_node_19, sr_addr_sk_node_19, sr_store_sk_node_19, sr_reason_sk_node_19, sr_ticket_number_node_19, sr_return_quantity_node_19, sr_return_amt_node_19, sr_return_tax_node_19, sr_return_amt_inc_tax_node_19, sr_fee_node_19, sr_return_ship_cost_node_19, sr_refunded_cash_node_19, sr_reversed_charge_node_19, sr_store_credit_node_19, sr_net_loss_node_19], build=[left])
:              :- Exchange(distribution=[hash[ca_address_sk_node_18]])
:              :  +- Calc(select=[ib_income_band_sk AS ib_income_band_sk_node_16, ib_lower_bound AS ib_lower_bound_node_16, ib_upper_bound AS ib_upper_bound_node_16, ws_sold_date_sk AS ws_sold_date_sk_node_17, ws_sold_time_sk AS ws_sold_time_sk_node_17, ws_ship_date_sk AS ws_ship_date_sk_node_17, ws_item_sk AS ws_item_sk_node_17, ws_bill_customer_sk AS ws_bill_customer_sk_node_17, ws_bill_cdemo_sk AS ws_bill_cdemo_sk_node_17, ws_bill_hdemo_sk AS ws_bill_hdemo_sk_node_17, ws_bill_addr_sk AS ws_bill_addr_sk_node_17, ws_ship_customer_sk AS ws_ship_customer_sk_node_17, ws_ship_cdemo_sk AS ws_ship_cdemo_sk_node_17, ws_ship_hdemo_sk AS ws_ship_hdemo_sk_node_17, ws_ship_addr_sk AS ws_ship_addr_sk_node_17, ws_web_page_sk AS ws_web_page_sk_node_17, ws_web_site_sk AS ws_web_site_sk_node_17, ws_ship_mode_sk AS ws_ship_mode_sk_node_17, ws_warehouse_sk AS ws_warehouse_sk_node_17, ws_promo_sk AS ws_promo_sk_node_17, ws_order_number AS ws_order_number_node_17, ws_quantity AS ws_quantity_node_17, ws_wholesale_cost AS ws_wholesale_cost_node_17, ws_list_price AS ws_list_price_node_17, ws_sales_price AS ws_sales_price_node_17, ws_ext_discount_amt AS ws_ext_discount_amt_node_17, ws_ext_sales_price AS ws_ext_sales_price_node_17, ws_ext_wholesale_cost AS ws_ext_wholesale_cost_node_17, ws_ext_list_price AS ws_ext_list_price_node_17, ws_ext_tax AS ws_ext_tax_node_17, ws_coupon_amt AS ws_coupon_amt_node_17, ws_ext_ship_cost AS ws_ext_ship_cost_node_17, ws_net_paid AS ws_net_paid_node_17, ws_net_paid_inc_tax AS ws_net_paid_inc_tax_node_17, ws_net_paid_inc_ship AS ws_net_paid_inc_ship_node_17, ws_net_paid_inc_ship_tax AS ws_net_paid_inc_ship_tax_node_17, ws_net_profit AS ws_net_profit_node_17, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18])
:              :     +- AdaptiveJoin(originalJoin=[ShuffleHashJoin], joinType=[InnerJoin], where=[(ca_gmt_offset_node_180 = ws_ext_sales_price)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit, ca_address_sk_node_18, ca_address_id_node_18, ca_street_number_node_18, ca_street_name_node_18, ca_street_type_node_18, ca_suite_number_node_18, ca_city_node_18, ca_county_node_18, ca_state_node_18, ca_zip_node_18, ca_country_node_18, ca_gmt_offset_node_18, ca_location_type_node_18, ca_gmt_offset_node_180], build=[right])
:              :        :- Exchange(distribution=[hash[ws_ext_sales_price]])
:              :        :  +- MultipleInput(readOrder=[0,1], members=[\
HashJoin(joinType=[InnerJoin], where=[(ib_upper_bound = ws_bill_addr_sk)], select=[ib_income_band_sk, ib_lower_bound, ib_upper_bound, ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit], isBroadcast=[true], build=[left])\
:- [#1] Exchange(distribution=[broadcast])\
+- [#2] TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])\
])
:              :        :     :- Exchange(distribution=[broadcast])
:              :        :     :  +- TableSourceScan(table=[[default_catalog, default_database, income_band]], fields=[ib_income_band_sk, ib_lower_bound, ib_upper_bound])
:              :        :     +- TableSourceScan(table=[[default_catalog, default_database, web_sales]], fields=[ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk, ws_item_sk, ws_bill_customer_sk, ws_bill_cdemo_sk, ws_bill_hdemo_sk, ws_bill_addr_sk, ws_ship_customer_sk, ws_ship_cdemo_sk, ws_ship_hdemo_sk, ws_ship_addr_sk, ws_web_page_sk, ws_web_site_sk, ws_ship_mode_sk, ws_warehouse_sk, ws_promo_sk, ws_order_number, ws_quantity, ws_wholesale_cost, ws_list_price, ws_sales_price, ws_ext_discount_amt, ws_ext_sales_price, ws_ext_wholesale_cost, ws_ext_list_price, ws_ext_tax, ws_coupon_amt, ws_ext_ship_cost, ws_net_paid, ws_net_paid_inc_tax, ws_net_paid_inc_ship, ws_net_paid_inc_ship_tax, ws_net_profit])
:              :        +- Exchange(distribution=[hash[ca_gmt_offset_node_180]])
:              :           +- Calc(select=[ca_address_sk AS ca_address_sk_node_18, ca_address_id AS ca_address_id_node_18, ca_street_number AS ca_street_number_node_18, ca_street_name AS ca_street_name_node_18, ca_street_type AS ca_street_type_node_18, ca_suite_number AS ca_suite_number_node_18, ca_city AS ca_city_node_18, ca_county AS ca_county_node_18, ca_state AS ca_state_node_18, ca_zip AS ca_zip_node_18, ca_country AS ca_country_node_18, ca_gmt_offset AS ca_gmt_offset_node_18, ca_location_type AS ca_location_type_node_18, CAST(ca_gmt_offset AS DECIMAL(7, 2)) AS ca_gmt_offset_node_180])
:              :              +- Sort(orderBy=[ca_street_name ASC])
:              :                 +- Exchange(distribution=[single])
:              :                    +- TableSourceScan(table=[[default_catalog, default_database, customer_address]], fields=[ca_address_sk, ca_address_id, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type])
:              +- Exchange(distribution=[hash[sr_ticket_number_node_19]])
:                 +- Calc(select=[sr_returned_date_sk AS 99yHS, sr_return_time_sk AS sr_return_time_sk_node_19, sr_item_sk AS sr_item_sk_node_19, sr_customer_sk AS sr_customer_sk_node_19, sr_cdemo_sk AS sr_cdemo_sk_node_19, sr_hdemo_sk AS sr_hdemo_sk_node_19, sr_addr_sk AS sr_addr_sk_node_19, sr_store_sk AS sr_store_sk_node_19, sr_reason_sk AS sr_reason_sk_node_19, sr_ticket_number AS sr_ticket_number_node_19, sr_return_quantity AS sr_return_quantity_node_19, sr_return_amt AS sr_return_amt_node_19, sr_return_tax AS sr_return_tax_node_19, sr_return_amt_inc_tax AS sr_return_amt_inc_tax_node_19, sr_fee AS sr_fee_node_19, sr_return_ship_cost AS sr_return_ship_cost_node_19, sr_refunded_cash AS sr_refunded_cash_node_19, sr_reversed_charge AS sr_reversed_charge_node_19, sr_store_credit AS sr_store_credit_node_19, sr_net_loss AS sr_net_loss_node_19], where=[(sr_return_tax >= 4.658842086791992E1)])
:                    +- TableSourceScan(table=[[default_catalog, default_database, store_returns, filter=[>=(sr_return_tax, 4.658842086791992E1:DOUBLE)]]], fields=[sr_returned_date_sk, sr_return_time_sk, sr_item_sk, sr_customer_sk, sr_cdemo_sk, sr_hdemo_sk, sr_addr_sk, sr_store_sk, sr_reason_sk, sr_ticket_number, sr_return_quantity, sr_return_amt, sr_return_tax, sr_return_amt_inc_tax, sr_fee, sr_return_ship_cost, sr_refunded_cash, sr_reversed_charge, sr_store_credit, sr_net_loss])
:- Exchange(distribution=[broadcast])
:  +- TableSourceScan(table=[[default_catalog, default_database, web_site, project=[web_manager], metadata=[]]], fields=[web_manager])
:- Exchange(distribution=[hash[cd_gender]])
:  +- Calc(select=[cd_gender, cd_marital_status], where=[(cd_demo_sk <= -30)])
:     +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, filter=[<=(cd_demo_sk, -30)], project=[cd_demo_sk, cd_gender, cd_marital_status], metadata=[]]], fields=[cd_demo_sk, cd_gender, cd_marital_status])
+- Exchange(distribution=[hash[cd_credit_rating]])
   +- TableSourceScan(table=[[default_catalog, default_database, customer_demographics, project=[cd_credit_rating, cd_dep_employed_count], metadata=[]]], fields=[cd_credit_rating, cd_dep_employed_count])

",
      "stderr": ""
    },
    "unopt": {
      "success": false,
      "error_name": "RuntimeException",
      "error_message": "An error occurred while calling o310759086.explain.
: java.lang.RuntimeException: Error while applying rule FlinkExpandConversionRule, args [rel#626060534:AbstractConverter.BATCH_PHYSICAL.hash[0, 1, 2]true.[3](input=RelSubset#626060532,convention=BATCH_PHYSICAL,FlinkRelDistributionTraitDef=hash[0, 1, 2]true,sort=[3]), rel#626060531:BatchPhysicalLocalHashAggregate.BATCH_PHYSICAL.any.[3](input=RelSubset#626060530,groupBy=ca_address_sk_node_18, ca_suite_number_node_18, ca_gmt_offset_node_180,select=ca_address_sk_node_18, ca_suite_number_node_18, ca_gmt_offset_node_180)]
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