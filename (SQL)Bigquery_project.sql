Project Eccomerce SQL:
-- Link: https://docs.google.com/spreadsheets/d/1NiKHlMK6Qvag75SgfxYZAOxRS2SAulmUUaYRYoXUeKY/edit#gid=0

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
Select  
    Format_date("%Y%m", Parse_date ("%Y%m%d", date)) as Month,
    Count(totals.visits) as Visits, 
    Sum(totals.pageviews) as Pageviews,
    Sum(totals.transactions) as Transactions, 
    Sum(totals.totalTransactionRevenue) as Revenues 
From `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix Between '20170101' And '20170331'
Group by 1   
Order by 1

-- Query 02: Bounce rate per traffic source in July 2017
Select trafficSource.source, 
        Sum(totals.visits) as total_visits,
        Sum(totals.bounces) as total_no_of_bounces, 
        Sum(totals.bounces) * 100.0 / sum(totals.visits) as bounce_rate
From `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix between '20170701' and '20170731'
Group by 1
Order by 2 desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017
Select 
    "month" as time_type,
    Format_date("%Y%m", parse_date("%Y%m%d", date)) as time,
    trafficSource.source, 
    Sum(totals.transactionRevenue)/1000000 as revenue
From `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170601' and '20170630' 
Group by 2, 3

Union all 

Select 
    "week" as time_type,
    format_date("%Y%W", parse_date("%Y%m%d", date)) as time,
    trafficSource.source, 
    Sum(totals.transactionRevenue)/1000000 as revenue
From `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170601' and '20170630' 
Group by 2, 3
Order by 3, 2;
 
--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. 
-- Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month)

select
    p.*,
    avg_pageviews_non_purchase
from purchaser_data p
left join non_purchaser_data using(month)
order by p.month
 
-- Query 05: Average number of transactions per user that made a purchase in July 2017

Select 
    Format_date ("%Y%m",Parse_date("%Y%m%d",date)) as month,
    Sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
Where totals.transactions >= 1
Group by month;
 
-- Query 06: Average amount of money spent per session

Select 
    Format_date("%Y%m",Parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
Where totals.transactions is not null
Group by month;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. 
-- Output should show product name and the quantity was ordered.

-- Solution 1: Use subquery in "where"
Select 
    product.v2ProductName, 
    Sum(product.productQuantity) as quantity
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
Unnest (hits) as hits,
Unnest (hits.product) as product
where fullvisitorid in (
                        Select  
                            fullvisitorid,
                        From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
                        Unnest (hits) as hits,
                        Unnest (hits.product) as product                                  
                        Where product.v2ProductName = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
And product.v2ProductName <> "YouTube Men's Vintage Henley"
And product.productRevenue is not null
Group by product.v2ProductName
Order by product.v2ProductName, quantity;

--Solution 2: Use CTE and join
with buyer_list as(
    Select
        distinct fullVisitorId
    From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    unnest(hits) as hits,
    unnest(hits.product) as product
    Where product.v2ProductName = "YouTube Men's Vintage Henley"
    And totals.transactions>=1
    And product.productRevenue is not null)

Select
  product.v2ProductName as other_purchased_products,
  Sum(product.productQuantity) as quantity
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest(hits) as hits,
unnest(hits.product) as product
Join buyer_list using(fullVisitorId)
Where product.v2ProductName != "YouTube Men's Vintage Henley"
And product.productRevenue is not null
Group by other_purchased_products
Order by quantity desc
 

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
y month;

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
unnest(hits) as hits,
unnest(hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data
