Project Eccomerce SQL:
-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé
--Thụt dòng cho từng đoạn, từng phần để dễ nhìn hơn


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
-- select
--     count(distinct fullVisitorId)
-- FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
-- Where _table_suffix between '20170101' and '20170331'

Query:

Select  
    Format_date("%Y%m", Parse_date ("%Y%m%d", date)) as Month,
    Count(totals.visits) as Visits, 
    Sum(totals.pageviews) as Pageviews,
    Sum(totals.transactions) as Transactions, 
    Sum(totals.totalTransactionRevenue) as Revenues 
From `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix Between '20170101' And '20170331'
Group by month   --group by 1
Order by month;  --order by 1

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

Select trafficSource.source, 
        Sum(totals.visits) as total_visits,
        Sum(totals.bounces) as total_no_of_bounces, 
        Sum(totals.bounces) * 100.0 / sum(totals.visits) as bounce_rate
From `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix between '20170701' and '20170731'
Group by trafficSource.source
Order by total_visits desc;


-- Query 3: Revenue by traffic source by week, by month in June 2017

Select 
    Cast("Month" as string) as time_type, --"month" as time_type
    Format_date("%Y%m", parse_date("%Y%m%d", date)) as time,
    trafficSource.source, 
    Sum (totals.transactionRevenue)   --e chỉ cần chia cho 1000000 là giống expected output; SUM(totals.totalTransactionRevenue)/1000000 AS
From `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170601' and '20170630' 
Group by time, trafficSource.source 

UNION ALL 

Select 
    Cast("Week" as string) as time_type, --"week" as time_type
    format_date("%Y%W", parse_date("%Y%m%d", date)) as time,
    trafficSource.source, 
    Sum (totals.transactionRevenue) --SUM(totals.totalTransactionRevenue)/1000000 AS
From `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170601' and '20170630' 
Group by time, trafficSource.source
Order by source asc, time asc;

Output: KHONG GIONG OUPUT LAM
 

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

với những bài như câu 3,4, lấy week vs month, purchase vs non_purchase thì mình sẽ làm theo hướng cte, rồi sau đó UNION hoặc join, tuỳ từng trường hợp

SELECT  
Format_date("%Y%m", Parse_date("%Y%m%d",date)) as month, 
Sum( Case When totals.transactions is not null Then totals.pageviews End) 
/ Count ( Case When totals.transactions is not null Then fullVisitorId End) as avg_pageviews_purchase, 
Sum( Case When totals.transactions is null Then totals.pageviews End) 
/ Count ( Case When totals.transactions is null Then fullVisitorId End) as avg_pageviews_non_purchase

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*` 
Where _table_suffix between '20170601' and '20170731' 
Group by month;

Cách 2:
-- Select 
-- format_date("%Y%m", parse_date("%Y%m%d",date)) as month,
-- avg(Case when totals.transactions is not null then totals.pageviews end),
-- avg(Case when totals.transactions is null then totals.pageviews end)

Output: KHONG GIONG OUPUT LAM
--
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month
 

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

Select 
Format_date ("%Y%m",Parse_date("%Y%m%d",date)) as month,
Avg(totals.transactions) as Avg_total_transactions_per_user
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
Where totals.transactions >= 1
Group by month;

Output: KHONG GIONG OUPUT LAM
a cắt nghĩa từng chữ nha :))), number of.... transactions  per user 
thì e phải có số bao nhiu user(1), rồi bao nhiu transactions(2), và lấy (2)/(1) để ra trung mình 1 user thì có bao nhiu trans á 

select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    sum(totals.transactions)/count(distinct fullvisitorid) as Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions>=1
group by month
 
-- Query 06: Average amount of money spent per session
#standardSQL
Query:
Select 
Format_date("%Y%m",Parse_date("%Y%m%d",date)) as month,
Avg(totals.totalTransactionRevenue) as avg_revenue_by_user_per_visit
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
Where totals.transactions is not null
Group by month;

Output: KHONG GIONG OUPUT LAM --tương tự ý tưởng câu trên nha
select
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    ((sum(totals.totalTransactionRevenue)/sum(totals.visits))/power(10,6)) as avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where  totals.transactions is not null
group by month
 

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

With customer_id as 
(Select  
fullvisitorid,
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
Unnest (hits) as hits,
Unnest (hits.product) as product                                    (1)
Where product.v2ProductName = "YouTube Men's Vintage Henley"
and product.productRevenue is not null)

Select 
product.v2ProductName, 
Sum(product.productQuantity) as quantity
From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
Unnest (hits) as hits,
Unnest (hits.product) as product
-- Where fullvisitorid in (
--   select fullvisitorid        --nếu e dùng where ....in () là subquery, thì e bỏ phần cte ở trên vào subquery luôn
--   from customer_id)
where fullvisitorid in (
                        Select  
                        fullvisitorid,
                        From `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` ,
                        Unnest (hits) as hits,
                        Unnest (hits.product) as product                                    (1)
                        Where product.v2ProductName = "YouTube Men's Vintage Henley"
                        and product.productRevenue is not null)
And product.v2ProductName <> "YouTube Men's Vintage Henley"
And product.productRevenue is not null
Group by product.v2ProductName
Order by product.v2ProductName, quantity;

cách 2 là e dùng join (inner join) để lấy phần giao 2 tập ID này

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC
 

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

With num_product_view_data as 
    (Select 
        Format_date("%Y%m",Parse_date("%Y%m%d",date)) as month,
        Count(h.eCommerceAction.action_type) as num_product_view
    From `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    Unnest (hits) as h
    Where _table_suffix between '20170101' and '20170331' 
    And h.eCommerceAction.action_type = '2'
    Group by month 
    Order by month),

num_addtocart_data as 
    (Select 
        Format_date("%Y%m",Parse_date("%Y%m%d",date)) as month,
        Count(h.eCommerceAction.action_type) as num_addtocart
    From `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    Unnest (hits) as h
    Where _table_suffix between '20170101' and '20170331' 
    And h.eCommerceAction.action_type = '3'
    Group by month 
    Order by month),

num_purchase_data as 
    (Select 
        Format_date("%Y%m",Parse_date("%Y%m%d",date)) as month,
        Count(h.eCommerceAction.action_type) as num_purchase
    From `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    Unnest (hits) as h
    Where _table_suffix between '20170101' and '20170331' 
    And h.eCommerceAction.action_type = '6'
    Group by month 
    Order by month)

Select 
    month, 
    num_product_view, 
    num_addtocart, 
    num_purchase,
    num_addtocart*100.0/num_product_view as add_to_cart_rate,
    num_purchase*100.0/num_product_view as purchase_rate
From num_product_view_data
Full join num_addtocart_data using (month)   --left join nha
Full join num_purchase_data using(month)

Order by month;

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data


 


