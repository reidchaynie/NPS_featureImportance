with first_purchase_products_2022 as (
select 
  order_id 
  ,listagg(distinct product_name,',') as product_name_
  ,listagg(distinct product_id,',') as product_id_ 
  ,purchasing_household_id  
  ,min(order_placed) as first_order_placed
  ,margin_Rating 
from 
  PROD_MART_GOLDBELLY_DB.ANALYTICS_MART.LINE_ITEM_DERIVED_DETAILS
left join 
    PROD_MART_GOLDBELLY_DB.ANALYTICS_MART.MERCHANT_MARGIN_RATINGS on LINE_ITEM_DERIVED_DETAILS.merchant_id = merchant_margin_ratings.merchant_id  
where 
  is_first_order 
  and year(order_placed) = 2022
  and month(order_placed) < 9
  and product_id not in (26945, 24586) -- removes gift card purchases, which is a tremendous outlier that skews the data 
  and product_id in (select id from PROD_MART_GOLDBELLY_DB.ANALYTICS_MART.products where is_featured and is_Available) -- narrows to currently active products  
  and not IS_SUBSCRIPTION_OG and not IS_SUBSCRIPTION_FULFILLMENT
  and not IS_CORP_ORDER
and not IS_CANCELED 
group by 1,4,6
having 
  product_id_ not like '%,%' --removes orders containing multiple products
),
counts_of_first_Purchase_by_product_2022 as (
  select 
    product_name_ 
    ,1 as join_on_me -- the next CTE joins the same value to every record in this CTE, so I am using this join key 
    ,count(distinct PURCHASING_HOUSEHOLD_ID) as num_first_Time_purchase 
  from
    first_purchase_products_2022 
  group by 1,2
  having num_first_Time_purchase >= 200 -- this removes products beneath a min volume threshold. If an order has 80% retention on 5 orders, who cares
),
first_time_factor as (
  select 
        1 as join_on_me
        ,max(num_first_time_purchase) as max_purchases -- the most first-time purchases belonging to a product 
  from
    counts_of_first_Purchase_by_product_2022
  group by 1 
),
volume_Factor_calc_2022 as (
  select 
      product_name_
      ,num_first_Time_purchase
      ,(num_first_Time_purchase/max_purchases) 
          as first_Time_purchase_factor  
  from 
    counts_of_first_Purchase_by_product_2022
  left join 
    first_time_factor on counts_of_first_Purchase_by_product_2022.join_on_me = first_Time_factor.join_on_me
),
follow_up_counts_setup as (
  select 
    purchasing_household_id 
    ,order_id
    ,min(order_placed) as order_Placed 
    from
    PROD_MART_GOLDBELLY_DB.ANALYTICS_MART.LINE_ITEM_DERIVED_DETAILS
  where 
    PURCHASING_HOUSEHOLD_ID  in 
      (select PURCHASING_HOUSEHOLD_ID from first_purchase_products_2022)
  group by 1,2
),
-- here we only want to count a purchase if it came in within 6 months of the first purchase 
-- this is so we can compare records from 2022 apples:apples with records in 2023 
order_range as (
  select 
    PURCHASING_HOUSEHOLD_ID
    ,min(order_placed) as first_order_placed
    ,dateadd('month',8,first_order_placed) as end_of_range
  from
    follow_up_counts_setup
  group by 1 
),
follow_up_counts as (
  select 
    follow_up_counts_setup.PURCHASING_HOUSEHOLD_ID 
    ,count(distinct order_id) - 1 as total_follow_up_orders -- minus 1 becuase we dont want to count the initial purchase 
    ,case when total_follow_up_orders = 0 then true else false end as is_oad -- if 0 then no follow-up purchases: One-and-done (oad)
  from
    follow_up_counts_setup
  join 
    order_range on follow_up_counts_setup.purchasing_household_id = order_range.purchasing_household_id 
  where 
    order_placed <= end_of_range -- limits to first 6 months 
  group by 1
  ),
retention_counts as (
  select 
    product_name_
    ,product_id_
    ,margin_Rating
    ,count(follow_up_counts.purchasing_household_id) as total_purchasers -- this is to limit by 200 first time purchasers like we did above  
    ,count(distinct case when is_oad then follow_up_counts.purchasing_household_id end) as total_oad 
    ,count(distinct case when not is_oad then follow_up_counts.purchasing_household_id end) as total_retained 
  from 
    first_purchase_products_2022
  left join
    follow_up_counts on first_purchase_products_2022.purchasing_household_id = follow_up_counts.purchasing_household_id 
  group by 1,2,3
  having total_purchasers >= 200 
),
--poorly titled cte brings data together and calculates percent retained 
final as (
select 
  1 as join_on_me 
  ,retention_counts.product_name_
  ,retention_counts.product_id_
  ,margin_rating
  ,total_oad 
  ,total_Retained  
  ,first_Time_purchase_factor
  ,num_first_Time_purchase
  ,total_retained/num_first_Time_purchase as pct_retained

from 
  retention_counts 
left join volume_Factor_calc_2022 on retention_counts.product_name_ = volume_Factor_calc_2022.product_name_)
,
-- here we are calculating max retention rate, just like we calculated max purchase volume. 
-- This is so we can scale products against eachother based on retention
max_retained as (
  select 
    1 as join_on_me 
    ,max(pct_retained) as max_retention 
  from 
    final 
  group by 1 
),
-- poorly titled CTE2 to calculate retention factor 
final_ as (
  select 
    final.* 
    ,max_Retention
    ,(pct_retained/max_retention) as retention_factor 
from
  final 
  left join max_retained on final.join_on_me = max_retained.join_on_me
)



-- output
-- score calc 
select 
  product_id_
  ,product_name_  
  ,num_first_time_purchase
  ,total_Retained 
  ,pct_retained
  ,first_time_purchase_factor 
  ,retention_factor
  ,round((first_time_purchase_factor + (retention_factor+retention_factor))/3,2)*100 as score_ 
  ,margin_rating 
from 
  final_
