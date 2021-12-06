--q5.sql--

 WITH ssr AS
  (SELECT s_store_id,
          sum(sales_price) as sales,
          sum(profit) as profit,
          sum(return_amt) as returns,
          sum(net_loss) as profit_loss
  FROM
    (SELECT ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    FROM store_sales
    UNION ALL
    SELECT sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    FROM store_returns)
    salesreturns, date_dim, store
  WHERE date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and ((cast('2000-08-23' as date) + interval '14' day))
       and store_sk = s_store_sk
 GROUP BY s_store_id),
 csr AS
 (SELECT cp_catalog_page_id,
         sum(sales_price) as sales,
         sum(profit) as profit,
         sum(return_amt) as returns,
         sum(net_loss) as profit_loss
 FROM
   (SELECT cs_catalog_page_sk as page_sk,
           cs_sold_date_sk  as date_sk,
           cs_ext_sales_price as sales_price,
           cs_net_profit as profit,
           cast(0 as decimal(7,2)) as return_amt,
           cast(0 as decimal(7,2)) as net_loss
    FROM catalog_sales
    UNION ALL
    SELECT cr_catalog_page_sk as page_sk,
           cr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           cr_return_amount as return_amt,
           cr_net_loss as net_loss
    from catalog_returns
   ) salesreturns, date_dim, catalog_page
 WHERE date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and ((cast('2000-08-23' as date) + interval '14' day))
       and page_sk = cp_catalog_page_sk
 GROUP BY cp_catalog_page_id)
 ,
 wsr AS
 (SELECT web_site_id,
         sum(sales_price) as sales,
         sum(profit) as profit,
         sum(return_amt) as returns,
         sum(net_loss) as profit_loss
 from
  (select  ws_web_site_sk as wsr_web_site_sk,
            ws_sold_date_sk  as date_sk,
            ws_ext_sales_price as sales_price,
            ws_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from web_sales
    union all
    select ws_web_site_sk as wsr_web_site_sk,
           wr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           wr_return_amt as return_amt,
           wr_net_loss as net_loss
    FROM web_returns LEFT  OUTER JOIN web_sales on
         ( wr_item_sk = ws_item_sk
           and wr_order_number = ws_order_number)
   ) salesreturns, date_dim, web_site
 WHERE date_sk = d_date_sk
       and d_date between cast('2000-08-23' as date)
                  and ((cast('2000-08-23' as date) + interval '14' day))
       and wsr_web_site_sk = web_site_sk
 GROUP BY web_site_id)
 SELECT channel,
        id,
        sum(sales) as sales,
        sum(returns) as returns,
        sum(profit) as profit
 from
 (select 'store channel' as channel,
         concat('store', s_store_id) as id,
         sales,
         returns,
        (profit - profit_loss) as profit
 FROM ssr
 UNION ALL
 select 'catalog channel' as channel,
        concat('catalog_page', cp_catalog_page_id) as id,
        sales,
        returns,
        (profit - profit_loss) as profit
 FROM  csr
 UNION ALL
 SELECT 'web channel' as channel,
        concat('web_site', web_site_id) as id,
        sales,
        returns,
        (profit - profit_loss) as profit
 FROM wsr
 ) x
 GROUP BY ROLLUP (channel, id)
 ORDER BY channel, id
 LIMIT 100
            
