# RWA
Solution to take home assigment of rwa.xyz

## Description
Repo contents : 
- RWA.ipynb : A Databricks Notebook with queries and functions necessary to generate data for most transferred ERC-20 token per week in 2023 based on transfer metrics (count of transfers / total amount transferred)
- output : directory containing outputs from above notebook
-  output/sum.csv : resultant data in csv with schema (month, token address, transfer metric),  based on transfer metric - total transferred amount 
-  output/count.csv : resultant data in csv with schema (month, token address, transfer metric),  based on transfer metric - total number of transfers 

## Improvements

Based on the analysis from queries in notebook, we can simply program a query generator function with timeframe as argument and necessary logic to support that timeframe

```sql
WITH transfer AS (
            SELECT 
                <units of timeframe>,
                t.address, 
                <aggregation of choice for transfer metric>
            FROM ethereum_logs e
            JOIN tokens t ON t.address = e.address
            WHERE e.topic0 = '{}' 
            AND year(e.block_date) = '2023' 
            AND e.params like '%\"value\"%'
            GROUP BY 
            <unit of timeframe>,
            t.address
        ),
        ranked AS (
            SELECT 
                <units of timeframe>,
                name, 
                address, 
                transfer_metric,
                ROW_NUMBER() OVER (PARTITION BY <units of timeframe> ORDER BY transfer_metric DESC) AS rn
            FROM transfer
        )
        SELECT <units of timeframe>, name, address, transfer_metric
        FROM ranked
        WHERE rn = 1 
        ORDER BY <units of timeframe>
```

For example, lets take daily and average transfer amount for which the query generator would give : 

```sql
WITH transfer AS (
            SELECT 
                e.block_date, -- <units of timeframe>,
                t.address, 
                avg(cast(get_json_object(params, 'value') as decimal(38,2)) as transfer_metric -- <aggregation of choice for transfer metric>
            FROM ethereum_logs e
            JOIN tokens t ON t.address = e.address
            WHERE e.topic0 = '{}' 
            AND year(e.block_date) = '2023' 
            AND e.params like '%\"value\"%'
            GROUP BY 
            e.block_date, --<unit of timeframe>,
            t.address
        ),
        ranked AS (
            SELECT 
                block_date, --<units of timeframe>,
                name, 
                address, 
                transfer_metric,
                ROW_NUMBER() OVER (PARTITION BY block_date /* <units of timeframe> */ ORDER BY transfer_metric DESC) AS rn
            FROM transfer
        )
        SELECT block_date /* <units of timeframe> */, name, address, transfer_metric
        FROM ranked
        WHERE rn = 1 
        ORDER BY block_date -- <units of timeframe>
```

This ca be done by a simple python script. 
In addition , we can also manipulate the  topic so we can aggregate other types of events if need be.


## API

We can expose a GET endpoint with query parameter being the timeframe.
For example, 
```
GET /most_transferred_tokens?timeframe=weekly
```
The question becomes would we execute the aggregation for every call. 
Obviously it would cause a huge on our compute resources.
Instead , we support few standard timeframes as in daily(1D) , weekly(1W) , monthly (1M).
And generate data for them in different tables : e_most_trans_tok_d,  e_most_trans_tok_w,  e_most_trans_tok_m.
Note that data of e_most_trans_tok_d cannot be used for e_most_trans_tok_w because most transferred token aggregations will have different values for daily and month.
Similarly for monthly and yearly.
