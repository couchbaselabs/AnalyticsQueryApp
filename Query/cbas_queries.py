# -*- coding: utf-8 -*-
cbas_queries = {
  "gideon_queries": [
    "select result from {0}.{1} where result is not null LIMIT 50000",
    "select claim from {0}.{1} where claim is not null limit 100",
    "select result,AVG(rating),MIN(rating),MAX(rating) from {0}.{1} where result is not missing and rating >= 400 and rating <= 1000 group by result",
    "select result,SUM(rating) from {0}.{1} where result is not null and rating >= 115 and rating <= 125 group by result",
    "select result,SUM(rating) from {0}.{1} where result is not null and rating >= 410 and rating <= 420 group by result",
    "select round(min(rating),2) as min_rating,round(max(rating),2) as max_rating,round(avg(rating),2) as avg_rating from {0}.{1} where result is not null and rating between 500 and 520]"
  ],
  "catapult_queries": [
    "select meta().id from {0}.{1} where country is not null and `type` is not null and (any r in {1}.reviews satisfies r.ratings.`Check in / front desk` is not null end) limit 100",
    "select avg(price) as AvgPrice, min(price) as MinPrice, max(price) as MaxPrice from {0}.{1} where free_breakfast=True and free_parking=True and price is not null and array_count(public_likes)>5 and `type`='Hotel' group by country limit 100",
    "select city,country,count(*) from {0}.{1} where free_breakfast=True and free_parking=True group by country,city order by country,city limit 100 offset 100"
  ]
}
