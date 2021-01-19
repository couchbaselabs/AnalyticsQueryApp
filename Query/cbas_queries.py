cbas_queries = {
  "flex_index_queries": [
    "select result,AVG(rating),MIN(rating),MAX(rating) from {0} use index (using fts) where  rating >= 200 and rating <= 1000 and `type`=\"gideon\" group by result limit 1000",
    "select result,SUM(rating) from {0} use index (using fts) where result =\"SUCCESS\" and rating >= 115 and rating <= 250 and `type`=\"gideon\" group by result limit 100",
    "SELECT * FROM {0} USE INDEX (USING FTS) WHERE result=\"SUCCESS\" and ANY v IN sizes SATISFIES (v > 100 and v < 200) END and `type`=\"gideon\" limit 200",
    "SELECT * FROM {0} WHERE ANY v IN sizes SATISFIES (v > 100 and v < 200) and `type`=\"gideon\" limit 250",
    "SELECT * FROM {0} USE INDEX (USING FTS) WHERE result=\"SUCCESS\" and (ANY v IN sizes SATISFIES (v > 100 and v < 200) END) and (any x in activity satisfies x > \"A\" and x < \"Z\"  END) and `type`=\"gideon\" limit 500",
    "SELECT city, active_hosts[0], FROM {0} USE INDEX (USING FTS) WHERE result in [\"SUCCESS\",\"FAILURE\"] and (ANY v IN sizes SATISFIES (v > 100 and v < 200) END) and (any x in activity satisfies x > \"A\" and x < \"Z\"  END) and ((build_id > 3000 and build_id < 5000) OR (build_id > 8000 and build_id < 9000)) order by city desc offset 10000 limit 200",
    "select t1.city, t1.priority from (select b2.* from {0} b2 use index (using fts) where b2.rating>200 and b2.rating<300 ) as t1 where t1.build_id > 4000 and t1.build_id < 5000 limit 300"
  ],
  "volume_queries": [
    "SELECT cid from {0} WHERE cid is not null order by cid LIMIT 1000",
    "SELECT RAW meta().id FROM {0} WHERE type=\"links\" AND owner=\"CC\" AND ANY v IN idents.name.names SATISFIES  ([v.`first`, v.`last`] = [\"Fannie\", \"McDermott\"]) END order by meta().id LIMIT 1000",
    "SELECT RAW meta().id FROM {0} WHERE type=\"links\" AND owner=\"AA\" AND ANY v IN idents.name.names SATISFIES (v.`first`=\"Elizabeth\") END order by meta().id LIMIT 1000",
    "SELECT RAW meta().id FROM {0} WHERE type=\"links\" AND owner=\"AA\" AND ANY v IN idents.name.names SATISFIES (v.`last` =\"Bernhard\") END order by meta().id LIMIT 1000",
    "SELECT cid,owner from {0} WHERE type = \"links\" AND ANY v in leads SATISFIES v.id like \"CCC::XX::YYY:000::1%\" END order by owner LIMIT 1000",
    "SELECT cid,meta().id,idents.cont.Phones from {0} WHERE type = \"links\" AND ANY v in idents.cont.Phones SATISFIES v.Country < 500 END order by cid LIMIT 1000",
    "SELECT cid,owner,idents.cont.Emails from {0} WHERE type = \"links\" AND owner = \"BB\" AND ANY v in idents.cont.Emails SATISFIES v.email = \"Jody_Okuneva@hotmail.com\" END order by owner LIMIT 1000",
    "SELECT meta().id,owner from {0} where type = \"links\" order by owner limit 1000",
    "SELECT idents.accts.profids.id,owner from {0} WHERE type = \"links\" AND owner = \"AA\" AND ANY v in idents.accts.profids SATISFIES v.id < \"GG\" END order by idents.accts.profids.id LIMIT 1000",
    "SELECT idents.accts.profids,owner from {0} WHERE type = \"links\" AND ANY v in idents.accts.profids SATISFIES v.id > \"GG\" END order by owner LIMIT 1000",
    "SELECT idents.accts.mems.memid,owner from {0} WHERE type = \"links\" AND owner = \"AA\" AND ANY v in idents.accts.mems SATISFIES v.memid between 100 and 300 END order by idents.accts.mems.memid limit 1000",
    "SELECT cid,meta().id,idents.cont.Phones from {0} WHERE type = \"links\" AND owner = \"AA\" AND ANY v in idents.cont.Phones SATISFIES v.`Number` = \"1-961-612-1069\" END order by cid LIMIT 1000",
    "SELECT owner,leads.alies[0] FROM {0} where type = \"links\" AND owner = \"AA\" AND ANY v in leads SATISFIES (ANY x in v.alies SATISFIES x like \"BBB::XX::YYY::000:%\" END) END order by leads.alies[0] limit 1000",
    "SELECT RAW meta().id FROM {0} WHERE type=\"links\" AND owner=\"AA\" AND ANY v IN idents.name.names SATISFIES  ([v.`first`, v.`last`] = [\"Adele\", “McCullough\"]) END order by meta().id LIMIT 1000",
    "SELECT RAW meta().id FROM {0} WHERE type=\"links\" AND owner=\"BB\" AND ANY v IN idents.name.names SATISFIES (v.`first`=\"Columbus\") END order by meta().id LIMIT 1000",
    "SELECT cid,owner from {0} WHERE type = \"links\" AND owner=\"AA\" AND ANY v in leads SATISFIES v.id like \"CCC::XX::YYY:000::%\" END order by owner LIMIT 1000",
    "SELECT cid,meta().id,idents.cont.Phones from {0} WHERE type = \"links\" AND owner=\"AA\" AND ANY v in idents.cont.Phones SATISFIES v.Country > 600 END order by cid LIMIT 1000",
    "SELECT cid,owner,idents.cont.Emails from {0} WHERE type = \"links\" AND owner = \"AA\" AND ANY v in idents.cont.Emails SATISFIES v.email = \"Angeline.Moore@gmail.com\" END order by owner LIMIT 1000",
    "SELECT meta().id from {0} WHERE owner = \"AB\" order by meta().id limit 1000",
    "SELECT owner,cid from {0} WHERE type = \"links\" and owner = \"ZZ\" order by owner limit 1000",
    "SELECT ARRAY_SORT(idents.accts.mems[*].memid) AS memids,owner from {0} WHERE type = \"links\" AND ANY v in idents.accts.mems SATISFIES v.memid between 10 and 250 END order by owner,memids limit 1000",
    "SELECT cid,meta().id,idents.cont.Phones from {0} WHERE type = \"links\" AND ANY v in idents.cont.Phones SATISFIES v.`Number` = \"1-961-612-1069\" END order by cid LIMIT 1000",
    "SELECT idents.accts.mems,owner from {0} WHERE type = \"links\" AND owner = \"AA\" AND ANY v in idents.accts.mems SATISFIES v.progid between \"BB\" and \"FF\" END order by idents.accts.mems limit 1000",
    "SELECT idents.accts.mems,owner from {0} WHERE type = \"links\" AND owner = \"XY\" AND ANY v in idents.accts.mems SATISFIES v.progid between \"TT\" and \"ZZ\" END order by idents.accts.mems limit 1000",
    "SELECT owner,leads.alies[0] FROM {0} where type = \"links\" AND ANY v in leads SATISFIES (ANY x in v.alies SATISFIES x like \"BBB::XX::YYY::000:%\" END) END order by leads.alies[0] limit 1000"
  ],
  "common_queries": [
    "select result from {0} where result is not null LIMIT 50000",
    "select claim from {0} where claim is not null limit 100",
    "select result,AVG(rating),MIN(rating),MAX(rating) from {0} where result is not missing and rating >= 400 and rating <= 1000 group by result",
    "select result,SUM(rating) from {0} where result is not null and rating >= 115 and rating <= 125 group by result",
    "select result,SUM(rating) from {0} where result is not null and rating >= 410 and rating <= 420 group by result",
    "select round(min(rating),2) as min_rating,round(max(rating),2) as max_rating,round(avg(rating),2) as avg_rating from {0} where result is not null and rating between 500 and 520]"
  ],
  "catapult_queries": []
}