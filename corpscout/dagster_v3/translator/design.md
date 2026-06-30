First I think two temporal workflows (create queue and translation are wrong). I think when we update translator package and add support for new language that should automatically create queue for that source (like norway_brreg). So we don't need from dagster to activate workflow to create that queue. 

Second thing is that we should create suorce queue dst queue (two parquet files). 

Alogirtham should be 
when we trigger start translation. 

workflow action should check is there input queue? If not, trigger for that specific queue small query that will create whole queue from clickhouse for that transalction, 

queue should have [source_text, destination_text, source_text_hash source language, destination langauge]

That source_text_hash should be cityHash64 from clickhouse that should be calculated directly on clickhouse when we build parquet file. 
rest of configurations shouldn't be in that columns per text. Since we have parquet file par source. 

after that worker algoirthma takes batch of the elements from queue, send them to translator. Wait for response

If response fail shouw fail to temporal and temporal should retry (up to 10 times)

if response is ok, store results in output queue and remove these elements from input queue. Output queue should have same columns as input queue. Just different file 
when we don't have elements in input queue file, we trigger specificn norway_breeg function. store output queue in the clickhouse text_translations table.

queueu central should have method to initialized queue (started from each individual package, in our case norway_brreg) with all configuration that are needed fro that queue (static config in norway_brreg). 
Should include 
- translation_endpoint_id 
these translation points with Id should be configured in central translator package as a config 
[{ 
    unique_name: xyz
    model:
    ipaddres:
    api ..
    authe ${env}
}]

from norway_brreg when initiazled queue we should use that id, that is staticly fixed when we build norway_brreg and run on each startup, central queue package if already have registred queue for that will just ignore that request.  Initialization of the queue means creating queue input file by dumping content from clickhouse to it, and activate queue by sending that path to the queue provider with Start method or something like that.




