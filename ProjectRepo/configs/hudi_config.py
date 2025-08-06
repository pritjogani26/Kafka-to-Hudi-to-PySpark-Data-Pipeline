# configs/hudi_config.py

HUDI_OPTIONS = {
    "hoodie.table.name": "users",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "ingestion_time",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.hive_style_partitioning": "false",
    "hoodie.datasource.write.partitionpath.field": "country,state,city",
    "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
    "hoodie.index.type": "BLOOM",
    "hoodie.bloom.index.update.partition.path": "true",
    "hoodie.compact.inline": "false",
    "hoodie.clustering.inline": "false",
    "hoodie.layout.optim.enable": "false",
    "hoodie.parquet.max.file.size": "134217728",
    "hoodie.copyonwrite.insert.parallelism": "100",
    "hoodie.upsert.shuffle.parallelism": "100",
    "hoodie.bulkinsert.shuffle.parallelism": "100",

    "hoodie.parquet.small.file.limit": "134217728",
    "hoodie.index.bloom.max.entries": "1000000",
}

HUDI_BASE_PATH = "/hudi_output"
