function reset_index() {
    local indexName=${1}
    curl -XDELETE localhost:9200/${indexName}
curl -XPUT "localhost:9200/${indexName}?pretty" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 3,
            "number_of_replicas" : 2,
            "mapping.total_fields.limit": 5000
        }
    }
}
'
}

reset_index "malml-sample"
reset_index "malml-upload"