#curl -XDELETE localhost:9200/malml-sample
#curl -XDELETE localhost:9200/malml-upload
#
#curl -X PUT \
#  http://localhost:9200/malml-sample/_settings \
#  -H 'Cache-Control: no-cache' \
#  -H 'Content-Type: application/json' \
#  -H 'Postman-Token: 162d1588-acac-4882-b715-57029ce78bd5' \
#  -d '{
#	"index.mapping.total_fields.limit": 5000
#}'
#
#curl -X GET \
#  http://localhost:9200/malml-sample/_settings \
#  -H 'Cache-Control: no-cache' \
#  -H 'Content-Type: application/json' \
#  -H 'Postman-Token: e4406217-5945-4e20-b442-7ea02997d78f' \
#  -d '{
#	"index.mapping.total_fields.limit": 2000
#}'
#


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