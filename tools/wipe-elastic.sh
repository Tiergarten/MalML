INSTALL_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

function reset_index() {
    local indexName=${1}
    curl -XDELETE localhost:9200/${indexName}
curl -XPUT "localhost:9200/${indexName}?pretty" -H 'Content-Type: application/json' -d'
{
    "settings" : {
        "index" : {
            "number_of_shards" : 3,
            "number_of_replicas" : 2,
            "mapping.total_fields.limit": 1000 
        }
    }
}
'
}

reset_index "malml-sample"
reset_index "malml-upload"

source ${INSTALL_DIR}/set_python_path.sh

python $(cygpath -w "${INSTALL_DIR}/../sample_mgr/sample_mgr.py") -e
python $(cygpath -w "${INSTALL_DIR}/../common.py")