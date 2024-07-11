from elasticsearch import Elasticsearch

def query_data():
    es = Elasticsearch(['http://localhost:9200'])
    res = es.search(index="sensor_data", body={
        "query": {
            "range": {
                "timestamp": {
                    "gte": "now-1h/h"
                }
            }
        }
    })
    for hit in res['hits']['hits']:
        print(hit['_source'])

if __name__ == "__main__":
    query_data()
