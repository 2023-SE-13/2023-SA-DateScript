curl -X PUT "elastic:yXC0ZTAbjmhmyLHb7fBv@127.0.0.1:9200/works" -H "Content-Type: application/json" -d'
{
  "mappings": {
    "properties": {
      "doi": { "type": "text" },
      "title": { "type": "text" },
      "display_name": { "type": "text" },
      "publication_year": { "type": "long" },
      "publication_date": { "type": "text" },
      "language": { "type": "keyword" },
      "primary_location": { 
        "type": "object"
      },
      "type": { "type": "keyword" },
      "authorships": { 
        "type": "nested"
      },
      "countries_distinct_count": { "type": "long" },
      "institutions_distinct_count": { "type": "long" },
      "cited_by_count": { "type": "long" },
      "keywords": { 
        "type": "nested",
        "properties": {
          "keyword": {
            "type": "text"
          },
          "score": {
            "type": "float"
          }
        }
      },
      "referenced_works_count": { "type": "long" },
      "referenced_works": { "type": "keyword" },
      "related_works": { "type": "keyword" },
      "counts_by_year": { 
        "type": "nested"
      },
      "updated_date": { "type": "date" },
      "created_date": { "type": "date" },
      "abstract_inverted_index": {
        "type": "object",
        "properties": {
          "word": {
            "type": "keyword"
          },
          "positions": {
            "type": "integer"
          }
        }
      },
      "concepts": { "type": "nested" },
      "collected_num": { "type": "long" }
    }
  }
}'
