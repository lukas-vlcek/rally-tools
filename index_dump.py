#! /usr/bin/env python

from elasticsearch import Elasticsearch
import sys
import getopt
import json


def main(argv):
    """
    Utility to dump all documents from one index (and all its types).
    Outputs one document per line, no pretty-print, NL only between documents.

    Supported argv:
      index: name of index to dump data from
      host: Elasticsearch host:port ('localhost:9200')
      path: path to ssl certificates ('/opt/usr/elasticsearch')

    :param argv:
    :return:
    """

    index = None
    host = "logging-es:9200"
    scroll = "1m"
    size = 1000
    kwargs = {}

    try:
        opts, args = getopt.getopt(argv, "i:h:p", ["index=", "host=", "path"])
    except getopt.GetoptError as err:
        print err
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-i", "--index"):
            index = arg
        elif opt in ("-h", "--host"):
            host = arg
        elif opt in ("-p", "--path"):
            kwargs["use_ssl"] = True
            kwargs["verify_certs"] = True
            kwargs["ca_certs"] = opt+'/ca'
            kwargs["client_cert"] = opt+'/cert'
            kwargs["client_key"] = opt+'/key'

    es = Elasticsearch(
        [host],
        **kwargs
    )

    if index is None:
        print "Indices found in this cluster:"
        print es.cat.indices(format="text", v=True)
        print "No index specified for dump"
        sys.exit()
    else:
        print es.cat.indices(index=index, format="text", v=True)

    es_response = es.search(index=index, scroll=scroll, format="json", size=size)
    scroll_id = es_response["_scroll_id"]

    cycles = 1
    dump_hits(es_response["hits"]["hits"])

    es_response = es.scroll(scroll_id=scroll_id, scroll=scroll)
    while es_response["hits"]["hits"]:
        cycles += 1
        dump_hits(es_response["hits"]["hits"])
        es_response = es.scroll(scroll_id=scroll_id, scroll=scroll)


def dump_hits(hits):
    for doc in hits:
        print json.dumps(doc["_source"], sort_keys=True, separators=(',', ': '))


if __name__ == "__main__":
    main(sys.argv[1:])
