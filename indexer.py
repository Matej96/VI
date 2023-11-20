import csv
from itertools import combinations
import os
from pathlib import Path

import lucene
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType, TextField
from org.apache.lucene.index import IndexOptions, IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher


def indexing():
    store = MMapDirectory(Paths.get('index'))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    input_file_name = 'books.csv'
    count = 0
    with open(input_file_name, mode='r', newline='') as file:
        csv_reader = csv.reader(file)

        for row in csv_reader:
            if count == 0:
                count += 1
                continue

            doc = Document()
            book_id = row[0]
            title = row[1]
            author = row[2]

            field_type = FieldType()
            field_type.setStored(True)
            field_type.setIndexOptions(IndexOptions.NONE)
            doc.add(Field("id", book_id, field_type))

            doc.add(Field("title", title, TextField.TYPE_STORED))
            doc.add(Field("author", author, TextField.TYPE_STORED))
            print(doc)

            writer.addDocument(doc)

            count += 1
            if count == 1000:
                break

        writer.commit()
        writer.close()


def searching():
    query_string = input('Book you are searching for: ')
    directory = MMapDirectory(Paths.get('index'))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()
    query = QueryParser("title", analyzer).parse(query_string)

    return searcher.search(query, 10).scoreDocs, searcher


if __name__ == "__main__":
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    indexing()
    while True:
        scoreDocs, searcher = searching()

        print("Found " + str(len(scoreDocs)) + " matches. \n")
        count = 0
        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            print("#" + str(count) + " [ID]: " + doc.get("id") + " [Title]: " + doc.get(
                "title") + " [Author]: " + doc.get("author"))
            print(scoreDoc)

            count += 1
