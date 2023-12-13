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

CSV_INPUT_FILE = 'extended_output.csv'


# funkcia sluziaca na zindexovanie predom nazbieranych dat
def indexing():
    store = MMapDirectory(Paths.get('index'))
    analyzer = StandardAnalyzer()
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
    writer = IndexWriter(store, config)

    input_file_name = 'extended_output.csv'
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
            publisher = row[3]
            publish_date = row[4]
            isbn = row[5]
            rating = row[6]
            founded = round(float(row[7])) if row[7] else ''

            doc.add(Field("id", book_id, TextField.TYPE_STORED))
            doc.add(Field("title", title, TextField.TYPE_STORED))
            doc.add(Field("author", author, TextField.TYPE_STORED))
            doc.add(Field("publisher", publisher, TextField.TYPE_STORED))
            doc.add(Field("publish_date", publish_date, TextField.TYPE_STORED))
            doc.add(Field("isbn", isbn, TextField.TYPE_STORED))
            doc.add(Field("rating", rating, TextField.TYPE_STORED))
            doc.add(Field("founded", founded, TextField.TYPE_STORED))

            writer.addDocument(doc)

            count += 1

        writer.commit()
        writer.close()

    print("Indexing done.\n")

# metoda sluziaca na prehladavanie v predom vytvorenom indexe
def searching(query_string):
    directory = MMapDirectory(Paths.get('index'))
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()
    query = QueryParser("title", analyzer).parse(query_string)

    return searcher.search(query, 100).scoreDocs, searcher

# hlavna metoda, ktora spusta cely program, kde v nekonecnom cykle caka na dopyt od pouzivatela
def main():
    while True:
        query_string = input('Which book are you searching for (press ENTER to exit): ')

        if query_string == '':
            break

        scoreDocs, searcher = searching(query_string)

        print("Found " + str(len(scoreDocs)) + " matches. \n")
        count = 0
        for scoreDoc in scoreDocs:
            doc = searcher.doc(scoreDoc.doc)
            print("Result #" + str(count) + ":\n[Book ID]:              " + doc.get(
                "id") + " \n[Title]:                " + (
                              doc.get("title") or "Unavailable") + " \n[Author]:               " + (
                              doc.get("author") or "Unavailable") + " \n[Publisher]:            " + (
                              doc.get("publisher") or "Unavailable") + "\n[Publisher founded in]: " + (
                              doc.get("founded") or "Unavailable"))
            print()

            count += 1


if __name__ == "__main__":
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print(
        '*** INFO ***\nWelcome, this program is able to search through the book data retrieved from the book database openlibrary.org. First, wait for the indexing to end, then you can enter the name of the book you are searching for. The program will provide info about the book and additional information from Wikipedia. \n*** INFO ***\n')
    print("Please wait, indexing in progress...")
    indexing()
    main()
