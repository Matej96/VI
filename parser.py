import csv
import os
import re

folder_path = 'raw_html_pages'
output_csv_file_name = 'books.csv'
count = 0

# v tomto skripte sa nacrawlovane data parsuju a ukladaju do suboru v CSV formate
with open(output_csv_file_name, mode='w', newline='') as output_file:
    header = ['ID', 'Title', 'Author', 'Publisher', 'Date of Publishment', 'ISBN', 'Rating']
    csv_writer = csv.writer(output_file)
    csv_writer.writerow(header)

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)

        try:
            with open(file_path, 'r') as file:
                file_content = file.read()
                file_content.replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'')
        except Exception as e:
            print(f"An error occurred: {e}")

        book_id = filename.split('.')[0]
        title = re.search(r'<h1 class="work-title" itemprop="name">(.*?)</h1>', file_content, flags=re.DOTALL)
        if title is None:
            continue
        else:
            title = title.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')
            title = " ".join(title.split())

        publish_date = re.search(r'<span itemprop="datePublished">(.*?)</span>', file_content)
        if publish_date:
            publish_date = publish_date.group(1)
            publish_date = " ".join(publish_date.split())
        isbn = re.search(r'itemprop=\"isbn\"> *(\d+),?.*?</dd>', file_content)
        if isbn:
            isbn = int(isbn.group(1).replace(' ', ''))
        rating = re.search(r'itemprop="ratingValue">(.*?)</span>', file_content)
        if rating:
            rating = float(rating.group(1))
        author = re.search(r'itemprop="author">(.*?)</a>', file_content)
        if author:
            author = author.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')
            author = " ".join(author.split())
        publisher = re.search(r'itemprop="publisher".*?>(.*?)</a>', file_content)
        if publisher:
            publisher = publisher.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')
            publisher = " ".join(publisher.split())
        header = ['ID', 'Title', 'Author', 'Publisher', 'Date of Publishment', 'ISBN', 'Rating']

        csv_writer.writerow([book_id, title, author, publisher, publish_date, isbn, rating])
        count += 1

        if count % 1000 == 0:
            print('Files processed: ' + str(count))
