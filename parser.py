import csv
import os
import re

folder_path = 'raw_html_pages'
output_csv_file_name = 'books.csv'
count = 0

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
                # print(file_content)
                # break
        except Exception as e:
            print(f"An error occurred: {e}")

        # print(filename)
        book_id = filename.split('.')[0]
        title = re.search(r'<h1 class="work-title" itemprop="name">(.*?)</h1>', file_content, flags=re.DOTALL)
        if title is None:
            continue
        else:
            title = title.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')

        publish_date = re.search(r'<span itemprop="datePublished">(.*?)</span>', file_content)
        if publish_date:
            publish_date = publish_date.group(1)
        isbn = re.search(r'itemprop=\"isbn\"> *(\d+),?.*?</dd>', file_content)
        if isbn:
            isbn = isbn.group(1).replace(' ', '')
        rating = re.search(r'itemprop="ratingValue">(.*?)</span>', file_content)
        if rating:
            rating = float(rating.group(1))
        author = re.search(r'itemprop="author">(.*?)</a>', file_content)
        if author:
            author = author.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')
        publisher = re.search(r'itemprop="publisher".*?>(.*?)</a>', file_content)
        if publisher:
            publisher = publisher.group(1).replace('\n', '').replace('&amp;', '&').replace('&#39;', '\'').replace('&quot;', '"')
        header = ['ID', 'Title', 'Author', 'Publisher', 'Date of Publishment', 'ISBN', 'Rating']

        csv_writer.writerow([book_id, title, author, publisher, publish_date, isbn, rating])
        # print(book_id, title, publish_year, isbn, publisher, author, rating, sep='\n', end='\n\n')
        count += 1
        if count % 1000 == 0:
            print('Files processed: ' + str(count))
