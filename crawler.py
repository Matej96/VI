import re
import os
import requests

def create_folder_for_raw_data(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Folder '{folder_path}' created.")
    else:
        print(f"Folder '{folder_path}' already exists.")


root_url = 'https://openlibrary.org'
create_folder_for_raw_data('raw_html_pages')

count = 380
book_ids = []
duplicates = 0
while True:
    print("Processing page #" + str(count + 1))
    search_page_url = 'https://openlibrary.org/search?subject=Fantasy&sort=rating&page=' + str(count + 1)
    search_page = requests.get(search_page_url).text.replace('\n', ' ')
    items = re.findall('<span class="bookcover ">.*?</span>', search_page)
    for item in items:
        if book_id := re.search(r'href="/works/([^/?"]+)', item).group(1):
            if os.path.exists('raw_html_pages/' + book_id + '.txt'):
                print("[DUPLICATE # " + str(duplicates) + "] Item ID:" + book_id)
                duplicates += 1
                continue
            response = requests.get(root_url + '/works/' + book_id).text.replace('\n', ' ')
            file_name = 'raw_html_pages/' + book_id + '.txt'
            with open(file_name, "w") as file:
                file.write(response)
            print('Item ID: ' + book_id + " done URL: " + root_url + "/works/" + book_id)
    count += 1