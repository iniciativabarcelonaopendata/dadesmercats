import re
import time
from urllib import parse

from bs4 import BeautifulSoup, NavigableString

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import mysql.connector

# CONFIGURATION PARAMETERS

pathToChrome = "/home/aabella/programs/chromedriver/chromedriver"  # path to the executable version of chrome browser for webdriver


# ENTRY POINT


def scraping(request):
    # Load env variables for database connection
    DIBA_USERNAME = "XX"
    DIBA_PASSWORD = "XX"
    DIBA_HOST = "XX"
    DIBA_PORT = "XX"
    DIBA_DATABASE = "XX"

    # Obtain connection variable
    cnx = mysql.connector.connect(
        user=DIBA_USERNAME,
        password=DIBA_PASSWORD,
        host=DIBA_HOST,
        port=DIBA_PORT,
        database=DIBA_DATABASE
    )
    cur = cnx.cursor()

    # Load markets from database and return a list of tuples containing:
    # [
    #   (market_1_id, market_1_name, market_1_lat, market_1_lng),
    #   ...
    #   (market_n_id, market_n_name, market_n_lat, market_n_lng)
    # ]
    mercats = load_mercats(cnx)
    cnx.close()
    print(mercats)
    limit = 133
    mercats = mercats[:min(limit, len(mercats))]

    mercats_str = [mercat[1] for mercat in mercats]

    popular_times = scrape_all_locations(mercats_str)

    # Preparing to insert popular times into the database
    popular_times_to_insert = []
    days = ["Diumenge", "Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte"]
    hours = range(6, 24)  # Only these hours are shown in popular times graphs
    ids = [mercat[0] for mercat in mercats]
    names = [mercat[1] for mercat in mercats]

    for i in range(len(mercats)):
        for day in days:
            for hour in hours:
                id_ = ids[i]
                location = names[i]
                value = popular_times[location][day][hour - 6]
                popular_times_to_insert.append((id_, day, hour, value))

    # Inserting into the database
    query = "INSERT INTO populartimes (mercat_id, day_of_week, hour, value) VALUES (%s, %s, %s, %s);"
    cnx = mysql.connector.connect(
        user=DIBA_USERNAME,
        password=DIBA_PASSWORD,
        host=DIBA_HOST,
        port=DIBA_PORT,
        database=DIBA_DATABASE
    )
    cur = cnx.cursor()
    cur.executemany(query, popular_times_to_insert)

    cnx.close()

    return True


# FUNCTIONS


def load_mercats(cnx):
    cur = cnx.cursor()
    cur.execute("SELECT id, nom, ST_X(posicio), ST_Y(posicio) FROM mercats;")
    return cur.fetchall()  # List of tuples (market_i_id, market_i_name, market_i_lat, market_i_lng)


def scrape_location(location):
    # Selenium setup
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(pathToChrome, options=chrome_options)
    encoded_location = parse.quote_plus(location)
    url = "https://www.google.com/maps/search/?api=1&query=" + encoded_location
    driver.get(url)

    # Start scraping
    popular_times = {}  # Return variable
    try:
        # Wait (max 30 seconds) until AJAX has loaded necessary part of the page
        element = WebDriverWait(driver, 30, 0.5).until(
            EC.element_to_be_clickable((By.CLASS_NAME, 'section-popular-times-arrow-left'))
        )

        day_number = 0
        days_of_week = ["Diumenge", "Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte"]
        popular_times = dict.fromkeys(days_of_week, [])  # {"Diumenge": [], "Dilluns": [], ...}

        # Extract data and append it to popular_times
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        container = soup.find('div', class_='section-popular-times-container')
        for child in container.children:
            # See HTML structure to understand following conditions
            if isinstance(child, NavigableString):
                continue
            if not child.has_attr('jstcache') or not child.has_attr('jsinstance'):
                break
            else:
                current_day = days_of_week[day_number]
                popular_times[current_day] = []
                # Following line is where we actually get the popular times
                popular_times_tags = child.find_all('div', class_='section-popular-times-bar', recursive=True)
                for tag in popular_times_tags:
                    percentage = re.findall("\\d+%", tag['aria-label'])  # Extract string fragments '\d%'
                    percentage = int(
                        percentage[-1].replace('%', '')) if percentage else None  # Keep last one, and just the number
                    popular_times[current_day].append(percentage)  # Put it into popular_times
            day_number += 1

        # We add zeros for the whole day for days with no information
        for day in popular_times:
            if None in popular_times[day]:
                popular_times[day] = [0 for _ in range(6, 24)]
        print(f"Les dades s'han pogut recollir correctament per a {location}. URL: {url}")

    except TimeoutException:  # No loading times were displayed OR there was more than one location being listed
        print(f"No s'han pogut recollir les dades per a {location}. URL: {url}")
        days_of_week = ["Diumenge", "Dilluns", "Dimarts", "Dimecres", "Dijous", "Divendres", "Dissabte"]
        popular_times = dict.fromkeys(days_of_week, [0 for _ in range(6, 24)])  # {"Diumenge": [], "Dilluns": [], ...}

    finally:
        driver.quit()

    print({location: popular_times})
    return {location: popular_times}


def scrape_all_locations(location_list):
    all_popular_times = {}  # {location1: popular_times1, location2: popular_times2, ...}
    time_per_iteration = 30  # We will scrape one location per minute (not faster)

    for location in location_list:
        start = time.time()
        popular_times = scrape_location(location)  # Returns {location: popular_times}
        all_popular_times[location] = popular_times[location]  # Only value is popular_times
        end = time.time()

        timeout = time_per_iteration - (end - start)
        if timeout > 0:
            time.sleep(timeout)

    return all_popular_times

request = {}
scraping(request)
