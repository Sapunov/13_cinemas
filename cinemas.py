#!/usr/bin/env python3

import os
import requests
import json
import argparse

import cacher as cache
from bs4 import BeautifulSoup


CACHE_LIFETIME = 60 * 60 * 24 * 7
SIMILARITY_THRESHOLD = 3

cache.set_cache_directory(os.path.join(os.path.dirname(__file__), ".cache"))


def levenshtein_distance(a, b):
    n, m = len(a), len(b)

    if n > m:
        a, b = b, a
        n, m = m, n

    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n

        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1,\
                current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)

    return current_row[n]


def fetch_page(url, headers=None):

    data = cache.get(url)

    if data is None:
        try:
            page = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError:
            return None

        if page.status_code == 200:
            try:
                data = page.json()
            except ValueError:
                data = page.text

            cache.put(url, data, CACHE_LIFETIME)

    return data


def fetch_afisha_page():

    afisha_page = "http://www.afisha.ru/msk/schedule_cinema"

    return fetch_page(afisha_page)


def fetch_kinozal_data(movie_title):

    kinozal_page = "https://suggest-kinopoisk.yandex.net/" \
        "suggest-kinopoisk?srv=kinopoisk&part={0}"

    user_agent = " ".join((
        "Mozilla/5.0 (X11; Linux x86_64)",
        "AppleWebKit/537.36",
        "(KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36"
    ))

    headers = {
        "Host": "suggest-kinopoisk.yandex.net",
        "Origin": "https://plus.kinopoisk.ru",
        "Pragma": "no-cache",
        "Referer": "https://plus.kinopoisk.ru/",
        "User-Agent": user_agent
    }

    return fetch_page(kinozal_page.format(movie_title), headers)


def parse_afisha_list(raw_html):

    soup = BeautifulSoup(raw_html, "lxml")
    movies = {}

    blocks = soup.find_all("div", {"class": "s-votes-hover-area"})
    for i, block in enumerate(blocks):
        title_block = block.find("h3")
        cinemas_table = block.find("table")

        cinemas = []
        cinemas_raw = filter(
            lambda it: it["href"] != "#",
            cinemas_table.find_all("a")
        )
        for ci in cinemas_raw:
            cinemas.append(
                {
                    "name": ci.text,
                    "link": ci["href"]
                }
            )

        movies[i] = {
            "name": title_block.a.text,
            "link": title_block.a["href"],
            "cinemas": cinemas,
            "cinemas_count": len(cinemas)
        }

        kinopoisk_data = fetch_movie_info(movies[i]["name"])
        a, b = kinopoisk_data["name"].lower(), movies[i]["name"].lower()

        if a == b or levenshtein_distance(a, b) <= SIMILARITY_THRESHOLD:
            movies[i].update(kinopoisk_data)

        # To reduce problems with sorting
        if "rate" not in movies[i]:
            movies[i].update({"rate": 0, "votes": 0})

    return movies


def interpret_kinopoisk_search_results(suggest_result):

    res = {"name": "", "votes": 0, "rate": 0}

    try:
        most_wanted = json.loads(suggest_result[2][0])

        res["name"] = most_wanted["title"]
        res["votes"] = most_wanted["rating"]["votes"]
        res["rate"] = most_wanted["rating"]["rate"]
    except (IndexError, KeyError):
        pass

    return res


def fetch_movie_info(movie_title):

    movie_title = movie_title.lower()

    return interpret_kinopoisk_search_results(fetch_kinozal_data(movie_title))


def _rate_with_votes(rate, votes, top, bottom):

    return rate + (votes - bottom) / (top - bottom) * 10


def console_output(movies, count, most_cinemas, take_votes):

    if take_votes:
        votes = [it["votes"] for it in movies]
        max_votes = max(votes)
        min_votes = min(votes)

        movies.sort(key=lambda it: _rate_with_votes(
            it["rate"], it["votes"], max_votes, min_votes), reverse=True)
    else:
        movies.sort(key=lambda it: it["rate"], reverse=True)

    if most_cinemas:
        cinemas_count = [it["cinemas_count"] for it in movies]
        cinemas_mean = sum(cinemas_count) / len(cinemas_count)

        movies = [it for it in movies if it["cinemas_count"] > cinemas_mean]

    movies = [it for it in movies if it["rate"] != 0]

    format_str = "{0:<4}{1:<50}{2:<7}{3:<8}{4}"
    print("-" * 76)
    print(format_str.format("#", "Movie", "Rate", "Votes", "Cinemas"))
    print("-" * 76)

    count = count if count != 0 else len(movies)

    for i, movie in enumerate(movies[:count]):
        print(format_str.format(
            i + 1,
            (movie["name"] + " ").ljust(48, "."),
            round(movie["rate"], 3),
            movie["votes"],
            movie["cinemas_count"]))

    print("-" * 76)


def main():

    description = "Get popular movies with highest kinopoisk rate"

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "-n", "--number",
        type=int,
        default=10,
        help="Number of results. Default: 10")
    parser.add_argument(
        "-m", "--most-cinemas",
        action="store_true",
        help="Show only movies that are on in most cinemas")
    parser.add_argument(
        "--votes",
        action="store_true",
        help="Take the number of votes into account")
    parser.add_argument(
        "--clean-cache",
        action="store_true",
        help="Remove cached data. New data will be downloaded from \
            afisha.ru and kinopoisk.ru")

    args = parser.parse_args()

    if args.clean_cache:
        cache.remove_all()

    popular_movies = cache.get("popular_movies")

    if popular_movies is None:
        print("% Data will be downloaded from Internet. This may take a while")

        popular_movies = cache.put(
            "popular_movies",
            parse_afisha_list(fetch_afisha_page()), CACHE_LIFETIME)

    console_output(
        list(popular_movies.values()),
        args.number,
        args.most_cinemas,
        args.votes
    )


if __name__ == "__main__":
    main()
