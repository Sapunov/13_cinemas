#!/usr/bin/env python3

import os
import errno
import time
import requests
import json
import argparse
import tempfile
import shutil
import statistics
from hashlib import sha1

import msgpack
from bs4 import BeautifulSoup

from json.decoder import JSONDecodeError


class Cacher:
    objects = {}
    cacher_dirname = "cacher"

    def __init__(self, cache_name):
        self.cache_path = os.path.join(
            tempfile.gettempdir(), self.cacher_dirname, cache_name)
        self._ensure_dir(self.cache_path)

    def put(self, name, obj, expires=60):
        self.objects[name] = {
            "data": obj,
            "expires": self._now() + expires
        }

        self._write(os.path.join(self.cache_path, name), self.objects[name])

        return self.objects[name]["data"]

    def get(self, name):
        filepath = os.path.join(self.cache_path, name)

        if name not in self.objects:
            if os.path.exists(filepath):
                self.objects[name] = self._read(filepath)
            else:
                return None

        if self.objects[name]["expires"] <= self._now():
            try:
                os.remove(filepath)
            except FileNotFoundError:
                pass

            return None
        else:
            return self.objects[name]["data"]

    def remove(self, name):
        filepath = os.path.join(self.cache_path, name)

        try:
            del self.objects[name]
        except KeyError:
            pass

        try:
            os.remove(filepath)
        except FileNotFoundError:
            pass

    def remove_all(self):
        try:
            shutil.rmtree(self.cache_path)
        except FileNotFoundError:
            pass

        self._ensure_dir(self.cache_path)
        self.objects = {}

    def _now(self):
        return int(time.time())

    def _write(self, filepath, obj):
        with open(filepath, "wb") as cache_file:
            msgpack.pack(obj, cache_file)

    def _read(self, filepath):
        with open(filepath, "rb") as cache_file:
            return msgpack.unpack(cache_file, encoding="utf-8")

    def _ensure_dir(self, dirname):
        try:
            os.makedirs(dirname)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise


CACHE = Cacher("cinemas")


def _levenshtein_distance(a, b):
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
    url_hash = "request-" + sha1(bytes(url, "utf-8")).hexdigest()

    ans = CACHE.get(url_hash)
    if ans is None:
        try:
            page = requests.get(url, headers=headers)
        except requests.exceptions.ConnectionError:
            return None

        if page.status_code == 200:
            try:
                ans = page.json()
            except JSONDecodeError:
                ans = page.text

            CACHE.put(url_hash, ans, 60 * 60 * 24)

    return ans


def fetch_afisha_page():
    afisha_page = "http://www.afisha.ru/msk/schedule_cinema"

    return fetch_page(afisha_page)


def fetch_kinozal_data(movie_title):
    kinozal_page = "https://suggest-kinopoisk.yandex.net/" \
        "suggest-kinopoisk?srv=kinopoisk&part={0}"

    user_agent = ("Mozilla/5.0 (X11; Linux x86_64) ",
                  "AppleWebKit/537.36 ",
                  "(KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36")
    headers = {
        "Host": "suggest-kinopoisk.yandex.net",
        "Origin": "https://plus.kinopoisk.ru",
        "Pragma": "no-cache",
        "Referer": "https://plus.kinopoisk.ru/",
        "User-Agent": "".join(user_agent)
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

        if a == b or _levenshtein_distance(a, b) <= 3:
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


def output_movies_to_console(movies, count, most_cinemas, take_votes):
    if take_votes:
        votes = [it["votes"] for it in movies]
        max_votes = max(votes)
        min_votes = min(votes)

        movies.sort(key=lambda it: _rate_with_votes(
            it["rate"], it["votes"], max_votes, min_votes), reverse=True)
    else:
        movies.sort(key=lambda it: it["rate"], reverse=True)

    if most_cinemas:
        cinemas = [it["cinemas_count"] for it in movies]
        cinemas_mean = statistics.mean(cinemas)

        movies = list(filter(
            lambda it: it["cinemas_count"] > cinemas_mean, movies))

    movies = list(filter(lambda it: it["rate"] != 0, movies))

    format_str = "{0:<4}{1:<50}{2:<7}{3:<8}{4}"
    print("-" * 76)
    print(format_str.format("#", "Movie", "Rate", "Votes", "Cinemas"))
    print("-" * 76)

    for i, movie in enumerate(movies[:count]):
        print(format_str.format(
            i + 1,
            (movie["name"] + " ").ljust(48, "."),
            round(movie["rate"], 3),
            movie["votes"],
            movie["cinemas_count"]))

    print("-" * 76)


def main(movies_count, most_cinemas, take_votes, remove_cache):
    if remove_cache:
        CACHE.remove_all()

    popular_movies = CACHE.get("popular_movies")
    if popular_movies is None:
        print("% Data will be downloaded from internet. This may take a while")

        popular_movies = CACHE.put(
            "popular_movies",
            parse_afisha_list(fetch_afisha_page()), 60 * 60 * 24 * 7)

    output_movies_to_console(
        list(popular_movies.values()), movies_count, most_cinemas, take_votes)


if __name__ == "__main__":
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
            afisha and kinopoisk")

    args = parser.parse_args()

    main(args.number, args.most_cinemas, args.votes, args.clean_cache)
