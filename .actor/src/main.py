# Apify SDK - toolkit for building Apify Actors (Read more at https://docs.apify.com/sdk/python).
from apify import Actor

from apify_client import ApifyClient
# Requests - library for making HTTP requests in Python (Read more at https://requests.readthedocs.io)
import requests
# Beautiful Soup - library for pulling data out of HTML and XML files (Read more at https://www.crummy.com/software/BeautifulSoup/bs4/doc)
from bs4 import BeautifulSoup

async def main():
    async with Actor:
        client = ApifyClient(token='uJEG3XpwBpmpPpjzHv99kEqc2')

        items = client.task('glorious_field/amazon-crawler-task-new').last_run().dataset().list_items().items

        for item in items:
            asin = item["asin"]
            
            if item["bestsellerRanks"]:
                bestsellerRanks_0_category = item["bestsellerRanks"][0]["category"]
                bestsellerRanks_0_rank = item["bestsellerRanks"][0]["rank"]
                bestsellerRanks_0_url = item["bestsellerRanks"][0]["url"]
                
                if len(item["bestsellerRanks"]) >= 2:
                    bestsellerRanks_1_category = item["bestsellerRanks"][1]["category"]
                    bestsellerRanks_1_rank = item["bestsellerRanks"][1]["rank"]
                    bestsellerRanks_1_url = item["bestsellerRanks"][1]["url"]
                else:
                    bestsellerRanks_1_category = None
                    bestsellerRanks_1_rank = None
                    bestsellerRanks_1_url = None
            else:
                bestsellerRanks_0_category = None
                bestsellerRanks_0_rank = None
                bestsellerRanks_0_url = None

                bestsellerRanks_1_category = None
                bestsellerRanks_1_rank = None
                bestsellerRanks_1_url = None

            
            brand = item["brand"]

            breadCrumbs = item["breadCrumbs"]

            description = item["description"]

            hasReviews = item["hasReviews"]

            if item["listPrice"]:
                listPrice_value = item["listPrice"]["value"]
            else:
                listPrice_value = None

            if item["price"]:
                price_value = item["price"]["value"]
            else:
                price_value = None

            reviewsCount = item["reviewsCount"]

            if hasReviews:
                reviewsLink = item["reviewsLink"]
            else:
                reviewsLink = None

            if item["seller"]:
                seller_name = item["seller"]["name"]
                seller_url = item["seller"]["url"]
            else:
                seller_name = None
                seller_url = None

            stars = item["stars"]

            thumbnailImage = item["thumbnailImage"]

            title = item["title"]

            url = item["url"]
            
            data = {
                "asin": asin,
                "bestsellerRanks_0_category": bestsellerRanks_0_category,
                "bestsellerRanks_0_rank": bestsellerRanks_0_rank,
                "bestsellerRanks_0_url": bestsellerRanks_0_url,
                "bestsellerRanks_1_category": bestsellerRanks_1_category,
                "bestsellerRanks_1_rank": bestsellerRanks_1_rank,
                "bestsellerRanks_1_url": bestsellerRanks_1_url,
                "brand": brand,
                "breadCrumbs": breadCrumbs,
                "description": description,
                "hasReviews": hasReviews,
                "listPrice_value": listPrice_value,
                "price_value": price_value,
                "reviewsCount": reviewsCount,
                "reviewsLink": reviewsLink,
                "seller_name": seller_name,
                "seller_url": seller_url,
                "stars": stars,
                "thumbnailImage": thumbnailImage,
                "title": title,
                "url": url,
            }

            await Actor.push_data(data)