# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import scrapy
from scrapy.pipelines.images import ImagesPipeline


class FootballVisPipeline:
    def process_item(self, item, spider):
        return item

# pipeline using Scrapy ImagesPipeline to convert image URL's to saved images


class PlayerImagePipeline(ImagesPipeline):
    # take any image URLs scraped by minibio spider and generate HTTP request for their content
    def get_media_requests(self, item, info):
        for image_url in item['image_urls']:
            yield scrapy.Request(image_url)

    # after URL requests have been made, results are delivered to item_completed method
    def item_completed(self, results, item, info):
        # filter list of result tuples for successful results and add file paths to list
        # tuple list format: [(True, Image), (False, Image)]
        # store file paths relative to directory specified by IMAGE_STORE var in settings.py
        image_paths = [img['path'] for ok, img in results if ok]
        if image_paths:
            item['player_image'] = image_paths[0]

        return item

# class DuplicatesPipeline:

#     def __init__(self):
#         self.titles_seen = set()

#     def process_item(self, item, spider):
#         if item['unique_id'] in self.titles_seen:
#             raise DropItem("Duplicate item title found: %s" % unique_id)
#         else:
#             self.titles_seen.add(item['unique_id'])
#             return item

# class ClubImagePipeline(ImagesPipeline):
#     # take any image URLs scraped by minibio spider and generate HTTP request for their content
#     def get_media_requests(self, item, info):
#         yield scrapy.Request(item['club_image'])

#     # after URL requests have been made, results are delivered to item_completed method
#     def item_completed(self, results, item, info):
#         # filter list of result tuples for successful results and add file paths to list
#         # tuple list format: [(True, Image), (False, Image)]
#         # store file paths relative to directory specified by IMAGE_STORE var in settings.py
#         image_paths = [img['path'] for ok, img in results if ok]
#         if image_paths:
#             item['club_image'] = image_paths[0]

#         return item
