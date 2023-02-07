import scrapy
import json

import pandas as pd
import numpy as np
from ast import literal_eval


class PlayerDetailItem(scrapy.Item):
    player_id = scrapy.Field()
    name = scrapy.Field()
    position = scrapy.Field()
    date_of_birth = scrapy.Field()
    image_urls = scrapy.Field()
    player_image = scrapy.Field()
    market_value = scrapy.Field()


class PlayerDetailSpider(scrapy.Spider):
    name = 'player_detail_list'
    allowed_domains = ['transfermarkt.com']
    start_urls = []

    # overrides ITEM_PIPLEINES in settings.py
    custom_settings = {
        'ITEM_PIPELINES': {'football_vis.pipelines.PlayerImagePipeline': 300},
    }

    f = open('player_stats.json', encoding='utf-8')
    data = json.load(f)

    url_list = []
    for i in data:
        url_list.append('https://www.transfermarkt.com' + i['player_url'])

    f.close()
    start_urls = set(url_list)

    def parse(self, response, **kwargs):
        data = {}

        data['player_id'] = int(response.request.url.split('/')[-1])

        # scrape name
        first_name = response.xpath(
            'normalize-space(//*[@class="data-header__headline-wrapper"]/text()[normalize-space()])').extract_first()

        if len(first_name):
            first_name += ' '

        last_name = response.xpath(
            'normalize-space(//*[@class="data-header__headline-wrapper"]/strong/text())').extract_first()

        data['name'] = first_name + last_name

        # scrape date of birth
        player_dob = response.xpath(
            'normalize-space(//*[@itemprop="birthDate"]/text())').extract_first()

        cleaned_dob = player_dob[:len(player_dob) - 5]

        data['date_of_birth'] = cleaned_dob

        # scrape player image
        player_image = response.xpath(
            '//*[@class = "data-header__profile-image"]/@src')

        if player_image:
            data['image_urls'] = player_image.extract()

        data['player_image'] = player_image

        # scrape and simplify player position
        position_map = {
            'defender': 'Defender',
            'right-back': 'Defender',
            'centre-back': 'Defender',
            'left-back': 'Defender',
            'abwehr': 'Defender',
            'sweeper': 'Defender',
            'midfield': 'Midfielder',
            'right midfield': 'Midfielder',
            'left midfield': 'Midfielder',
            'defensive midfield': 'Midfielder',
            'attacking midfield': 'Midfielder',
            'central midfield': 'Midfielder',
            'mittelfeld': 'Midfielder',
            'attack': 'Forward',
            'left winger': 'Forward',
            'right winger': 'Forward',
            'sturm': 'Forward',
            'centre-forward': 'Forward',
            'second striker': 'Forward',
            'goalkeeper': 'Goalkeeper'
        }

        # scrape position
        position = response.xpath(
            'normalize-space(//*[contains(text(), "Position:")]/span/text())').extract_first()
        basic_position = position_map[position.lower()]
        data['position'] = basic_position

        # scrape market value dict
        market_value = None

        script = response.xpath(
            '//script[@type = "text/javascript"]/text()')

        idx = np.where(['/*<![CDATA[*/' in x[:20]
                        for x in script.extract()])[0][-1]
        script = script[idx].extract()

        mv_section = script.find(
            "'series':[{'type':'area','name':'Marktwert','data':[")

        if mv_section > 0:
            table = script.split(
                "'series':[{'type':'area','name':'Marktwert','data':[")[1].split(']')[0]

            table = pd.DataFrame(literal_eval(table))
            table = table[table.datum_mw != '-']
            table['datum_mw'] = pd.to_datetime(
                table['datum_mw'])
            table['datum_mw'] = table['datum_mw'].dt.year

            table = table.rename(
                columns={'y': 'value', 'datum_mw': 'year'})

            table_dict = table.to_dict(orient='list')

            market_value = {}

            for i, year in enumerate(table_dict['year']):
                market_value[year] = table_dict['value'][i]

        data['market_value'] = market_value

        yield PlayerDetailItem(**data)
