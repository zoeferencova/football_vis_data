import scrapy
import re


class PlayerStatItem(scrapy.Item):
    player_id = scrapy.Field()
    year = scrapy.Field()
    citizenship = scrapy.Field()
    import_country = scrapy.Field()
    club = scrapy.Field()
    goals_scored = scrapy.Field()
    appearances = scrapy.Field()
    player_url = scrapy.Field()


class PlayerStatSpider(scrapy.Spider):
    name = 'player_stat_list'
    allowed_domains = ['transfermarkt.com']
    start_urls = []

    for num in range(0, 194):
        start_urls.append(
            f'https://www.transfermarkt.com/wettbewerbe/national/wettbewerbe/{num}')

    def parse(self, response, **kwargs):
        league_url = response.xpath(
            '//*[@class="hauptlink"]/table/tr/td[2]/a/@href').extract_first()

        if league_url:
            request = scrapy.Request(
                url='https://www.transfermarkt.com' + league_url,
                callback=self.parse_league_page,
                meta={
                    'item': PlayerStatItem()
                })

            yield request

    def parse_league_page(self, response):
        item = response.meta['item']
        player_list_url = response.xpath(
            "//*[@class='data-header__items']/li[3]/span/a/@href").extract_first()

        years = range(1994, 2023)

        season_urls = []

        for year in years:
            if player_list_url:
                season_urls.append(f'{player_list_url}/saison_id/{year}')

        for url in season_urls:
            request = scrapy.Request(
                url='https://www.transfermarkt.com' + url,
                callback=self.parse_foreign_country_list,
                meta={
                    'item': item
                })

            yield request

    def parse_foreign_country_list(self, response):
        item = response.meta['item']
        country_links = response.xpath(
            "//*[contains(@class, 'even') or contains(@class, 'odd')]/td[2]/a/@href").extract()

        country_names = response.xpath(
            "//*[contains(@class, 'even') or contains(@class, 'odd')]/td[1]/a/text()").extract()

        for i, country in enumerate(country_links):
            request = scrapy.Request(
                url='https://www.transfermarkt.com' + country,
                callback=self.parse_player_list,
                dont_filter=True,
                meta={
                    'citizenship': country_names[i],
                    'item': item
                })

            yield request

    def parse_player_list(self, response):
        item = response.meta['item']

        year = int(re.findall(r'\d{4}', response.request.url)[0]) + 1

        import_country = response.xpath(
            "normalize-space(//*[@class = 'data-header__club']/a/text())").extract_first()

        players = response.xpath(
            "//*[contains(@class, 'even') or contains(@class, 'odd')]")

        for player in players:
            player_club = player.xpath(
                "./td[2]/a/@title").extract_first()

            player_appearances = int(player.xpath(
                "./td[4]/a/text()").extract_first())

            player_goals = int(player.xpath(
                "./td[5]/a/text()").extract_first())

            player_profile_url = player.xpath(
                "./td/table/tr/td[2]/a/@href").extract_first()

            player_id = int(player_profile_url.split('/')[-1])
            item['player_id'] = player_id

            item = response.meta['item']
            item['year'] = year
            item['citizenship'] = response.meta['citizenship']
            item['import_country'] = import_country
            item['club'] = player_club
            item['appearances'] = player_appearances
            item['goals_scored'] = player_goals
            item['player_id'] = player_id
            item['player_url'] = player_profile_url

            yield item

        next_page = response.xpath(
            '//*[@title = "Go to next page"]/@href').extract_first()

        if next_page is not None:
            next_page = response.urljoin(next_page)
            request = scrapy.Request(
                url=next_page,
                callback=self.parse_player_list,
                meta={
                    'item': item,
                    'citizenship': response.meta['citizenship']
                })

            yield request
