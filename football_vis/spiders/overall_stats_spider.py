import scrapy


class OverallStatsItem(scrapy.Item):
    foreigner_count = scrapy.Field()
    year = scrapy.Field()
    league_id = scrapy.Field()


class OverallStatsSpider(scrapy.Spider):
    name = 'overall_stat_list'
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
                    'item': OverallStatsItem(),
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
                    'item': item,
                })

            yield request

    def parse_foreign_country_list(self, response):
        item = response.meta['item']
        country_counts = response.xpath(
            "//*[contains(@class, 'even') or contains(@class, 'odd')]/td[2]/a/text()").extract()

        split_url = response.request.url.split('/')

        item['league_id'] = split_url[-3]
        item['year'] = int(split_url[-1])

        foreigner_count = 0

        for country in country_counts:
            foreigner_count += int(country)

        item['foreigner_count'] = foreigner_count

        yield item
