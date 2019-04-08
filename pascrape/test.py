rom scrapy.linkextractors import LinkExtractor
from pprint import pprint


class URLLister(SGMLParser):
    def reset(self):
        SGMLParser.reset(self)
        self.urls = []

    def start_a(self, attrs):
        href = [v for k, v in attrs if k=='href']
        if href:
            self.urls.extend(href)


def get_urls_from(url):
    url_list = []
    import urllib
    usock = urllib.urlopen(url)
    parser = URLLister()
    parser.feed(usock.read())
    usock.close()
    parser.close()
    map(url_list.append,
        [item for item in parser.urls \
         if item.startswith(('http', 'ftp', 'www'))])
    return url_list


pprint(get_urls_from("http://www.rochacbruno.com.br"))