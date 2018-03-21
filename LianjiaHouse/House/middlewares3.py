from scrapy import Selector
from scrapy.downloadermiddlewares.redirect import RedirectMiddleware
from scrapy.selector import HtmlXPathSelector
from scrapy.utils.response import get_meta_refresh
# from scrapy import log
import logging
import requests

class WJRedirectMiddleware(RedirectMiddleware):


    def process_request(self, request, spider) :
        user_agent = 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.22 \
                         Safari/537.36 SE 2.X MetaSr 1.0'
        headers = {'User-Agent': user_agent}
        print 'WangJun request 111',request
#        s = requests.Session()
#        p = s.get(request, headers=headers, allow_redirects=True)
#        print p
#        print p.cookies
        
    def process_response(self, request, response, spider):
        url = response.url
        logging.debug( 'WangJun I am OK111'	)	
        logging.debug( 'WangJun I am OK111 %d' %response.status	)
        print 'Cookie',response
        
        if response.status in [301, 302]:
            logging.info("WangJun trying to redirect us: %s" %url)
            reason = 'redirect %d' %response.status
            return self._retry(request, reason, spider) or response
        interval, redirect_url = get_meta_refresh(response)
        # handle meta redirect
        if redirect_url:
            logging.info("trying to redirect us: %s" %url)
            reason = 'meta'
            return self._retry(request, reason, spider) or response
        hxs = Selector(response) #HtmlXPathSelector(response)
        # test for captcha page
        captcha = hxs.xpath(".//input[contains(@id, 'captchacharacters')]").extract()
        if captcha:
            logging.info("captcha page %s" %url)
            reason = 'capcha'           
            return self._retry(request, reason, spider) or response
        return response