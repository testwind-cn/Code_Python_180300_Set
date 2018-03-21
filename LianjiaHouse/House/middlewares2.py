from scrapy import Selector
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.selector import HtmlXPathSelector
from scrapy.utils.response import get_meta_refresh
# from scrapy import log
from scrapy.http.request import Request
import logging


class CustomRetryMiddleware(RetryMiddleware):

    def process_response(self, request, response, spider):
        url = response.url
        logging.debug('WangJun 1 I am OK111'	)
        logging.debug('WangJun response %s' %response)
        logging.debug('WangJun request %s' %request)
        if response.status in [301, 302]:
            logging.info("WangJun 2 trying to redirect us: %s" %url)
            reason = 'redirect %d' %response.status
            requestNew = request.replace(url=response.headers["Location"])
            print response.headers["Location"]
            return self._retry(requestNew, reason, spider)
#        redirect_url = get_meta_refresh(response)
#        print redirect_url
        # handle meta redirect
#        if redirect_url:
#            logging.info("WangJun 3 trying to redirect us: %s" %url)
#            reason = 'meta'
#            return self._retry(request, reason, spider) or response
#        hxs = Selector(response) #HtmlXPathSelector(response)
        # test for captcha page
#        captcha = hxs.xpath(".//input[contains(@id, 'captchacharacters')]").extract()
#        if captcha:
#            logging.info("WangJun 4 captcha page %s" %url)
#            reason = 'capcha'           
#            return self._retry(request, reason, spider) or response
        return response
