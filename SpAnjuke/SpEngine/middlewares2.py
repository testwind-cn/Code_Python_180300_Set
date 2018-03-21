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
        logging.debug("\033[1;32;47m WangJ := \033[0m" ' Middleware OK'	)
        logging.debug("\033[1;32;47m WangJ := \033[0m" ' response %s' %response)
        logging.debug("\033[1;32;47m WangJ := \033[0m" ' request %s' %request)
        if response.status in [301, 302]:
            newURL = response.headers["Location"].decode('utf-8')
            # print(response.headers["Location"])
            # >>> '€20'.encode('utf-8')
            # b'\xe2\x82\xac20'
            # >>> b'\xe2\x82\xac20'.decode('utf-8')
            # '€20'

            logging.info("\033[1;32;47m WangJ := \033[0m" "trying to redirect us to: %s from: %s" %( newURL ,url ))
            reason = 'redirect %d' % response.status
            requestNew = request.replace(url=newURL)

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
