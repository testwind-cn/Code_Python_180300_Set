from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.exceptions import IgnoreRequest
#from scrapy import log
import logging

class FilterURLs(object):
    def process_response(self,request, response, spider):
        logging.debug( 'WangJun I am OK22 %d' %response.status	)	
        if response.status == 301 :
            return response
        else:
            headers = ['text/html; charset=UTF-8', 'text/html; charset=utf-8', 'text/html;charset=UTF-8', 'text/html;charset=utf-8','text/html;charset=ISO-8859-1','application/xhtml+xml; charset=utf-8']
            logging.info( "In Middleware " + repr(response.headers['Content-Type']) )
            for header in headers:
                if response.headers['Content-Type'] != header:
                    logging.info("Ignoring response %r" % request)
                    raise IgnoreRequest()
                else:
                    return response