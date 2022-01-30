from splunk_http_event_collector import http_event_collector
import time
import logging


class SplunkSend(object):

    def __init__(self, http_event_collector_host, http_event_collector_key, http_event_collector_index):
        self.http_event_collector_host = http_event_collector_host
        self.http_event_collector_key = http_event_collector_key
        self.http_event_collector_index = http_event_collector_index

    @classmethod
    def validate_settings(cls, settings):
        def validate_setting(setting_key):
            if settings[setting_key] is None:
                raise InvalidSettingsException('%s is not defined in settings.py' % setting_key)

        required_settings = {'SPLUNK_URL', 'SPLUNK_HEC_TOKEN', 'SPLUNK_INDEX'}

        for required_setting in required_settings:
            validate_setting(required_setting)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            http_event_collector_host=crawler.settings.get('SPLUNK_URL'),
            http_event_collector_key=crawler.settings.get('SPLUNK_HEC_TOKEN'),
            http_event_collector_index=crawler.settings.get('SPLUNK_INDEX')
        )

    def open_spider(self, spider):
        self.client=http_event_collector(self.http_event_collector_key, self.http_event_collector_host)
        # perform a HEC reachable check
        hec_reachable = self.client.check_connectivity()
        if not hec_reachable:
            sys.exit(1)


    def process_item(self, item, spider):

        message = {}

        logging.info("Spider name is also the index name. Make sure idx exists. Spider name: {}".format(spider.name))
        message['index'] = self.http_event_collector_index
        message['sourcetype'] = str(spider.name)
        message['source'] = str(spider.name)
        message['host'] = str(spider.name)

        if ( 'time' in item ):
            message.update({"time":item['time']})
        else:
            message.update({"time": int(time.time()) })

        message.update({"event":item})
        self.client.sendEvent(message)
        return item
