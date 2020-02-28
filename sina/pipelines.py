# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import psycopg2


class SinaPipeline(object):
    def open_spider(self, spider):
        hostname = 'localhost'
        username = 'postgres'
        password = '19891223'  # your password
        database = 'quotes'
        self.connection = psycopg2.connect(
            host=hostname, user=username, password=password, dbname=database)
        self.cur = self.connection.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.connection.close()

    def process_item(self, item, spider):
        self.cur.execute("insert into quotes_content(key,title,content,pub_date,source,keywords) values(%s,%s,%s,%s,%s,%s) on conflict(key) do update set title = %s, content = %s,pub_date = %s,source = %s,keywords = %s",
                         (item['_id'], item['title'], item['content'], item['pub_date'], item['source'], item['keywords'], item['title'], item['content'], item['pub_date'], item['source'], item['keywords']))
        self.connection.commit()
        return item
