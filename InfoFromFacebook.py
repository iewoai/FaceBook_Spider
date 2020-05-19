# -*- coding: utf-8 -*-
import re
import time
from urllib.parse import urljoin, urlparse, unquote
import redis
import chardet
import scrapy
from ..items import InfofromfacebookspiderItem
from ..settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASS, REDIS_KEY, REDIS_KEY_COM, REDIS_KEY_CAT

class InfoFromFacebookSpider(scrapy.Spider):
    name = 'InfoFromFacebook'
    redis_server = redis.Redis(REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASS, decode_responses=True)

    def start_requests(self):
        # 爬取分类
        cat_num = self.redis_server.scard(REDIS_KEY_CAT)
        # 爬取详情
        com_num = self.redis_server.scard(REDIS_KEY)
        if com_num != 0:
            while com_num != 0:
                if not cat_num:
                    start_urls = 'https://www.facebook.com/pages/category/'
                    yield scrapy.Request(start_urls, callback = self.parse_cat)
                    break
                else:
                    if com_num < 10000:
                        print('######## 原有%d个分类'%cat_num)
                        cat_url = self.redis_server.spop(REDIS_KEY_CAT)
                        print('##### 当前爬取的分类url为：%s' % cat_url)
                        yield scrapy.Request(cat_url, callback=self.parse_com)
                print('######## 原有%d个公司主页'%com_num)
                com_url = self.redis_server.spop(REDIS_KEY)
                print('##### 当前爬取的公司url为：%s' % com_url)
                yield scrapy.Request(com_url, callback=self.parse)
                com_num = self.redis_server.scard(REDIS_KEY)
                cat_num = self.redis_server.scard(REDIS_KEY_CAT)
        # 测试专用
        # yield scrapy.Request('https://www.facebook.com/iffcokisan/', callback = self.parse)

    # 爬取公司facebook首页，获取about页面链接
    def parse(self, response):
        """解析公司facebook首页，获取about页面链接"""
        # 查找简介页面
        page_source = response.text
        about_url = response.xpath('//a[contains(@href, "/about/")]/@href').extract_first()
        # 可以通过xpath找到
        if about_url:
            url = urljoin(response.url, about_url)
            # print('about', url)
            yield scrapy.Request(url, callback=self.parse_about, headers={'accept-language': 'en;q=0.9'}, meta = {'facebookIndex': response.url})
        else:
            url = response.request.url + "about/?ref=page_internal"
            yield scrapy.Request(url, callback=self.parse_about, headers={'accept-language': 'en;q=0.9'},meta = {'facebookIndex': response.request.url})

    # 爬取公司详情页信息
    def parse_about(self, response):
        about_url = response.url
        item = InfofromfacebookspiderItem()
        item['phone'],item['description'],item['mainBusiness'],item['email'],item['address'],item['category'],item['originalUrl'],item['website'],item['sns'] = None,None,None,None,None,None,None,None,None

        # coopanyName出现特殊样式字体如：𝐀𝐥𝐢 𝐖𝐫𝐢𝐭𝐞𝐬，未解决
        companyName = re.findall(r'id="pageTitle">(.*?)- About', response.text)
        if not companyName:
            companyName = re.findall(r'_62uk">See more of(.*?)on Facebook<', response.text)
        companyName = re.sub(r' +', ' ', companyName[0].replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\xa0', ' ')).strip() if companyName else None
        item['companyName'] = companyName
        if not item['companyName'] or not chardet.detect(item['companyName'].encode())['encoding']:
            item['companyName'] = response.meta['facebookIndex'].replace('https://www.facebook.com/', '').replace('/', '').strip()

        email = response.xpath('//img[contains(@src, "https://static.xx.fbcdn.net/rsrc.php/v3/yy/r/vKDzW_MdhyP.png")]/../following-sibling::*/a/div[@class="_50f4"]/text()').extract_first()
        item['email'] = email.strip() if email else None
        if not item['email']:
            email = response.xpath('//a[contains(@href, "@")]/@href').extract_first()
            item['email'] = email.replace('mailto:', '').encode('utf-8').decode('unicode_escape').strip() if email else None

        phone_label = response.xpath('//img[contains(@src, "https://static.xx.fbcdn.net/rsrc.php/v3/yJ/r/4VjyF4t9Hqt.png")]/../following-sibling::div[1]')
        phone_str = phone_label[0].xpath('string(.)').extract_first() if phone_label else None
        phones = re.findall(r'[-\)\(\d \.­\+]{7,}', phone_str) if phone_str else None
        item['phone'] = ','.join(phones) if phones else None

        address = response.xpath('//img[@src="https://static.xx.fbcdn.net/rsrc.php/v3/yH/r/lrqcOTQhBUL.png"]/../following-sibling::div[1]')
        address = address[0].xpath('string(.)').extract_first() if address else None
        item['address'] = address.strip() if address else None

        info_div_list = response.xpath('//div[text()="MORE INFO"]/../../div[@class="_5aj7 _3-8j"]')
        info = ''
        for div in info_div_list:
            key = div.xpath('./div[@class="_4bl9"]/div[@class="_50f4"]/text()').extract_first()
            value = div.xpath('./div[@class="_4bl9"]/div[@class="_3-8w"]').xpath('string(.)').extract()[0]
            if key:
                info += '||' + key + ': '
            if value:
                info += value
        info = info[1:].replace('...', '').replace('See More', '') + '||'
        # 公司的描述 首先采用公司概况，然后简介
        description_list = list()
        if 'About' in info:  # 简介
            description = re.findall(r'About: (.*?)\|\|', info)
            if description:
                description_list.append(description[0])
        if 'Company Overview' in info:  # 公司概况
            description = re.findall(r'Company Overview: (.*?)\|\|', info)
            if description:
                description_list.append(description[0])
        if 'General Information' in info:
            description = re.findall(r'General Information: (.*?)\|\|', info)
            if description:
                description_list.append(description[0])
        # 产品
        if 'Products' in info:
            try:
                mainBusiness = re.findall(r'Products: (.*?)\|\|', info)
                item['mainBusiness'] = mainBusiness[0] if mainBusiness else None
            except Exception as e:
                item['mainBusiness'] = None
        else:
            item['mainBusiness'] = None

        item['description'] = ' '.join([i for i in description_list]) if description_list else None
        item['description'] = item['description'].strip() if item['description'] else None

        category = response.xpath('//u[text()="categories"]/../../following-sibling::div/a/text()').extract()
        item['category'] = '>'.join(category) if category else None

        originalUrl = response.xpath('//img[@class="_1579 img" and @src="https://static.xx.fbcdn.net/rsrc.php/v3/yV/r/EaDvTjOwxIV.png"]/../following-sibling::*/a/@href').extract_first()
        if originalUrl:
            if originalUrl.startswith('https://l.facebook.com/l.php?u='):
                originalUrl = unquote(originalUrl.replace('https://l.facebook.com/l.php?u=', '')).split('&h=')[0]
            if '.' in originalUrl and 'www.youtube.com' not in originalUrl and 'www.facebook.com' not in originalUrl and 'twitter.com' not in originalUrl and 'https://instagram.com' not in originalUrl:
                originalUrl = originalUrl.strip().replace('http://http://', 'http://')
                if '://' not in originalUrl:
                    originalUrl = 'http://' + originalUrl
                item['website'] = urlparse(originalUrl).netloc.replace('www.', '')
            else:
                originalUrl = None
        else:
            originalUrl = None
        item['originalUrl'] = originalUrl

        item['updatetime'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

        if not item['email']:
            email_list = self.match_email(response)
            item['email'] = ','.join(email_list) if email_list else None

        sns = []
        sns_list = response.xpath('//span[@class="fwb"]/a/@href').extract()
        sns_list.append(response.xpath('//img[@class="_1579 img" and @src="https://static.xx.fbcdn.net/rsrc.php/v3/y_/r/8TRfTVHth97.png"]/../following-sibling::*/a/@href').extract_first())
        sns_list = sns_list + re.findall(r'(https://l\.facebook\.com/l\.php\?u=.*?)\"', response.text)
        sns.append(response.meta['facebookIndex'])
        if len(sns_list) > 0:
            for i in sns_list:
                if i and i.startswith('https://l.facebook.com/l.php?u='):
                    sns.append(unquote(i.replace('https://l.facebook.com/l.php?u=', '')))
        sns = list(set(list(filter(None, [re.sub(r' +', ' ', i.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').replace('\xa0', ' ').replace('&amp;h=', '&h=')).split('&h=')[0].strip() for i in sns if (item['website'] and item['website'] not in i) or not item['website']])))) if sns else None
        item['sns'] = ','.join([i for i in sns if i not in ['https://instagram.com/', 'https://www.youtube.com/','https://www.facebook.com/','https://twitter.com/']]) if sns else None

        item['detailUrl'] = response.url
        # print(item)
        yield item

    # 匹配邮箱字段
    def match_email(self, response):
        """
        从返回的网页源码中匹配邮箱
        :param page_source: 网页源码
        :return: 匹配后的邮箱
        """
        no_suffix_list =['jsp','cfm','shtml','html','aspx','php','json','0','1','2','3','4','5','6','7','8','9','jpg','png','js','css','jpeg','gif','bmp','doc','docs','xls','xlxs','txt','ppt','html','dat','pdf','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
        new_email_set = set()
        try:
            # 查找所有文本
            text_list = response.xpath('//text()').extract()
            # 查找包含mailto的a标签
            href_list = response.xpath('//a[contains(@href, "mailto:")]/@href').extract()
            text_list.extend(href_list)
        except Exception as e:
            print(e)
            text_list = []
        for text in text_list:  # 遍历所有文本，检查是否符合规则
            if text and text.strip():
                email_list = re.findall(r'[A-Za-z0-9_\.\-\+]+@[a-zA-Z0-9_-]+(?:\.[a-zA-Z0-9_-]+)+', text)
                for email in email_list:
                    sp_email = email.split('@')[1]
                    index = str(sp_email).rfind('.')
                    if index != -1:
                        if str(sp_email[index + 1:]).lower() not in no_suffix_list:
                            new_email_set.add(email)
                    else:
                        new_email_set.add(email)
        return list(new_email_set)

    #爬取分类url
    def parse_cat(self, response):
        div_list = response.xpath('//h1[text()="All Categories"]/following-sibling::div/div[@class="_717a"]')
        for div in div_list:
            pcid = div.xpath('string(.)').extract_first()
            pcid = pcid.strip()
            pcid_href = div.xpath('.//a/@href').extract_first()
            pcid_url = urljoin(response.url, pcid_href)
            cid1_div_list = div.xpath('./following-sibling::div[@class="_7178"][1]/div/div[@class="_717a"]')
            # cid*都是子类
            if cid1_div_list:
                # print(pcid, len(cid1_div_list))
                for cid1_div in cid1_div_list:
                    cid1 = cid1_div.xpath('string(.)').extract_first()
                    cid1 = cid1.strip()
                    cid1_href = cid1_div.xpath('.//a/@href').extract_first()
                    cid1_url = urljoin(response.url, cid1_href)
                    self.redis_server.sadd(REDIS_KEY_CAT, cid1_url)
                    cid2_div_list = cid1_div.xpath('./following-sibling::div[@class="_7178"][1]/div/div[@class="_717a"]')
                    if cid2_div_list:
                        # print(pcid, cid1, len(cid2_div_list))
                        for cid2_div in cid2_div_list:
                            cid2 = cid2_div.xpath('string(.)').extract_first()
                            cid2 = cid2.strip()
                            cid2_href = cid2_div.xpath('.//a/@href').extract_first()
                            cid2_url = urljoin(response.url, cid2_href)
                            self.redis_server.sadd(REDIS_KEY_CAT, cid2_url)
                            cid3_div_list = cid2_div.xpath('./following-sibling::div[@class="_7178"][1]/div/div[@class="_717a"]')
                            if cid3_div_list:
                                # print(pcid, cid1, cid2, len(cid3_div_list))
                                for cid3_div in cid3_div_list:
                                    cid3 = cid3_div.xpath('string(.)').extract_first()
                                    cid3 = cid3.strip()
                                    cid3_href = cid3_div.xpath('.//a/@href').extract_first()
                                    cid3_url = urljoin(response.url, cid3_href)
                                    self.redis_server.sadd(REDIS_KEY_CAT, cid3_url)
                                    cid4_div_list = cid3_div.xpath('./following-sibling::div[@class="_7178"][1]/div/div[@class="_717a"]')
                                    if cid4_div_list:
                                        # print(pcid, cid1, cid2, cid3, len(cid4_div_list))
                                        for cid4_div in cid4_div_list:
                                            cid4 = cid4_div.xpath('string(.)').extract_first()
                                            cid4 = cid4.strip()
                                            cid4_href = cid4_div.xpath('.//a/@href').extract_first()
                                            cid4_url = urljoin(response.url, cid4_href)
                                            self.redis_server.sadd(REDIS_KEY_CAT, cid4_url)
                                            cid5_div_list = cid4_div.xpath('./following-sibling::div[@class="_7178"][1]/div/div[@class="_717a"]')
                                            if cid5_div_list:
                                                # print(pcid, cid1, cid2, cid3, cid4, len(cid5_div_list))
                                                for cid5_div in cid5_div_list:
                                                    cid5 = cid5_div.xpath('string(.)').extract_first()
                                                    cid5 = cid5.strip()
                                                    cid5_href = cid5_div.xpath('.//a/@href').extract_first()
                                                    cid5_url = urljoin(response.url, cid5_href)
                                                    self.redis_server.sadd(REDIS_KEY_CAT, cid5_url)

    # 爬取公司详情url
    def parse_com(self, response):
        coms = re.findall(r'<a class=\"_6x0d\" href=\"(.*?)\"', response.text)
        if coms:
            print('coms:', len(coms), response.url)
            for com in coms:
                com = urljoin(response.url, com)
                self.redis_server.sadd(REDIS_KEY, com)

        next_url = re.findall(r'<link rel=\"next\" href=\"(.*?)\"',response.text)
        if next_url:
            next_url = urljoin(response.url, next_url[0])
            print('next_url:', next_url)
            yield scrapy.Request(next_url, callback = self.parse_com)
