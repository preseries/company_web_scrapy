# -*- coding: utf-8 -*-
from multiprocessing import TimeoutError

import scrapy
from pymongo.mongo_client import MongoClient
from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TCPTimedOutError
from PyDictionary import PyDictionary
from bs4 import BeautifulSoup
import logging
import urlparse
import datetime
import re

import nltk
from nltk.corpus import wordnet, stopwords

from nltk.tokenize import sent_tokenize, word_tokenize
import spacy
from spacy.symbols import VERB

class CompanyWebSiteSpider(scrapy.Spider):

    name = "website_crawler"

    EMAIL_RE = r'[\w\.-]+@[\w\.-]+'

    speciality = None

    mongo_client = None
    gatherer_database = None

    nlp = None
    dictionary = PyDictionary()

    stop_words = None

    words_to_find = None

    verbs_to_find = None

    def __init__(self, speciality=None, *args, **kwargs):
        super(CompanyWebSiteSpider, self).__init__(*args, **kwargs)

        self.logger.logger.setLevel(logging.INFO)

        if speciality is None:
            raise Exception("The speciality must be informed")

        self.speciality = speciality

        nltk.data.path.append(
            '/Users/xalperte/BigMLDev/company_web_scrapy/webscrawler/nltk_data')

        self.stop_words = stopwords.words('english')

        self.nlp = spacy.load('en')

    def init(self):

        self.mongo_client = MongoClient(
            self.settings.get('MONGO_GATHERER_BD_URI'))
        self.gatherer_database = self.mongo_client.get_default_database()

        self.prepare_words_to_find()
        self.prepare_verbs_to_find()

    def start_requests(self):

        self.init()

        companies_by_id = self.load_companies()
        company_num = 0
        current_company = None
        try:
            # companies_by_id = self.load_jim_companies()

            for company_id, company in companies_by_id.iteritems():
                current_company = company

                if 'webpage' in company:
                    company_num += 1
                    self.logger.info("[%d] Launch Home Page Request for %s - %s - %s - %s" %
                                     (
                                        company_num,
                                        company['webpage'],
                                        company['company_id'],
                                        company['company_name'],
                                        company['webpage']
                                     ))
                    yield scrapy.Request(url=company['webpage'],
                                         meta={
                                                'url': company['webpage'],
                                                'company_id': company['company_id'],
                                                'company_name': company['company_name'],
                                                'company_num': company_num,
                                                'company_home_page': company['webpage']},
                                         errback=self.error,
                                         callback=self.parse_website)

                    # company_num += 1
                    # if company_num == 10:
                    #     break
                else:
                    self.logger.warning("The company [%s] "
                                        "doesn't have webpage infomed" %
                                        company_id)
        except Exception as e:
            self.logger.error("Error processing company at [%d]. Company ID: [%s]. Cause [%s]" %
                              (company_num, current_company['company_id'], repr(e)))

    def parse_website(self, response):

        try:
            self.logger.info("[%d] Parsing Home Page from %s - %s - %s - %s" % (
                                    response.meta['company_num'],
                                    response.url,
                                    response.meta['company_id'],
                                    response.meta['company_name'],
                                    response.meta['company_home_page']))

            self.update_company_page(response)

            home_url = urlparse.urlparse(response.url)
            home_netloc = home_url.netloc.lower()

            # Following only the links to the company website. Forget about the
            # links to other websites.
            processed_links = set()
            requested_links = 0
            for link_data in self.get_links(self.guess_root(response.url), response.body):
                if link_data[1] not in processed_links:
                    processed_links.add(link_data[1])

                    link_url = urlparse.urlparse(link_data[1])
                    link_netloc = link_url.netloc.lower()

                    if home_netloc == link_netloc:
                        if self.follow_link(link_data):
                            requested_links += 1

                            # Only X links to follow
                            if requested_links > 10:
                                break

                            yield scrapy.Request(url=link_data[1],
                                                 meta={
                                                     'url_name': link_data[0],
                                                     'url': link_data[1],
                                                     'company_id': response.meta['company_id'],
                                                     'company_name': response.meta['company_name'],
                                                     'company_home_page': response.meta['company_home_page']},
                                                 errback=self.error,
                                                 callback=self.parse_internal_page)

                            self.logger.debug("\t Link to follow: [%s] - [%s]" %
                                              (link_data[0], link_data[1]))
                        else:
                            self.logger.debug(
                                "\t NOT Link to follow (not words in the link): [%s] - [%s]." %
                                (link_data[0], link_data[1]))
                    else:
                        self.logger.debug(
                            "\t NOT Link to follow (out of the company website): [%s] - [%s]. "
                            "Home Netloc [%s] - Link Netloc [%s]" %
                            (link_data[0], link_data[1], home_netloc, link_netloc))

        except Exception as e:
            self.logger.error("Home page parsing exception. URL [%s]. Cause [%s]" %
                              (response.url, repr(e)))

    def parse_internal_page(self, response):

        try:
            self.logger.debug("Parsing Internal Page from %s - %s - %s - %s - %s" % (
                                    response.meta['url_name'],
                                    response.url,
                                    response.meta['company_id'],
                                    response.meta['company_name'],
                                    response.meta['company_home_page']))

            self.update_company_page(response,
                                     url_name=response.meta['url_name'],
                                     is_home=False)

        except Exception as e:
            self.logger.error("Internal page parsing exception. URL [%s]. Cause [%s]" %
                              (response.url), repr(e))



    def follow_link(self, link_data):
        for word in self.words_to_find:
            if word in link_data[0] or \
                            word in link_data[2] or \
                            word in link_data[3]:
                return True
        return False


    def error(self, failure):
        # log all failures
        self.logger.error("Request Error!")
        self.logger.error(repr(failure))

        # in case you want to do something special for some errors,
        # you may need the failure's type:
        if failure.check(HttpError):
            # these exceptions come from HttpError spider middleware
            # you can get the non-200 response
            response = failure.value.response
            self.logger.error('HttpError on %s', response.url)
            self.write_wrong_website('HTTP',
                                     response.meta['company_id'], response.meta['company_name'],
                                     response.url,
                                     repr(failure))

        elif failure.check(DNSLookupError):
            # this is the original request
            request = failure.request
            self.logger.error('DNSLookupError on %s', request.url)
            self.write_wrong_website('DNSLookup',
                                     request.meta['company_id'],
                                     request.meta['company_name'],
                                     request.url,
                                     repr(failure))

        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error('TimeoutError on %s', request.url)
            self.write_wrong_website('Timeout',
                                     request.meta['company_id'],
                                     request.meta['company_name'],
                                     request.url,
                                     repr(failure))

    def get_links(self, root, html_doc):
        soup = BeautifulSoup(html_doc, 'html.parser')
        return self.resolve_links(root, soup.find_all('a', href=True))


    def guess_root(self, base_url):
        if base_url.startswith('http'):
            parsed_link = urlparse.urlparse(base_url)
            scheme = parsed_link.scheme + '://'
            netloc = parsed_link.netloc
            return scheme + netloc


    def resolve_links(self, root, links):
        for link in links:
            link_title = link.get_text()
            link_href = link['href']
            if not link_href.startswith('http'):
                link_href = urlparse.urljoin(root, link_href)

            # Bad urls (email attached to the url)
            match = re.findall(self.EMAIL_RE, link_href)
            if match and len(match) > 0:
                for email in match:
                    link_href = link_href.replace(email, '')

            if link_href.endswith('/admin'):
                link_href = link_href.replace('/admin', '')

            yield (link_title, link_href,
                   urlparse.urlparse(link_href).path,
                   urlparse.urlparse(link_href).query)


    def prepare_verbs_to_find(self):

        base_verbs = [ 'give', 'offer', 'contribute', 'administer',
                       'bring', 'provide', 'supply', 'manufacture', 'produce',
                       'automate', 'commodity', 'sell', 'solve', 'build' ]

        self.verbs_to_find = set()
        for word in base_verbs:
            self.verbs_to_find.add(word)
            for idx, synset in enumerate(wordnet.synsets(word)):
                for synonym in synset.lemma_names():
                    self.verbs_to_find.add(synonym.replace('_', ' '))

                    # hypernyms = [l.lemma_names() for l in synset.hypernyms()]
                    # for hypernym in hypernyms:
                    #     for word in hypernym:
                    #         self.verbs_to_find.add(word.replace('_', ' '))
                    #
                    # hyponyms = [l.lemma_names() for l in synset.hyponyms()]
                    # for hyponym in hyponyms:
                    #     for word in hyponym:
                    #         self.verbs_to_find.add(word.replace('_', ' '))


        stop_verbs = set(['get', 'have', 'be', 'add', 'work',
                          'reach', 'open', 'create', 'take', 'break'])

        self.verbs_to_find  = self.verbs_to_find.difference(stop_verbs)

    def prepare_words_to_find(self):

        all_speciality_words = set()
        for idx, synset in enumerate(wordnet.synsets(self.speciality)):
            for synonym in synset.lemma_names():
                all_speciality_words.add(synonym.replace('_', ' '))

            # hypernyms = [l.lemma_names() for l in synset.hypernyms()]
            # for hypernym in hypernyms:
            #     for word in hypernym:
            #         all_speciality_words.add(word.replace('_', ' '))
            #
            # hyponyms = [l.lemma_names() for l in synset.hyponyms()]
            # for hyponym in hyponyms:
            #     for word in hyponym:
            #         all_speciality_words.add(word.replace('_', ' '))

        # Words related to the maket
        market_words = ['mechanics','unfolding','marketplace','deploy','give',
                        'contribute','administer', 'bring', 'service', 'result',
                        'technology','market','use', 'compose', 'prepare',
                        'provide','make','support','business','supply',
                        'manufacture','product','robotics','ability','form',
                        'automate','produce','about','resource', 'commodity',
                        'vend','wholesale','work','solution','duty','retail',
                        'display', 'mission', 'vision']

        all_market_words = set()
        for word in market_words:
            all_market_words.add(word)
            for idx, synset in enumerate(wordnet.synsets(word)):
                for synonym in synset.lemma_names():
                    all_market_words.add(synonym.replace('_', ' '))

            # hypernyms = [l.lemma_names() for l in synset.hypernyms()]
            # for hypernym in hypernyms:
            #     for word in hypernym:
            #         all_market_words.add(word.replace('_', ' '))
            #
            # hyponyms = [l.lemma_names() for l in synset.hyponyms()]
            # for hyponym in hyponyms:
            #     for word in hyponym:
            #         all_market_words.add(word.replace('_', ' '))

        communication_words = ['disclosure','communication','article',
                               'announcement','story','record', 'blog',
                               'intelligence', 'journal', 'advice', 'diary',
                               'news','forum']

        all_communication_words = set()
        for word in communication_words:
            all_communication_words.add(word)
            for idx, synset in enumerate(wordnet.synsets(word)):
                for synonym in synset.lemma_names():
                    all_communication_words.add(synonym.replace('_', ' '))

            # hypernyms = [l.lemma_names() for l in synset.hypernyms()]
            # for hypernym in hypernyms:
            #     for word in hypernym:
            #         all_communication_words.add(word.replace('_', ' '))
            #
            # hyponyms = [l.lemma_names() for l in synset.hyponyms()]
            # for hyponym in hyponyms:
            #     for word in hyponym:
            #         all_communication_words.add(word.replace('_', ' '))

        special_words = set('rss')

        self.logger.debug("Speciality Words to find: [%s]" %
                          ','.join(all_speciality_words))
        self.logger.debug("Communication Words to find: [%s]" %
                          ','.join(all_communication_words))
        self.logger.debug("Market Words to find: [%s]" %
                          ','.join(all_market_words))
        self.logger.debug("Commons Words between sections: [%s]" %
                          ','.join(set.intersection(
                              all_speciality_words,
                              all_market_words,
                              all_communication_words)))

        self.words_to_find = set.union(all_speciality_words,
                                       all_market_words,
                                       all_communication_words,
                                       special_words)

    def write_wrong_website(self, type, company_id, company_name, company_url, error):
        with open('error-%s.txt' % type, 'ab') as f:
            f.write("\"%s\",\"%s\",\"%s\",\"%s\"\n" %
                    (company_id, company_name, company_url, error))

    def write_wrong_specialty(self, company_id, company_name, company_url):
        with open('wrong-specialty-%s.txt' % self.speciality, 'ab') as f:
            f.write("\"%s\",\"%s\",\"%s\"\n" %
                    (company_id, company_name, company_url))


    def update_company_page(self, response, url_name='Home', is_home=True):

        companies_pages = \
            self.gatherer_database['company_webpages']

        soup = BeautifulSoup(response.body)

        # Remove the script and style tags
        [x.extract() for x in soup.findAll('script')]
        [x.extract() for x in soup.findAll('style')]
        [x.extract() for x in soup.select('[style*="visibility:hidden"]')]
        [x.extract() for x in soup.select('[style*="display:none"]')]

        page_text = soup.get_text()

        # strip empty lines
        page_text = "".join([s for s in page_text.strip().splitlines(True) if s.strip()])

        if self.speciality not in page_text and is_home:
            self.write_wrong_specialty(response.meta['company_id'],
                                       response.meta['company_name'],
                                       response.url)

        keywords = self.get_page_meta('keywords', response.body)
        if keywords is not None:
            keywords = [keyword.strip() for keyword in keywords.split(',')]

        title = self.get_page_meta('title', response.body)
        title_bags_of_words = self.decompose_sentences([title] if title is not None else None)

        description = self.get_page_meta('description', response.body)
        description_bags_of_words = self.decompose_sentences([description] if description is not None else None)

        abstract = self.get_page_meta('abstract', response.body)
        abstract_bags_of_words = self.decompose_sentences([abstract] if abstract is not None else None)

        sentences = self.find_sentences(page_text)
        sentences = self.decompose_sentences(sentences)

        companies_pages.update({
            'company_id': response.meta['company_id'],
            'url': response.url
        }, {
            '$setOnInsert': {
                'company_id': response.meta['company_id'],
                'url': response.url,
                'created': datetime.datetime.now()
            },

            '$set': {
                'updated': datetime.datetime.now(),
                'url_name': url_name.strip(),
                'company_name': response.meta['company_name'],
                'specialty': self.speciality,
                'title': title,
                'description': description,
                'abstract': abstract,
                'keywords': keywords,
                'is_home': is_home,
                'content': soup.prettify(),
                'content_plain_text': page_text,
                'sentences': sentences,
                'bags_of_words_in_meta': {
                    'title': title_bags_of_words,
                    'description': description_bags_of_words,
                    'abstract': abstract_bags_of_words,
                    'keywords': keywords
                }
            }
        }, upsert=True)


    def find_sentences(self, page_content):
        # http://stackoverflow.com/questions/36610179/how-to-get-the-dependency-tree-with-spacy/36612605

        # page_content = page_content.lower()

        # lines = page_content.split('\n')

        # remove lines with less than 4 words
        # processed_text = ""
        # for line in page_content.split('\n'):
        #     if line.split(' ') >= 4:
        #         processed_text += line

        if isinstance(page_content, str):
            page_content = page_content.decode('utf-8')

        doc = self.nlp(page_content.replace('\n','.\n'))

        sents = set()

        for sent in doc.sents:
            for token in sent:

                # Phasal verb?
                if token.dep_ == "prt" and token.head.pos_ == "VERB" :
                    verb = token.head.orth_
                    particle = token.orth_
                    phrasal_verb = ' '.join([verb, particle])
                    if phrasal_verb in self.verbs_to_find:
                        sents.add(sent.string)

                elif token.pos == VERB and \
                    token.lemma_ in self.verbs_to_find:
                    sents.add(sent.string)

        return list(sents) if len(list(sents)) > 0 else None
        #     for token in sent:
        #         if token.is_alpha:
        #
        # sentences_list = []
        # for line in lines:
        #     sentences_list.extend(word_tokenize(sentence) for sentence in sent_tokenize(line))
        #
        # parser = nltk.ChartParser(gro)
        # for sentence in sentences_list:
        #

        # return sentences_list

    def decompose_sentences(self, sentences):
        """
        For each sentence we are going to create different bags of words based
        on the kind of the meaning/content:

            entities: {
                persons:
                organizations:
                locations:
                products:
                events:
                work_of_art:
                languages:
            },

            noun_chunks: {
            }

            nouns: {
            }

            verbs: {
            }

        Supported entities

            PERSON	People, including fictional.
            NORP	Nationalities or religious or political groups.
            FACILITY	Buildings, airports, highways, bridges, etc.
            ORG	Companies, agencies, institutions, etc.
            GPE	Countries, cities, states.
            LOC	Non-GPE locations, mountain ranges, bodies of water.
            PRODUCT	Objects, vehicles, foods, etc. (Not services.)
            EVENT	Named hurricanes, battles, wars, sports events, etc.
            WORK_OF_ART	Titles of books, songs, etc.
            LANGUAGE	Any named language.

        :param sentences: the list of sentences
        :return: a dictionary with the different types of bags of words
        """

        sentences_list = []

        if sentences is not None:
            for sentence in sentences:

                sentence_data = {
                    'sentence': sentence,
                    'bags_of_words': None
                }

                bags_of_words = {
                    'all': [],
                    'entities': {
                        'PERSON': [],
                        'NORP': [],
                        'FACILITY': [],
                        'ORG': [],
                        'GPE': [],
                        'LOC': [],
                        'PRODUCT': [],
                        'EVENT': [],
                        'WORK_OF_ART': [],
                        'LANGUAGE': []
                    },
                    'noun_chunks': [],
                    'VERB': [],
                    'NOUN': []
                }

                doc = self.nlp(sentence if isinstance(sentence, unicode) else sentence.decode('utf-8'))

                # process the entities of the sentence
                for entity in doc.ents:
                    if entity.label_ in ['PERSON', 'NORP', 'FACILITY', 'ORG',
                                         'GPE', 'LOC', 'PRODUCT', 'EVENT',
                                         'WORK_OF_ART', 'LANGUAGE']:
                        entity_val = self.clean_stopwords(entity.string.lower())
                        if len(entity_val.strip()) > 0:
                            synonyms = self.get_synonyms(sentence, entity_val)
                            bags_of_words['entities'][entity.label_].append((entity_val, synonyms))
                            bags_of_words['all'].append((entity_val, synonyms))

                # process the noun chunks of the sentence
                for noun_chunk in doc.noun_chunks:
                    # Lemmatizing the nouns (steamming)
                    noun_chunk_val = self.clean_stopwords(noun_chunk.lemma_.lower())
                    if len(noun_chunk_val.strip()) > 0:
                        synonyms = self.get_synonyms(sentence, noun_chunk_val)

                        bags_of_words['noun_chunks'].append((noun_chunk_val, synonyms))
                        bags_of_words['all'].append((noun_chunk_val, synonyms))

                # process verbs and nouns of the sentence
                for word in doc:
                    if word.pos_ in ['VERB', 'NOUN']:
                        # Lemmatizing the words (stemming)
                        word_val = self.clean_stopwords(word.lemma_.lower())
                        if len(word_val.strip()) > 0:
                            synonyms = self.get_synonyms(sentence, word_val)

                            bags_of_words[word.pos_].append((word_val, synonyms))
                            bags_of_words['all'].append((word_val, synonyms))

                sentence_data['bags_of_words'] = bags_of_words
                sentences_list.append(sentence_data)

        return sentences_list

    def clean_stopwords(self, text):
        return ' '.join([w for w in text.split(' ') if w.lower() not in self.stop_words])


    def get_synonyms(self, sentence, word):
        from pywsd.lesk import simple_lesk

        synonyms = set()

        if isinstance(sentence, str):
            sentence = sentence.decode('utf-8')

        if isinstance(word, str):
            word = word.decode('utf-8')


        synset = simple_lesk(sentence, word)
        if synset is not None:
            for synonym in synset.lemma_names():
                synonyms.add(synonym.replace('_', ' '))

        # for idx, synset in enumerate(wordnet.synsets(word)):
        #     for synonym in synset.lemma_names():
        #         synonyms.add(synonym.replace('_', ' '))

        return list(synonyms)

    def get_page_meta(self, meta_name, page_html):
        soup = BeautifulSoup(page_html)

        value = ""
        for meta in soup.findAll("meta"):
            metaname = meta.get('name', '').lower()
            metaprop = meta.get('property', '').lower()
            if meta_name == metaname or metaprop.find(meta_name) > 0:
                if 'content' in meta.__dict__['attrs']:
                    try:
                        value = ' '.join([value, meta['content'].strip().encode('utf-8')])
                    except:
                        self.logger.error("Error looking for [%s] in the metadata. Meta: [%s]" % (meta_name, meta))
                        raise Exception("Error looking for [%s] in the metadata. Meta: [%s]" % (meta_name, meta))

        return value.strip() if value != "" else None


    def load_jim_companies(self):
        companies_collection = \
            self.gatherer_database['consolidated_company']

        companies_by_id = {}

        with open('wp.txt', 'rb') as file:
            for i, line in enumerate(file):
                values = line.split(',')
                company_id = values[0].replace('"', '')
                webpage = values[1].replace('"', '')
                if len(webpage.strip()) > 0:
                    if webpage.startswith('http//'):
                        webpage = webpage.replace('http//', 'http://')
                    if webpage.startswith('https//'):
                        webpage = webpage.replace('https//', 'https://')
                    if not webpage.startswith('http://') and not webpage.startswith('https://'):
                        webpage = "http://%s" % webpage

                    companies_by_id[company_id] = {
                        'company_id': company_id,
                        'webpage': webpage,
                        'top_speciality': self.speciality,
                    }

        # Adding the website information from the consolidated_company collection
        companies_domain = companies_collection.find(
            {
                "company_id": {
                    '$in': [company['company_id'] for (company_id, company) in
                            companies_by_id.iteritems()]},
                "webpage": {'$exists': True},
            },
            {"company_id": 1, "company_name": 1})

        for company in companies_domain:
            companies_by_id[company['company_id']]['company_name'] = company['company_name']

        self.logger.info(
            "Companies with website informed: [%d]" % len(companies_by_id))

        return companies_by_id

    def load_companies(self):

        companies_collection = \
            self.gatherer_database['consolidated_company']
        companies_top_speciality_collection = \
            self.gatherer_database['company_specialities']

        # Looking for all the companies in Machine Learning
        companies = companies_top_speciality_collection.find(
            {"tf_idf.top_tag": self.speciality},
            {"company_id": 1, "tf_idf.top_tag": 1})

        companies_by_id = {}

        for company in companies:
            companies_by_id[company['company_id']] = {
                'company_id': company['company_id'],
                'top_speciality': company['tf_idf']['top_tag']
            }

        # Adding the website information from the consolidated_company collection
        companies_domain = companies_collection.find(
            {
                "company_id": {
                    '$in': [company['company_id'] for (company_id, company) in
                            companies_by_id.iteritems()]},
                "webpage": {'$exists': True},
            },
            {"company_id": 1, "company_name": 1, "webpage": 1})

        for company in companies_domain:
            companies_by_id[company['company_id']]['company_name'] = company['company_name']
            webpage = company['webpage']
            if webpage.startswith('http//'):
                webpage = webpage.replace('http//', 'http://')
            if webpage.startswith('https//'):
                webpage = webpage.replace('https//', 'https://')
            if not webpage.startswith('http://') and not webpage.startswith(
                    'https://'):
                webpage = "http://%s" % webpage

            companies_by_id[company['company_id']]['webpage'] = webpage

        self.logger.info(
            "Companies with website informed: [%d]" % len(companies_by_id))

        return companies_by_id