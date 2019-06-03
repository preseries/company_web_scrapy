# encoding: utf-8

import sys
import argparse
import os
import io
from pymongo.mongo_client import MongoClient

from common.common import mkdatetime, get_snapshot_date, \
    get_file_by_snapshot_date

mongo_client = None
gatherer_database = None
company_webpages_collection = None

META_ENTITIES = 'title', 'abstract', 'description'

FILES_PREFIX = 'speciality'

VALID_ENTITIES = ['LOC', 'PERSON', 'PRODUCT', 'GPE', 'LANGUAGE']

def init_db(gatherer_db_uri,company_webpages_collection_name):

    global mongo_client, gatherer_database, company_webpages_collection

    mongo_client = MongoClient(gatherer_db_uri)
    gatherer_database = mongo_client.get_default_database()
    company_webpages_collection = gatherer_database[company_webpages_collection_name]


def init_csv_files(snapshot_date, path):

    files = [i for i in os.listdir(path)
             if os.path.isfile(os.path.join(path, i))
                and i.startswith(FILES_PREFIX)
                and i[len(i)-10:len(i)] == snapshot_date.strftime('%Y-%m-%d')]

    for file in files:
        os.remove("%s/%s" % (path, file))

def write_header(file_name, columns_name):
    with open(file_name, 'ab') as file:
        file.write("%s\n" % ",".join(columns_name))


def escape(text):
    if isinstance(text, str):
        text = text.decode('utf-8')

    return text. \
        replace('"', '""'). \
        replace('\r\n', '.'). \
        replace('\r', '.'). \
        replace('\n', '.')

def write_data(file_name, columns_name,
               company_id,
               meta_terms, meta_synonyms,
               sentences_terms, sentences_synonyms):

    if not os.path.exists(file_name):
        write_header(file_name, columns_name)

    meta_terms_str = escape(','.join(meta_terms))
    meta_synonyms_str = escape(','.join(meta_synonyms))
    sentences_terms_str = escape(','.join(sentences_terms))
    sentences_synonyms_str = escape(','.join(sentences_synonyms))

    with io.open(file_name, 'a', encoding="utf-8") as file:
        file.write(
            "\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"\n" % (
                escape(company_id),
                meta_terms_str,
                meta_synonyms_str,
                sentences_terms_str,
                sentences_synonyms_str,
                ",".join([meta_terms_str, meta_synonyms_str, sentences_terms_str, sentences_synonyms_str])
            ))


def process_meta(company):

    meta_terms = []
    meta_synonyms = []

    for bag_of_terms in company['bags_of_words_in_meta']:
        for meta_entity in META_ENTITIES:
            for entity_data in bag_of_terms[meta_entity]:
                entity_bag_of_words = entity_data['bags_of_words']

                if len(entity_bag_of_words['noun_chunks']) > 0:
                    for data in entity_bag_of_words['noun_chunks']:
                        meta_terms.append(data[0])
                        if len(data[1]) > 0:
                            meta_synonyms.extend(data[1])

                for valid_entity in VALID_ENTITIES:
                    if len(entity_bag_of_words['entities'][valid_entity]) > 0:
                        for data in entity_bag_of_words['entities'][valid_entity]:
                            meta_terms.append(data[0])
                            if len(data[1]) > 0:
                                meta_synonyms.extend(data[1])

    return meta_terms, meta_synonyms

def process_sentence(sentence):

    sentence_terms = []
    sentence_synonyms = []

    bag_of_words = sentence['bags_of_words']

    if len(bag_of_words['noun_chunks']) > 0:
        for data in bag_of_words['noun_chunks']:
            sentence_terms.append(data[0])
            if len(data[1]) > 0:
                sentence_synonyms.extend(data[1])

    for valid_entity in VALID_ENTITIES:
        if len(bag_of_words['entities'][valid_entity]) > 0:
            for data in bag_of_words['entities'][valid_entity]:
                sentence_terms.append(data[0])
                if len(data[1]) > 0:
                    sentence_synonyms.extend(data[1])

    return sentence_terms, sentence_synonyms


def process_sentences(company):

    sentence_terms = []
    sentence_synonyms = []

    for list_of_sentences in company['sentences']:
        for sentence in list_of_sentences:
            local_terms, local_synonyms = process_sentence(sentence)
            sentence_terms.extend(local_terms)
            sentence_synonyms.extend(local_synonyms)

    return sentence_terms, sentence_synonyms

def do_process(last_execution, current_date, output_path, wf_output_path,
               gatherer_db_uri, company_webpages_collection_name):

    init_db(gatherer_db_uri, company_webpages_collection_name)

    # snapshot_date is calculated from current_execution_date
    snapshot_date = get_snapshot_date(current_date)

    init_csv_files(snapshot_date, wf_output_path)

    print "Processing snapshot_date: [%s]" % snapshot_date.strftime('%Y-%m-%d')

    pipeline = [
        { '$group': {
                '_id': {
                    'speciality': "$specialty",
                    'company_id': "$company_id"
                },
                'bags_of_words_in_meta': {
                    '$addToSet': "$bags_of_words_in_meta"
                },
                'sentences': {
                    '$addToSet': "$sentences"
                }
        }},
        { '$project': {
            'speciality': "$_id.speciality",
            'company_id': "$_id.company_id",
            'bags_of_words_in_meta': 1,
            'sentences': 1}
        }]

    for i, company in enumerate(company_webpages_collection.aggregate(pipeline, allowDiskUse=True)):

        csv_file_name = "%s/%s_%s_%s" % (wf_output_path,
                                     FILES_PREFIX,
                                     company['speciality'].replace(' ', '_'),
                                     snapshot_date.strftime('%Y-%m-%d'))

        meta_terms, meta_synonyms = process_meta(company)
        sentences_terms, sentences_synonyms = process_sentences(company)

        write_data(csv_file_name,
                   ['company_id','meta_terms','meta_synonyms',
                    'sentences_terms', 'sentences_synonyms', 'all_terms'],
                   company['company_id'],
                   meta_terms, meta_synonyms,
                   sentences_terms, sentences_synonyms)


def main(args=sys.argv[1:]):
    """Parses command-line parameters and calls the actual main function.
    :param args: List of arguments
    """

    # Process arguments
    parser = argparse.ArgumentParser(
        description="Create Companies Text Dataset by Speciality",
        epilog="PreSeries, Inc")

    # Path to source with data

    # Last execution date
    parser.add_argument('--context_last_execution_date',
                        required=False,
                        action='store',
                        dest='last_execution_date',
                        default=None,
                        type=mkdatetime,
                        help="The previous date in which this process"
                             " was started."
                             " Ex. '2015-12-16 12:49:34.194736")
    # Current execution date
    parser.add_argument('--context_current_execution_date',
                        required=False,
                        action='store',
                        dest='current_execution_date',
                        default=None,
                        type=mkdatetime,
                        help="The current execution date of this process."
                             " Ex. '2015-12-16 12:49:34.194736")
    # Output Path
    parser.add_argument('--context_output_path',
                        required=False,
                        action='store',
                        dest='output_path',
                        default=None,
                        type=str,
                        help="The path to be used as a local repository "
                             "for current instance of the worklfow."
                             " Ex. '/data/etlprocess/papis_dataset/"
                             "upload_dataset_2016-05-01_20:20:12.32333'")

    # Global Output Path
    parser.add_argument('--context_wf_output_path',
                        required=False,
                        action='store',
                        dest='wf_output_path',
                        default=None,
                        type=str,
                        help="The path to be used as a local repository for "
                             "all the instances of the same workflow."
                             " Ex. '/data/etlprocess/....'")

    # DB Uri to reach the Phases Collection
    parser.add_argument('--gatherer-db-uri',
                        required=True,
                        action='store',
                        dest='gatherer_db_uri',
                        default="mongodb://localhost/TauCeti_pre_gatherer",
                        help="The gatherer db uri connection ."
                             "Ex. mongodb://localhost/TauCeti_pre_gatherer")
    # Phases Collection
    parser.add_argument('--company-webpages-collection-name',
                        required=False,
                        action='store',
                        dest='company_webpages_collection_name',
                        default="company_webpages",
                        help="The collection that holds the company "
                             "crawled webpages."
                             " Ex. company_webpages")

    args, unknown = parser.parse_known_args(args)

    last_execution = args.last_execution_date
    curr_date = args.current_execution_date

    output_path = args.output_path if 'output_path' in args else None
    wf_output_path = args.wf_output_path if 'wf_output_path' in args else None

    do_process(last_execution, curr_date, output_path, wf_output_path,
               args.gatherer_db_uri, args.company_webpages_collection_name)

    return


if __name__ == "__main__":
    main()