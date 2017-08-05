from elasticsearch import Elasticsearch, helpers
import time
import logging
FORMAT = '%(asctime)-15s Line: %(lineno)s, %(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT)


class ElasticSearchUpdateMapping(object):
    '''
    Zero-Downtime elasticsearch mapping update both for aliased and non aliased indexes
    '''
    def mapping_cmp(self, current_mapping, new_mapping, comparison_level, check):
        #TODO mapping comparison other levels
        if comparison_level == 0:
            # Checks for all the keys
            res = self.key_comparison(current_mapping.keys()[0], new_mapping)
            if check == 'hard' and not res:
                logging.error('Mapping comparison failed')
                return False
            return True
        # by default
        return True

    def update_mapping(self, client, alias, mapping, comparing_mapping=False, comparison_level=0, check='soft'):
        '''
        Simple zero-downtime elasticsearch mapping update function

        :param client: instance of :class:`~elasticsearch.Elasticsearch` to use
        :param alias: index to update mapping
        :param mapping: new mapping for the index
        :param comparing_mapping: whether to compare old and new mapping
        :param comparison_level: mapping comparison level
        :param check: 'hard' or 'soft' whether to stop or not on failed mapping check
        :return: {'message': 'Update successful'} or {'message': 'Update failed'}
        '''
        current_mapping = client.indices.get_mapping(index=alias)
        if comparing_mapping:
            if not self.mapping_cmp(current_mapping, mapping, comparison_level, check):
                return {'Message': 'Update failed'}
        aliases = client.indices.get_alias(index=alias)
        if len(aliases.keys()) > 1:
            logging.error('Multiple indices associated with the alias. Aborting...')
            logging.error('Indices Associated: %s' % aliases.keys())
            return {'Message': 'Update failed'}
        if alias in aliases.keys():
            new_index = alias + '_' + str(int(time.time()))
            try:
                client.indices.create(index=new_index, body=mapping, ignore=[400, 404])
            except Exception as e:
                logging.warning('Index creation failed')
                logging.error(e)
                return {'Message': 'Update failed'}
            try:
                helpers.reindex(client=client, target_index=new_index, source_index=alias, target_client=client)
            except Exception as e:
                client.indices.delete(index=new_index)
                logging.warning('Reindexing failed')
                logging.error(e)
                return {'Message': 'Update failed'}
            client.indices.delete(index=alias)
            try:
                client.indices.put_alias(index=new_index, name=alias)
                return {'Message': 'Update successful'}
            except Exception as e:
                logging.warning('Alias creation failed. Data backup in index: %s' % new_index)
                logging.error(e)
                return {'Message': 'Update failed'}
        else:
            new_index = alias + '_' + str(int(time.time()))
            current_index = aliases.keys()[0]
            try:
                client.indices.create(index=new_index, body=mapping, ignore=[400, 404])
            except Exception as e:
                logging.warning('Index creation failed')
                logging.error(e)
                return {'Message': 'Update failed'}
            try:
                helpers.reindex(client=client, target_index=new_index, source_index=alias, target_client=client)
            except Exception as e:
                client.indices.delete(index=new_index)
                logging.warning('Reindexing failed')
                logging.error(e)
                return {'Message': 'Update failed'}
            client.indices.delete_alias(index=current_index, name=alias)
            client.indices.put_alias(index=new_index, name=alias)
            return {'Message': 'Update successful'}

    def key_comparison(self, cuerrent_mapping, new_mapping):
        doc_types_current_mapping = cuerrent_mapping['mappings'].keys()
        doc_types_new_mapping = new_mapping['mappings'].keys()
        if not self.x_in_y(set(doc_types_current_mapping), set(doc_types_new_mapping)):
            logging.warning('doc_types missing in new mapping')
            return False
        for doc_type in doc_types_current_mapping:
            if not self.x_in_y(cuerrent_mapping['mappings'][doc_type]['properties'],
                        new_mapping['mappings'][doc_type]['properties']):
                logging.warning('fields missing in new mapping')
                return False
        return True

    def x_in_y(self, set_1, set_2):
        mapping_check = set_1.issubset(set_2)
        if mapping_check:
            return True
        else:
            return False
