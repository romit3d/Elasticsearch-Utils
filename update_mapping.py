from elasticsearch import Elasticsearch, helpers
import time
import logging
FORMAT = '%(asctime)-15s Line: %(lineno)s, %(levelname)s: %(message)s'
logging.basicConfig(format=FORMAT)


class ElasticSearchUpdateMapping(object):

    def mapping_cmp(self, current_mapping, new_mapping):
        #TODO mapping comparison
        pass

    def update_mapping(self, client, alias, mapping, comparing_mapping=True):
        current_mapping = client.indices.get_mapping(index=alias)
        if comparing_mapping:
            self.mapping_cmp(current_mapping, mapping)
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
