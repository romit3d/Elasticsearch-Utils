from elasticsearch import Elasticsearch, helpers
import time


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
            raise Exception('Multiple indices associated with the alias. Aborting..\n'
                            'Indices Associated: %s' % aliases.keys())
        if alias in aliases.keys():
            new_index = alias + '_' + str(int(time.time()))
            try:
                client.indices.create(index=new_index, body=mapping, ignore=[400, 404])
            except Exception as e:
                raise Exception('Index creation failed. Message: %s' % e.message)
            try:
                helpers.reindex(client=client, target_index=new_index, source_index=alias, target_client=client)
            except Exception as e:
                client.indices.delete(index=new_index)
                raise Exception('Reindexing failed. Moving back to old index. Message: %s' % e.message)
            client.indices.delete(index=alias)
            try:
                client.indices.put_alias(index=new_index, name=alias)
                return True
            except Exception as e:
                raise Exception('Alias creation failed. Data backup in index: %s. Message: %s' % (new_index, e.message))
        else:
            new_index = alias + '_' + str(int(time.time()))
            current_index = aliases.keys()[0]
            try:
                client.indices.create(index=new_index, body=mapping, ignore=[400, 404])
            except Exception as e:
                raise Exception('Index creation failed. Message: %s' % e.message)
            try:
                helpers.reindex(client=client, target_index=new_index, source_index=alias, target_client=client)
            except Exception as e:
                client.indices.delete(index=new_index)
                raise Exception('Reindexing failed. Moving back to old index. Message: %s' % e.message)
            client.indices.delete_alias(index=current_index)
            client.indices.put_alias(index=new_index, name=alias)
            return True

