from collections import namedtuple
import logging

from concurrent import futures

import requests

from qvarnclient import QvarnRequests, QvarnClient

logger = logging.getLogger(__name__)

NO_DEFAULT = object()


class QvarnError(Exception):
    pass


class QvarnResourceNotFound(QvarnError):
    pass


class QvarnMultipleFoundError(QvarnError):
    pass


class QvarnUnauthorized(QvarnError):
    pass


class QvarnResourceConflict(QvarnError):
    pass


class QvarnResultDict(dict):
    """Dict wrapper to help extract data from Qvarn results."""

    def _filter_item(self, item, filters):
        for key, val in filters.items():
            if item[key] != val:
                return False
        else:
            return True

    def get_one(self, key, **filters):
        """Get one dict item satisfying filters.

        Raises ValueError if multiple items are returned and KeyError if no items satisfy
        filters.
        """
        items = self.get_multiple(key, **filters)
        if len(items) > 1:
            raise ValueError('Multiple items found for {}'.format(key))
        elif len(items) == 0:
            raise KeyError('No items found found for key {}'.format(key))
        else:
            return items[0]

    def get_multiple(self, key, **filters):
        items = self[key]
        if filters:
            items = [item for item in items
                     if self._filter_item(item, filters)]
        return items


QvarnCapabilities = namedtuple('QvarnCapabilities',
                               ['extended_project_fields'])


class QvarnApi(object):
    """Wrapper around Tilaajavastuu Qvarn client."""

    def __init__(self, qvarn_client, qvarn_capabilities=None):
        self.client = qvarn_client
        if qvarn_capabilities:
            self.caps = qvarn_capabilities
        else:
            self.caps = QvarnCapabilities(extended_project_fields=False)

    def _resolve_future(self, future):
        """Resolve requests-futures future and handle exceptions."""
        resp = future.result()
        if resp.status_code in [requests.codes.ok, requests.codes.created]:
            if resp.headers.get('content-type').lower() == 'application/json':
                return QvarnResultDict(resp.json())
            else:
                return resp.text
        else:
            if resp.status_code in [401, 403]:
                raise QvarnUnauthorized(resp.text)
            elif resp.status_code == 404:
                raise QvarnResourceNotFound(resp.text)
            elif resp.status_code == 409:
                raise QvarnResourceConflict(resp.text)
            else:
                raise QvarnError('Unknown error: {}'.format(resp.text))

    def _resolve_futures(self, futs):
        """Resolve multiple futures. Return a list of dicts."""
        futures.wait(futs)
        return [self._resolve_future(fut) for fut in futs]

    def _resolve_list_future(self, fut, *, flatten_list=True):
        resp = self._resolve_future(fut)
        if flatten_list:
            return [item['id'] for item in resp['resources']]
        else:
            return list(map(QvarnResultDict, resp['resources']))

    def get(self, resource, id, subresources=()):
        """Retrieve a resource and one or more subresources."""
        fut = self.client.resource(resource).single(id).get()
        doc = self._resolve_future(fut)

        for subresource in subresources:
            doc[subresource] = self._get_subresource(resource, subresource, id)
        return doc

    def get_file(self, resource, subresource, id):
        future = self.client.resource(resource).single(id).filesubresource(subresource).get()
        return self._resolve_future(future)

    def _get_subresource(self, resource, subresource, id):
        return self._resolve_future(
            self.client.resource(resource).single(id).subresource(subresource).get()
        )

    def get_list(self, resource):
        """Retrieve a list of IDs."""
        return self._resolve_list_future(self.client.resource(resource).get())

    def get_multiple(self, resource, ids):
        """Retrieve multiple resources in parallel. Does not fetch subresources.
        """
        futs = [self.client.resource(resource).single(id).get() for id in ids]
        return self._resolve_futures(futs)

    def get_multiple_subresources(self, resource, subresource, ids):
        futs = [self.client.resource(resource).single(id).subresource(subresource).get()
                for id in ids]
        return self._resolve_futures(futs)

    def get_version(self):
        fut = self.client.resource('version').get()
        return self._resolve_future(fut)

    def create(self, resource, payload, subresources=(), files=()):
        subresources = self._pop_subresource_data(payload, subresources)
        files = self._pop_subresource_data(payload, files)
        created = self._resolve_future(self.client.resource(resource).post(payload))
        logger.info('%r resource created with id: %r', resource, created['id'])
        self._update_subresources(resource, created, subresources)
        self._update_files(resource, created, files)
        return QvarnResultDict(created)

    def update(self, resource, id, payload, subresources=(), files=()):
        if not payload.get('revision'):
            doc = self.get(resource, id)
            payload['revision'] = doc['revision']

        subresources = self._pop_subresource_data(payload, subresources)
        files = self._pop_subresource_data(payload, files)
        updated = self._resolve_future(self.client.resource(resource).single(id).put(payload))
        logger.info('%r resource with id: %r has been updated', resource, id)
        self._update_subresources(resource, updated, subresources)
        self._update_files(resource, updated, files)
        return QvarnResultDict(updated)

    def _pop_subresource_data(self, payload, subresources):
        return {subresource: payload.pop(subresource) for subresource in subresources}

    def _update_subresources(self, resource, doc, subresources):
        for subresource, payload in subresources.items():
            doc[subresource] = self.update_subresource(resource, subresource, doc, payload)

    def _update_files(self, resource, doc, files):
        for subresource, payload in files.items():
            ct, body = payload['content_type'], payload['body']
            # Do not include file response, since it only contains id and
            # revision which are present in doc anyway.
            self.update_file(resource, subresource, doc, ct, body)

    def update_subresource(self, resource, subresource, doc, payload):
        payload['revision'] = doc['revision']
        request = self.client.resource(resource).single(doc['id']).subresource(subresource)
        response = self._resolve_future(request.put(payload))
        doc['revision'] = response['revision']
        logger.info('%r subresource of %r resource with id: %r has been updated',
                    subresource, resource, doc['id'])
        return response

    def update_file(self, resource, subresource, doc, content_type, body: bytes):
        request = self.client.resource(resource).single(doc['id']).filesubresource(subresource)
        future = request.put(body, content_type, doc['revision'])
        response = self._resolve_future(future)
        doc['revision'] = response['revision']
        logger.info('%r subresource of %r resource with id: %r has been updated',
                    subresource, resource, doc['id'])
        return response

    def delete(self, resource, id):
        result = self._resolve_future(self.client.resource(resource).single(id).delete())
        logger.info('%r resource with id: %r has been deleted', resource, id)
        return result

    def delete_multiple(self, resource, ids):
        futs = [self.client.resource(resource).single(id).delete() for id in ids]
        return self._resolve_futures(futs)

    def search(self, resource, show=(), show_all=False, **query):
        """Perform search using Django ORM style syntax.

        Example
        -------

        Search by email field::

            >>> qvarn.search('persons', email_address__exact='test@example.com')

        In case, when there are more than one field with same name, like for example here::

            {
                'contract_type': ACCOUNT_CONTRACT_TYPE,
                'contract_parties': [
                    {'type': 'person', 'resource_id': person_id},
                    {'type': 'org', 'resource_id': organisation_id},
                ]
            }

        Here we have two ``resource_id`` fields with different values. In this case you can search
        for both fields like this::

            >>> qvarn.search('contracts', resource_id__exact=(person_id, organisation_id))

        This query makes sure, that at least two ``resource_id`` fields exists with two different
        given values.

        Returns
        -------
        List[str] or List[QvarnResultDict]
            Returns a list of Qvarn IDs or QvarnResultDict's if either show or show_all is given.

        """
        criteria = []
        for key, value in query.items():
            if '__' not in key:
                field, method = key, 'exact'
            else:
                try:
                    field, method = key.split('__')
                except ValueError:
                    raise ValueError('Invalid search query {}'.format(key))
            criteria.append((method, field, value))

        search = self.client.resource(resource).search()
        if show_all:
            search = search.show_all()
        elif show:
            for field in show:
                search = search.show(field)

        for method, field, value in sorted(criteria):
            if isinstance(value, (tuple, list)):
                # Handle ``resource_id__exact=(person_id, organisation_id)`` case.
                for value_i in value:
                    search = getattr(search, method)(field, str(value_i))
            else:
                # Handle ``resource_id__exact=person_id`` case, where ``exact`` is a ``method``.
                search = getattr(search, method)(field, str(value))

        flatten_list = not (show_all or show)
        return self._resolve_list_future(search.get(), flatten_list=flatten_list)

    def search_one(self, resource, *, default=NO_DEFAULT, subresources=(), show=(), show_all=False,
                   **query):
        """Does the search and returns first result making sure that this is the only result.

        If search does not found anything, raise QvarnResourceNotFound or return ``default`` if it
        is given.

        If search returns more than one result, raise QvarnMultipleFoundError.

        If only one result is found, do another query to Qvarn to get whole resource or return
        result directly if show or show_all is specified.

        Parameters
        ----------
        resource : str
            Qvarn resource name, i.e.: 'orgs'.
        default : any
            A default value, that will be returned if search query does not match any results.
        subresources : tuple or list
            Include specified subresources of a resource. Only applicable if no show or show_all is
            specified.
        **kwargs
            All other parameters are the same as for ``search`` method.

        Returns
        -------
        QvarnResultDict, or default

        """
        result = self.search(resource, show, show_all, **query)

        if len(result) == 1:
            if show or show_all:
                return result[0]
            else:
                return self.get(resource, result[0], subresources)

        elif len(result) == 0:
            if default is NO_DEFAULT:
                raise QvarnResourceNotFound(
                    "Resource %r with %r query was not found." % (resource, query)
                )
            else:
                return default

        else:
            # XXX: Qvarn does not support unique constraints
            #      See: https://jira.tilaajavastuu.fi/browse/BOL-297
            raise QvarnMultipleFoundError(
                "Found more than one %r with %r query." % (resource, query)
            )

    def status_check(self, resources):
        """Check for resource availability.

        Tries to access all the requested resources.  Raises if anything fails.
        """
        futs = [self.client.resource(resource).search().exact('id', '*statuscheck*').get()
                for resource in resources]
        self._resolve_futures(futs)


def setup_qvarn_client(config):
    qvarn_base_url = config.get('qvarn', 'base_url')
    qvarn_client_id = config.get('qvarn', 'client_id')
    qvarn_client_secret = config.get('qvarn', 'client_secret')
    qvarn_verify_requests = config.getboolean('qvarn', 'verify_requests')
    qvarn_threads = config.getint('qvarn', 'threads', fallback=1)
    scopes = config.get('qvarn', 'scope').replace(',', ' ').split()

    qvarn_requests = QvarnRequests(
        qvarn_base_url, qvarn_client_id, qvarn_client_secret,
        scopes, qvarn_verify_requests, max_workers=qvarn_threads
    )
    qvarn_client = QvarnClient(qvarn_base_url, qvarn_requests)
    return qvarn_client


def setup_qvarn_api(config):
    qvarn_capabilities = QvarnCapabilities(
        extended_project_fields=config.getboolean('qvarn', 'extended_project_fields',
                                                  fallback=False)
    )
    return QvarnApi(setup_qvarn_client(config), qvarn_capabilities)
