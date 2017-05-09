import re
import sqlite3
import logging
import json
import time

import jwt
import webtest
import qvarnclient
import requests_mock

from qvarnmr.clients.qvarn import QvarnApi, QvarnClient


RESOURCE_TYPES = {
    'persons': {
        'path': '/persons',
        'type': 'person',
        'versions': [
            {
                'version': 'v0',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'names': [
                        {
                            'full_name': '',
                            'sort_key': '',
                            'titles': [''],
                            'given_names': [''],
                            'surnames': [''],
                        },
                    ],
                    'gluu_user_id': '',
                },
                'subpaths': {
                    'photo': {
                        'prototype': {
                            'body': memoryview(b''),
                            'content_type': '',
                        },
                    },
                    'private': {
                        'prototype': {
                            'date_of_birth': '',
                            'gov_ids': [
                                {
                                    'country': '',
                                    'id_type': '',
                                    'gov_id': '',
                                },
                            ],
                            'contacts': [
                                {
                                    'contact_type': '',
                                    'contact_roles': [''],
                                    'contact_source': '',
                                    'contact_timestamp': '',
                                    'phone_number': '',
                                    'email_address': '',
                                    'full_address': '',
                                    'country': '',
                                    'address_lines': [''],
                                    'post_code': '',
                                    'post_area': '',
                                    'verification_code': '',
                                    'verification_code_expiration_date': '',
                                    'email_verification_timestamp': '',
                                },
                            ],
                            'nationalities': [''],
                            'residences': [
                                {
                                    'country': '',
                                    'location': '',
                                },
                            ],
                        },
                    },
                    'sync': {
                        'prototype': {
                            'sync_sources': [
                                {
                                    'sync_source': '',
                                    'sync_id': '',
                                },
                            ],
                            'sync_revision': '',
                        },
                    },
                },
                'files': [
                    'photo',
                ],
            },
        ],
    },
    'orgs': {
        'path': '/orgs',
        'type': 'org',
        'versions': [
            {
                'version': 'v0',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'country': '',
                    'names': [''],
                    'gov_org_ids': [
                        {
                            'country': '',
                            'org_id_type': '',
                            'gov_org_id': '',
                        },
                    ],
                    'contacts': [
                        {
                            'contact_type': '',
                            'contact_roles': [''],
                            'contact_source': '',
                            'contact_timestamp': '',
                            'phone_number': '',
                            'email_address': '',
                            'full_address': '',
                            'country': '',
                            'address_lines': [''],
                            'post_code': '',
                            'post_area': '',
                            'einvoice_operator': '',
                            'einvoice_address': '',
                        },
                    ],
                    'is_luotettava_kumppani_member': False,
                },
                'subpaths': {
                    'sync': {
                        'prototype': {
                            'sync_sources': [
                                {
                                    'sync_source': '',
                                    'sync_id': '',
                                },
                            ],
                            'sync_revision': '',
                        },
                    },
                },
            },
        ],
    },
    'contracts': {
        'path': '/contracts',
        'type': 'contract',
        'versions': [
            {
                'version': 'v7',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'contract_type': '',
                    'start_date': '',
                    'end_date': '',
                    'date_created': '',
                    'date_updated': '',
                    'contract_parties': [
                        {
                            'type': '',
                            'resource_id': '',
                            'role': '',
                            'username': '',
                            'user_role': '',
                            'contacts': [
                                {
                                    'contact_type': '',
                                    'contact_roles': '',
                                    'contact_source': '',
                                    'contact_timestamp': '',
                                    'phone_number': '',
                                    'email_address': '',
                                    'full_address': '',
                                    'country': '',
                                    'address_lines': [''],
                                    'post_code': '',
                                    'post_area': '',
                                },
                            ],
                            'permissions': [
                                {
                                    'permission_name': '',
                                }
                            ],
                            'global_permissions': [
                                {
                                    'permission_name': '',
                                }
                            ],
                        },
                    ],
                    'terms_of_service': [
                        {
                            'terms_of_service_version': '',
                            'terms_of_service_language': '',
                            'acceptance_time': '',
                            'accepter_person_id': ''
                        }
                    ],
                    'right_to_work_based_on': '',
                    'id06_issuer_requires_bankid': False,
                    'id06_contact_name': '',
                    'id06_contact_email': '',
                    'contract_state': '',
                    'preferred_language': '',
                    'contract_state_history': [
                        {
                            'state': '',
                            'modified_by': '',
                            'modification_timestamp': '',
                        },
                    ],
                    'signers': [
                        {
                            'signer': '',
                            'signing_request_timestamp': '',
                            'signing_request_message': '',
                            'signing_timestamp': '',
                        },
                    ],
                },
                'subpaths': {
                    'document': {
                        'prototype': {
                            'body': memoryview(b''),
                            'content_type': '',
                        },
                    },
                    'sync': {
                        'prototype': {
                            'sync_sources': [
                                {
                                    'sync_source': '',
                                    'sync_id': '',
                                },
                            ],
                            'sync_revision': '',
                        },
                    },
                },
                'files': [
                    'document',
                ],
            },
        ],
    },
    'reports': {
        'path': '/reports',
        'type': 'report',
        'versions': [
            {
                'version': 'v0',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'org': '',
                    'report_type': '',
                    'generated_timestamp': '',
                    'tilaajavastuu_status': '',
                },
                'subpaths': {
                    'pdf': {
                        'prototype': {
                            'body': memoryview(b''),
                            'content_type': '',
                        },
                    },
                    'sync': {
                        'prototype': {
                            'sync_sources': [
                                {
                                    'sync_source': '',
                                    'sync_id': '',
                                },
                            ],
                            'sync_revision': '',
                        },
                    },
                },
                'files': [
                    'pdf',
                ],
            },
        ],
    },
    'projects': {
        'path': '/projects',
        'type': 'project',
        'versions': [
            {
                'version': 'v0',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'names': [''],
                    'project_responsible_org': '',
                    'project_responsible_person': '',
                    'project_ids': [
                        {
                            'project_id_type': '',
                            'project_id': '',
                        },
                    ],
                    # New fields, not yet in Qvarn.
                    'state': '',
                    'start_date': '',
                    'end_date': '',
                },
                'subpaths': {
                    'sync': {
                        'prototype': {
                            'sync_sources': [
                                {
                                    'sync_source': '',
                                    'sync_id': '',
                                },
                            ],
                            'sync_revision': '',
                        },
                    },
                },
            },
        ],
    },
    'bolagsfakta_suppliers': {
        'type': 'bolagsfakta_supplier',
        'path': '/bolagsfakta_suppliers',
        'versions': [
            {
                'version': 'v0',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    'project_resource_id': '',
                    'supplier_type': u'',
                    'parent_supplier_id': '',
                    'parent_org_id': '',
                    'supplier_org_id': '',
                    'contract_start_date': '',
                    'contract_end_date': '',
                    'materialized_path': [''],
                    'bolagsfakta_status': '',
                }
            },
        ],
    },
    'jobs': {
        'path': '/jobs',
        'type': 'job',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'job_type': '',
                    'person_id': '',
                    'org_id': '',
                    'submitted_at': '',
                    'started_at': '',
                    'done_at': '',
                    'status': '',
                    'reserved_until': '',
                    'parameters': [
                        {
                            'key': '',
                            'value': '',
                        }
                    ],
                },
            },
        ],
    },
    'data_cache': {
        'path': '/data_cache',
        'type': 'data_cache',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    'cache_name': '',
                    'key': '',
                    'value': '',
                    'expires_at': '',
                },
            },
        ],
    },

    # map/reduce derived resource types
    'company_reports__map': {
        'path': '/company_reports__map',
        'type': 'company_reports__map',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    '_mr_key': '',
                    '_mr_source_id': '',
                    '_mr_source_type': '',
                },
            },
        ],
    },
    'company_reports': {
        'path': '/company_reports',
        'type': 'company_report',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'type': '',
                    'id': '',
                    'revision': '',
                    '_mr_key': '',
                    '_mr_value': '',
                    'org_id': '',
                    'report_id': '',
                },
            },
        ],
    },
}


class PretenderQvarn:

    def __init__(self, requests, base_url, migrate=True, **qvarnapi_opts):
        self.requests = requests
        self.base_url = base_url
        self.db = sqlite3.connect(':memory:', check_same_thread=False)

        self.init_routes()
        self.init_qvarn_routes()

        scopes = ['scope1', 'scope2', 'scope3']
        qvarn_requests = qvarnclient.QvarnRequests(self.base_url, 'client_id', 'client_secret',
                                                   scopes, max_workers=1)
        client = QvarnClient(self.base_url, qvarn_requests)
        self.qvarn = QvarnApi(client, **qvarnapi_opts)

    def dump(self):
        return ''.join(line for line in self.db.iterdump())

    def load(self, dump):
        # Borrowed from: http://stackoverflow.com/a/548297/475477
        self.db.executescript("""\
            PRAGMA writable_schema = 1;
            DELETE FROM sqlite_master WHERE type IN ('table', 'index', 'trigger');
            PRAGMA writable_schema = 0;
            VACUUM;
            PRAGMA INTEGRITY_CHECK;
        """)
        self.db.executescript(dump)

    def init_routes(self):

        # Authentication
        self.requests.post('/auth/token', json={
            'scope': 'scope1 scope2 scope3',
            'access_token': generate_dummy_jwt_token(),
            'expires_in': 300,
        })

        # Version
        self.requests.get('/version', headers={
            'content-type': 'application/json',
        }, json={
            "api": {
                "version": "0.74",
            },
            "implementation": {
                "name": "PretenderQvarn",
                "version": "1.0",
            },
        })

    def init_qvarn_routes(self):
        from qvarn import ResourceServer, SqliteAdapter, DatabaseConnection, QvarnException
        from qvarn.slog import SlogHandler

        # Qvarn's SlogHandler requires logging to be used in a non standard way.
        logger = logging.getLogger()
        logger.handlers = [h for h in logger.handlers if not isinstance(h, SlogHandler)]

        for resource_type, resource_type_spec in RESOURCE_TYPES.items():
            server = ResourceServer()
            server.set_resource_path(resource_type_spec['path'])
            server.set_resource_type(resource_type_spec['type'])
            server.add_resource_type_versions(resource_type_spec['versions'])
            server.create_resource()

            sql = SqliteAdapter()
            sql._conn = self.db
            server._app._dbconn = DatabaseConnection()
            server._app._dbconn.set_sql(sql)

            with server._app._dbconn.transaction() as t:
                server._app._vs.prepare_storage(t)

            routes = server._app._prepare_resources()
            server._app.add_routes(routes)

            def handler(request, context):
                try:
                    return self.qvarn_request_handler(resource_type, server, request, context)
                except QvarnException as e:
                    context.status_code = e.status_code
                    return json.dumps(e.error).encode('utf-8')

            match = re.compile('^%s%s' % (self.base_url, resource_type_spec['path']))

            self.requests.register_uri(requests_mock.ANY, match, content=handler)

    def qvarn_request_handler(self, resource_type, server, request, context):
        context.status_code = 200
        app = webtest.TestApp(server._app._app)
        req = app.RequestClass.blank(
            request.url,
            method=request.method,
            headers=request.headers,
            body=request.body.encode('utf-8') if isinstance(request.body, str) else request.body,
        )
        req.environ['REQUEST_URI'] = request.url
        response = app.do_request(req, expect_errors=True)
        context.status_code = response.status_int
        context.headers = response.headers
        return response.body


def generate_dummy_jwt_token(lifetime=3600):
    iat = time.time()
    exp = time.time() + lifetime
    payload = {
        'iat': iat,
        'exp': exp,
    }
    # jwt.encode returns bytes but we want str to be able encode it in json later
    return str(jwt.encode(payload, 'shared secret key'), 'utf-8')
