import re
import sqlite3
import logging
import json
import time
import traceback

import jwt
import webtest
import qvarnclient
import requests_mock
import yaml

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
                            'body': 'blob',
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
                            'body': 'blob',
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
                            'body': 'blob',
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
    'qvarnmr_listeners': {
        'path': '/qvarnmr_listeners',
        'type': 'qvarnmr_listener',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    # Name of a project using qvarn-mr.
                    'instance': '',
                    # Resource type to listen for.
                    'resource_type': '',
                    # Listener id assigned for the resource type.
                    'listener_id': '',
                    # Owner process currently listeting for notifications.
                    'owner': '',
                    # Date and time showing when the owner was active last time.
                    'timestamp': '',
                },
            },
        ],
    },
    'qvarnmr_handlers': {
        'path': '/qvarnmr_handlers',
        'type': 'qvarnmr_handler',
        'versions': [
            {
                'version': 'v1',
                'prototype': {
                    'id': '',
                    'type': '',
                    'revision': '',
                    # QvarnMR deployed instance name.
                    'instance': '',
                    # Source and target resource type names, source and target should be unique.
                    # One source and one target should be used only by one handler.
                    'target': '',
                    'source': '',
                    # Handler version, is used to check if full resync is needed.
                    'version': 0,
                },
            },
        ],
    },
}


class RealQvarn:

    def __init__(self, requests, base_url, migrate=True, **qvarnapi_opts):
        self.requests = requests
        self.base_url = base_url
        self.db = sqlite3.connect(':memory:', check_same_thread=False)

        self.resource_type_names = set()
        self.init_qvarn_routes()
        self.init_routes()

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
        import qvarn

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
                "version": qvarn.__version__,
            },
            "implementation": {
                "name": "RealQvarn",
                "version": "1.0",
            },
        })

    def init_qvarn_routes(self):
        from qvarn.slog import SlogHandler

        # Qvarn's SlogHandler requires logging to be used in a non standard way.
        logger = logging.getLogger()
        logger.handlers = [h for h in logger.handlers if not isinstance(h, SlogHandler)]

        from qvarn import BackendApplication, SqliteAdapter, DatabaseConnection

        self.qvarnapp = BackendApplication()

        sql = SqliteAdapter()
        sql._conn = self.db
        self.qvarnapp._dbconn = DatabaseConnection()
        self.qvarnapp._dbconn.set_sql(sql)

        self.add_resource_types(RESOURCE_TYPES)

    def add_resource_types(self, resource_types):
        from qvarn import QvarnException, add_resource_type_to_server

        self.qvarnapp._store_resource_types([
            (spec, yaml.dump(spec))
            for spec in resource_types.values()
            if spec['type'] not in self.resource_type_names
        ])
        specs = self.qvarnapp._load_specs_from_db()

        for spec in specs:
            if spec['type'] not in self.resource_type_names:
                resources = add_resource_type_to_server(self.qvarnapp, spec)
                self.qvarnapp.add_routes(resources)

        with self.qvarnapp._dbconn.transaction() as t:
            for vs in self.qvarnapp._vs_list:
                if vs._resource_type not in self.resource_type_names:
                    vs.prepare_storage(t)

        self.resource_type_names.update(spec['type'] for spec in resource_types.values())

        self.qvarnapp._app.catchall = False

        def handler(request, context):
            try:
                return self.qvarn_request_handler(request, context)
            except QvarnException as e:
                context.status_code = e.status_code
                return json.dumps(e.error).encode('utf-8')

        for request_type, spec in resource_types.items():
            match = re.compile('^%s%s' % (self.base_url, spec['path']))
            self.requests.register_uri(requests_mock.ANY, match, content=handler)

    def qvarn_request_handler(self, request, context):
        context.status_code = 200

        def wrapped_app(environ, start_response):
            try:
                return self.qvarnapp._app(environ, start_response)
            except Exception:
                # We want to see all errors coming from Qvarn.
                # Make sure server._app._app.catchall is set to False.
                print(traceback.format_exc())
                raise

        app = webtest.TestApp(wrapped_app)
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
