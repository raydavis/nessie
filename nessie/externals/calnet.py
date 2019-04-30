"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""


import os
import ssl

import ldap3
from nessie.lib import mockingbird

SCHEMA_DICT = {
    'berkeleyEduAffiliations': 'affiliations',
    'berkeleyEduCSID': 'csid',
    'berkeleyEduOfficialEmail': 'campus_email',
    'cn': 'sortable_name',
    'displayName': 'name',
    'mail': 'email',
    'givenName': 'first_name',
    'sn': 'last_name',
    'uid': 'uid',
}

BATCH_QUERY_MAXIMUM = 500


def client(app):
    if mockingbird._environment_supports_mocks():
        c = MockClient(app)
    else:
        c = Client(app)
    return c


class Client:
    def __init__(self, app):
        self.app = app
        self.host = app.config['LDAP_HOST']
        self.bind = app.config['LDAP_BIND']
        self.password = app.config['LDAP_PASSWORD']
        tls = ldap3.Tls(validate=ssl.CERT_REQUIRED)
        server = ldap3.Server(self.host, port=636, use_ssl=True, get_info=ldap3.ALL, tls=tls)
        self.server = server

    def connect(self):
        conn = ldap3.Connection(self.server, user=self.bind, password=self.password, auto_bind=ldap3.AUTO_BIND_TLS_BEFORE_BIND)
        return conn

    def search_csids(self, csids):
        return self._search(csids, 'berkeleyeducsid', 'csid')

    def search_uids(self, uids):
        return self._search(uids, 'uid', 'uid')

    def _search(self, ids, ldap_id_type, id_key_in_ldap_result):
        all_out = []
        for i in range(0, len(ids), BATCH_QUERY_MAXIMUM):
            batch = ids[i:i + BATCH_QUERY_MAXIMUM]
            with self.connect() as conn:
                matches = self._ldap_search(conn, batch, ldap_id_type)
                all_out += matches
                ids_of_persons_found = [p[id_key_in_ldap_result] for p in matches]
                ids_not_found = [person_id for person_id in batch if person_id not in ids_of_persons_found]
                if ids_not_found:
                    # Next, with no-match ids, search for 'expired users' in LDAP.
                    expired_users = self._ldap_search(conn, ids_not_found, ldap_id_type, search_expired=True)
                    all_out += expired_users
                    for p in expired_users:
                        ids_not_found.remove(p[id_key_in_ldap_result])
                    self.app.logger.warn(f'Did not find: {ids_not_found}')
        return all_out

    @classmethod
    def _ldap_search(cls, conn, ids, ldap_id_type, search_expired=False):
        search_filter = cls._ldap_search_filter(ids, ldap_id_type, search_expired)
        conn.search('dc=berkeley,dc=edu', search_filter, attributes=ldap3.ALL_ATTRIBUTES)
        return [_attributes_to_dict(entry) for entry in conn.entries]

    @classmethod
    def _ldap_search_filter(cls, ids, ldap_id_type, search_expired=False):
        ids_filter = ''.join(f'({ldap_id_type}={_id})' for _id in ids)
        ou_scope = '(ou=expired people)' if search_expired else '(ou=people) (ou=advcon people)'
        return f"""(&
            (objectclass=person)
            (|
                {ids_filter}
            )
            (|
                { ou_scope }
            )
        )"""


class MockClient(Client):
    def __init__(self, app):
        self.app = app
        self.host = app.config['LDAP_HOST']
        self.bind = app.config['LDAP_BIND']
        self.password = app.config['LDAP_PASSWORD']
        server = ldap3.Server.from_definition(self.host, _fixture_path('server_info'), _fixture_path('server_schema'))
        self.server = server

    def connect(self):
        conn = ldap3.Connection(self.server, user=self.bind, password=self.password, client_strategy=ldap3.MOCK_SYNC)
        conn.strategy.entries_from_json(_fixture_path('search_entries'))
        return conn


def _attributes_to_dict(entry):
    out = dict.fromkeys(SCHEMA_DICT.values(), None)
    # ldap3's entry.entry_attributes_as_dict would work for us, except that it wraps a single value as a list.
    for attr in SCHEMA_DICT:
        if attr in entry.entry_attributes:
            out[SCHEMA_DICT[attr]] = entry[attr].value
    return out


def _create_fixtures(app, sample_csids):
    fixture_output = os.environ.get('FIXTURE_OUTPUT_PATH') or mockingbird._get_fixtures_path()
    cl = Client(app)
    cl.server.info.to_file(f'{fixture_output}/calnet_server_info.json')
    cl.server.schema.to_file(f'{fixture_output}/calnet_server_schema.json')
    conn = cl.connect()
    conn.search('ou=people,dc=berkeley,dc=edu', cl._csids_filter(sample_csids), attributes=ldap3.ALL_ATTRIBUTES)
    conn.response_to_file(f'{fixture_output}/calnet_search_entries.json', raw=True)
    conn.unbind()


def _fixture_path(pattern):
    fixtures_path = mockingbird._get_fixtures_path()
    return f'{fixtures_path}/calnet_{pattern}.json'
