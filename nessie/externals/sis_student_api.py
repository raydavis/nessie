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

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture

"""Official access to student data."""


def get_v1_student(cs_id):
    response = _get_student(cs_id)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('any', {}).get('students', [])
        if unwrapped:
            unwrapped = unwrapped[0]
        return unwrapped
    else:
        return


def get_v2_student(cs_id):
    response = _get_v2_single_student(cs_id)
    if response and hasattr(response, 'json'):
        return response.json().get('apiResponse', {}).get('response', {})
    else:
        return


@fixture('sis_student_api_{cs_id}')
def _get_student(cs_id, mock=None):
    url = http.build_url(app.config['STUDENT_V1_API_URL'] + '/' + str(cs_id) + '/all')
    with mock(url):
        return authorized_request(url)


def get_term_gpas(cs_id):
    response = _get_registrations(cs_id)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('any', {}).get('registrations', [])
        term_gpas = {}
        for registration in unwrapped:
            # Ignore terms in which the student was not an undergraduate.
            if registration.get('academicCareer', {}).get('code') != 'UGRD':
                continue
            # Ignore terms in which the student took no classes with units. These may include future terms.
            total_units = next((u for u in registration.get('termUnits', []) if u['type']['code'] == 'Total'), None)
            if not total_units or not total_units.get('unitsTaken'):
                continue
            term_id = registration.get('term', {}).get('id')
            gpa = registration.get('termGPA', {}).get('average')
            term_units = registration.get('termUnits', [])
            units_taken_for_gpa = next((tu['unitsTaken'] for tu in term_units if tu['type']['code'] == 'For GPA'), None)
            if term_id and gpa is not None:
                term_gpas[term_id] = {
                    'gpa': gpa,
                    'unitsTakenForGpa': units_taken_for_gpa,
                }
        return term_gpas
    else:
        return

def get_v2_bulk_undergrads(size=100, page=1):
    response = _get_v2_bulk_undergrads(size, page)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('students', [])
        if len(unwrapped) < 100:
            app.logger.warn(f'End of the loop; {len(unwrapped)} students returned')
        return unwrapped
    else:
        app.logger.error(f'End of the loop; got error reponse: {response}')
        return False

def get_v2_bulk_by_sids(sids, term_id=None):
    response = _get_v2_bulk_sids(sids, term_id)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('students', [])
        if len(unwrapped) < len(sids):
            app.logger.warn(f'{len(sids)} SIDs requested; {len(unwrapped)} students returned')
        return unwrapped
    else:
        app.logger.error(f'Got error reponse: {response}')
        return False

def loop_for_sid(sid):
    page = 1
    while True:
        coes = get_v2_bulk_undergrads(page=page)
        if not coes:
            return None
        rec = next((r for r in coes if r['identifiers'][0]['id'] == sid), None)
        if rec:
            return rec
        page = page + 1


def loop_all_advisee_sids(term_id=None):
    from nessie.lib.queries import get_all_student_ids
    all_sids = [s['sid'] for s in get_all_student_ids()]
    all_feeds = []
    for i in range(0, len(all_sids), 100):
        sids = all_sids[i:i + 100]
        feeds = get_v2_bulk_by_sids(sids, term_id)
        if feeds:
            all_feeds += feeds
    app.logger.warn(f'Wanted {len(all_sids)} ; got {len(all_feeds)}')
    # The bulk API may have filtered out some students altogether, and may have returned others with feeds that
    # are missing necessary data (notably cumulative units and GPA, which are tied to registration term).
    # Try to fill that missing student data with a follow-up loop of slower single-SID API calls.
    missing_sids = list(all_sids)
    gappy_sids = []
    for feed in all_feeds:
        # TODO Instead look for "type": "student-id"
        sid = feed['identifiers'][0]['id']
        missing_sids.remove(sid)
        academic_statuses = feed.get('academicStatuses')
        if (not academic_statuses):
            gappy_sids.append(sid)
        elif len(academic_statuses) > 1:
            app.logger.warn(f'SID {sid} has mult academicStatuses: {academic_statuses}')

    app.logger.warn(f'SIDs which were not returned from list API: {missing_sids}')
    app.logger.warn(f'SIDs which were missing academicStatuses: {gappy_sids}')
    for sid in missing_sids + gappy_sids:
        feed = get_v2_student(sid)
        if feed:
            all_feeds.append(feed)
        else:
            app.logger.warn(f'Could not find data for SID {sid}')
    return all_feeds

def loop_all_undergrads():
    page = 1
    while True:
        list = get_v2_bulk_undergrads(page=page)
        if not list:
            app.logger.warn(f'All done as of page {page}')
            return None
        app.logger.warn(f'Page {page} has {len(list)} records')
        page = page + 1

def loop_undergrads_test():
    sids = []
    with open('sids_snapshot.txt', 'r') as f:
        sids = f.read().splitlines()
    app.logger.warn(f'Got {len(sids)} advisee SIDs')
    page = 1
    count = 0
    while True:
        list = get_v2_bulk_undergrads(page=page)
        if not list:
            app.logger.warn(f'All done as of page {page}')
            break
        app.logger.warn(f'Page {page} has {len(list)} records')
        count += len(list)
        for feed in list:
            sid = feed['identifiers'][0]['id']
            if sid in sids:
                sids.remove(sid)
        # if len(list) < 100: Security filters are apparently applied after the original count, and so this cannot be relied on
        #     break
        page = page + 1
    app.logger.warn(f'API returned {count} feeds; {len(sids)} advisee SIDs left to fetch: {sids}')

def _get_v2_bulk_undergrads(size=100, page=1):
    # Returns 100 records in 10-to-18 seconds. The default page-size, 50, returns in 8 seconds.
    # Whereas the SID-specific interface will always return the current cumulative GPA and units in academicStatuses
    # data, this population-wide interface will only include cumulative GPA and units if "inc-regs" is
    # specified.
    url = http.build_url(app.config['STUDENT_API_URL'], {
        # 'collection-type': 'degree_list',
        'affiliation-code': 'UNDERGRAD',
        # Engineering Undeclared
        # 'plan-code': '162B0U',
        # Used by 24788567
        # 'plan-code': '16330U',
        # Used by 3032272725
        # 'plan-code': '16306U',

        'inc-regs': True,
        'term-id': '2192',
        'inc-acad': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'page-number': page,
        'page-size': size,
        'affiliation-status': 'ALL',
        # 'affiliation-status': 'ACT',
    })
    return authorized_request_v2(url)


def _get_v2_bulk_sids(up_to_100_sids, term_id=None):
    id_list = ','.join(up_to_100_sids)
    params = {
        'id-list': id_list,
        'inc-regs': True,
        'inc-acad': True,
        'inc-cntc': True,
        'inc-attr': True,
        'inc-completed-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        'affiliation-status': 'ALL',
    }
    if term_id:
        params['term-id'] = term_id
    url = http.build_url(app.config['STUDENT_API_URL'] + '/list', params)
    return authorized_request_v2(url)


def _get_v2_single_student(sid):
    url = http.build_url(app.config['STUDENT_API_URL'] + f'/{sid}', {
        'inc-acad': True,
        'inc-attr': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        'inc-regs': True,
        'affiliation-status': 'ALL',
    })
    return authorized_request_v2(url)


def _get_v2_single_student_as_of(sid):
    url = http.build_url(app.config['STUDENT_API_URL'] + f'/{sid}', {
        'inc-acad': True,
        'inc-attr': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        'inc-regs': True,
        'as-of-date': '2019-02-01',
        'affiliation-status': 'ALL',
    })
    return authorized_request_v2(url)


@fixture('sis_registrations_api_{cs_id}')
def _get_registrations(cs_id, mock=None):
    url = http.build_url(app.config['STUDENT_API_URL'] + '/' + str(cs_id) + '/registrations')
    with mock(url):
        return authorized_request(url)


def authorized_request_v2(url):
    if app.config['STUDENT_API_USER']:
        return basic_auth(url)
    auth_headers = {
        'app_id': app.config['STUDENT_API_ID'],
        'app_key': app.config['STUDENT_API_KEY'],
        'Accept': 'application/json',
    }
    return http.request(url, auth_headers)


def authorized_request(url):
    auth_headers = {
        'app_id': app.config['STUDENT_API_ID'],
        'app_key': app.config['STUDENT_API_KEY'],
        'Accept': 'application/json',
    }
    return http.request(url, auth_headers)


def basic_auth(url):
    headers = {
        'Accept': 'application/json',
    }
    auth_params = {
        app.config['STUDENT_API_USER']: app.config['STUDENT_API_PWD'],
    }
    return http.request(url, headers, auth=(app.config['STUDENT_API_USER'], app.config['STUDENT_API_PWD']))
