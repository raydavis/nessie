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

from timeit import default_timer as timer

from flask import current_app as app
from nessie.lib import http
from nessie.lib.mockingbird import fixture

"""Official access to student data."""


def get_v1_student(cs_id):
    response = _get_v1_student(cs_id)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('any', {}).get('students', [])
        if unwrapped:
            unwrapped = unwrapped[0]
        return unwrapped
    else:
        return


def get_v2_student(cs_id, term_id=None, as_of=None):
    response = _get_v2_single_student(cs_id, term_id, as_of)
    if response and hasattr(response, 'json'):
        return response.json().get('apiResponse', {}).get('response', {})
    else:
        return


@fixture('sis_student_api_{cs_id}')
def _get_v1_student(cs_id, mock=None):
    url = http.build_url(app.config['STUDENT_V1_API_URL'] + '/' + str(cs_id) + '/all')
    with mock(url):
        return authorized_request(url)


def _get_v1_student_acst(cs_id):
    url = http.build_url(app.config['STUDENT_V1_API_URL'] + '/' + str(cs_id) + '/academic-status')
    return authorized_request(url)


def get_term_gpas(cs_id):
    response = _get_v1_registrations(cs_id)
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


def get_v2_bulk_undergrads(term_id, size=100, page=1):
    response = _get_v2_bulk_undergrads(term_id, size, page)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('students', [])
        if len(unwrapped) < size:
            app.logger.warn(f'{size} students requested; only {len(unwrapped)} returned')
        return unwrapped
    else:
        app.logger.error(f'End of the loop; got error response: {response}')
        return False


def get_v2_bulk_by_sids(sids, term_id=None, as_of=None, with_registration=False):
    response = _get_v2_bulk_sids(sids, term_id, as_of, with_registration)
    if response and hasattr(response, 'json'):
        unwrapped = response.json().get('apiResponse', {}).get('response', {}).get('students', [])
        if len(unwrapped) < len(sids):
            app.logger.warn(f'{len(sids)} SIDs requested; {len(unwrapped)} students returned')
        return unwrapped
    else:
        app.logger.error(f'Got error response: {response}')
        return False


def loop_all_advisee_sids_v1(all_sids=None):
    from nessie.lib.queries import get_all_student_ids
    if not all_sids:
        all_sids = [s['sid'] for s in get_all_student_ids()]
    all_feeds = []
    sids_without_academic_statuses = []
    sids_without_cum_gpa = []
    start_api = timer()
    success_count = 0
    failure_count = 0
    index = 1
    for csid in all_sids:
        app.logger.info(f'Fetching SIS student API for SID {csid} ({index} of {len(all_sids)})')
        feed = get_v1_student(csid)
        if feed:
            success_count += 1
            academic_statuses = feed.get('academicStatuses')
            if not academic_statuses:
                sids_without_academic_statuses.append(csid)
            else:
                academic_status = next(
                    (ac for ac in academic_statuses if ac['studentCareer']['academicCareer']['code'] != 'UCBX'),
                    None,
                )
                if academic_status and not academic_status.get('cumulativeGPA'):
                    sids_without_cum_gpa.append(csid)
            all_feeds.append(feed)
        else:
            failure_count += 1
            app.logger.error(f'SIS student API import failed for CSID {csid}.')
        index += 1
    app.logger.warn(f'Wanted {len(all_sids)} ; got {len(all_feeds)} in {timer() - start_api} secs')
    app.logger.warn(f'sids_without_academic_statuses = {sids_without_academic_statuses}')
    app.logger.warn(f'sids_without_cum_gpa = {sids_without_cum_gpa}')
    return {
        'all_feeds': all_feeds,
        'sids_without_academic_statuses': sids_without_academic_statuses,
        'sids_without_cum_gpa': sids_without_cum_gpa,
    }


def loop_all_advisee_sids(term_id=None, as_of=None, with_registration=False):
    from nessie.lib.queries import get_all_student_ids
    all_sids = [s['sid'] for s in get_all_student_ids()]
    all_feeds = []
    start_api = timer()
    # for i in range(0, len(all_sids), 100):
    #     sids = all_sids[i:i + 100]
    #     feeds = get_v2_bulk_by_sids(sids, term_id, as_of, with_registration)
    #     if feeds:
    #         all_feeds += feeds
    #
    #     if i > 600:
    #         break
    all_sids = all_sids[0:600]
    for sid in all_sids:
        feed = get_v2_student(sid, term_id)
        if feed:
            all_feeds.append(feed)

    app.logger.warn(f'Wanted {len(all_sids)} ; got {len(all_feeds)} in {timer() - start_api} secs')
    # The bulk API may have filtered out some students altogether, and may have returned others with feeds that
    # are missing necessary data (notably cumulative units and GPA, which are tied to registration term).
    # Try to fill that missing student data with a follow-up loop of slower single-SID API calls.
    missing_sids = list(all_sids)
    ucbx_only_sids = []
    sids_without_academic_statuses = {}
    sids_without_cum_gpa = {}

    count_without_academic_statuses = 0
    count_sids_without_cum_gpa = 0
    for feed in all_feeds:
        sid = next((id['id'] for id in feed['identifiers'] if id['type'] == 'student-id'), None)
        if not sid:
            app.logger.error(f"Got a student feed with no student-id: {feed['identifiers']}")
            continue
        missing_sids.remove(sid)
        registrations = feed.get('registrations')
        academic_statuses = feed.get('academicStatuses')
        if registrations:
            last_date = registrations[-1]['term']['endDate']
        else:
            last_date = ''
        if (not academic_statuses):
            count_without_academic_statuses += 1
            sids_without_academic_statuses.setdefault(last_date, []).append(sid)
        else:
            academic_status = next(
                (ac for ac in academic_statuses if ac['studentCareer']['academicCareer']['code'] != 'UCBX'),
                None,
            )
            if not academic_status:
                ucbx_only_sids.append(sid)
                continue
            if not academic_status.get('cumulativeGPA'):
                if not last_date:
                    last_date = (
                        academic_status.get('studentCareer', {}).get('toDate')
                        or academic_status.get('studentPlans', [{}])[0].get('toDate')
                        or ''
                    )
                count_sids_without_cum_gpa += 1
                sids_without_cum_gpa.setdefault(last_date, []).append(sid)
    app.logger.warn(f'{len(missing_sids)} SIDs were not returned from list API: {missing_sids}')
    app.logger.warn(f'{len(ucbx_only_sids)} SIDs were UCBX only: {ucbx_only_sids}')
    app.logger.warn(f'{count_without_academic_statuses} SIDs were missing academicStatuses: {sids_without_academic_statuses}')
    app.logger.warn(f'{count_sids_without_cum_gpa} SIDs  were missing cumumultiveGPA: {sids_without_cum_gpa}')
    return {
        'all_feeds': all_feeds,
        'missing_sids': missing_sids,
        'ucbx_only_sids': ucbx_only_sids,
        'sids_without_academic_statuses': sids_without_academic_statuses,
        'sids_without_cum_gpa': sids_without_cum_gpa,
    }


def _get_v2_bulk_undergrads(term_id, size=100, page=1):
    url = http.build_url(
        app.config['STUDENT_API_URL'], {
            'affiliation-code': 'UNDERGRAD',
            'affiliation-status': 'ALL',
            'inc-acad': True,
            'inc-cntc': True,
            'inc-completed-programs': True,
            'inc-regs': True,
            'page-number': page,
            'page-size': size,
            'term-id': term_id,
        },
    )
    return authorized_request_v2(url)


def _get_v2_bulk_sids(up_to_100_sids, term_id=None, as_of=None, with_registration=False):
    id_list = ','.join(up_to_100_sids)
    params = {
        'id-list': id_list,
        'affiliation-status': 'ALL',
        'inc-acad': True,
        'inc-attr': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        # 'inc-regs': True,
    }
    if term_id:
        params['term-id'] = term_id
    if as_of:
        params['as-of-date'] = as_of
    if with_registration:
        params['inc-regs'] = True
    url = http.build_url(app.config['STUDENT_API_URL'] + '/list', params)
    return authorized_request_v2(url)


def _get_v2_single_student(sid, term_id=None, as_of=None):
    params = {
        'affiliation-status': 'ALL',
        'inc-acad': True,
        'inc-attr': True,
        'inc-cntc': True,
        'inc-completed-programs': True,
        'inc-dmgr': True,
        'inc-gndr': True,
        'inc-regs': True,
    }
    if term_id:
        params['term-id'] = term_id
    if as_of:
        # In format '2018-12-01'. Choosing a date before the end of the no-longer-enrolled student's last enrolled
        # term appears to be the only way to restore a full v1-style academicStatuses element
        # which includes cumulativeUnits, cumulativeGPA, termsInAttendance, and major.
        params['as-of-date'] = as_of
    url = http.build_url(app.config['STUDENT_API_URL'] + f'/{sid}', params)
    return authorized_request_v2(url)


@fixture('sis_registrations_api_{cs_id}')
def _get_v1_registrations(cs_id, mock=None):
    url = http.build_url(app.config['STUDENT_V1_API_URL'] + '/' + str(cs_id) + '/registrations')
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
    return http.request(url, headers, auth=(app.config['STUDENT_API_USER'], app.config['STUDENT_API_PWD']))
