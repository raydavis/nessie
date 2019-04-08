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

import nessie.externals.sis_student_api as student_api
from nessie.lib.mockingbird import MockResponse, register_mock
import pytest


class TestSisStudentApi:
    """SIS student API query."""

    def test_get_student(self, app):
        """Returns unwrapped data."""
        student = student_api.get_v1_student(11667051)
        assert len(student['academicStatuses']) == 2
        assert student['academicStatuses'][0]['currentRegistration']['academicCareer']['code'] == 'UCBX'
        assert student['academicStatuses'][1]['cumulativeGPA']['average'] == pytest.approx(3.8, 0.01)
        assert student['academicStatuses'][1]['currentRegistration']['academicLevel']['level']['description'] == 'Junior'
        assert student['academicStatuses'][1]['currentRegistration']['athlete'] is True
        assert student['academicStatuses'][1]['currentRegistration']['termUnits'][0]['unitsMax'] == 24
        assert student['academicStatuses'][1]['currentRegistration']['termUnits'][0]['unitsMin'] == 15
        assert student['academicStatuses'][1]['studentPlans'][0]['academicPlan']['plan']['description'] == 'English BA'
        assert student['academicStatuses'][1]['termsInAttendance'] == 5
        assert student['emails'][0]['emailAddress'] == 'oski@berkeley.edu'

    def test_inner_get_student(self, app):
        """Returns fixture data."""
        oski_response = student_api._get_student(11667051)
        assert oski_response
        assert oski_response.status_code == 200
        students = oski_response.json()['apiResponse']['response']['any']['students']
        assert len(students) == 1

    def test_get_term_gpas(self, app):
        gpas = student_api.get_term_gpas(11667051)
        assert len(gpas) == 7
        assert gpas['2158']['gpa'] == 3.3
        assert gpas['2158']['unitsTakenForGpa'] > 0
        assert gpas['2162']['gpa'] == 4.0
        assert gpas['2162']['unitsTakenForGpa'] > 0
        assert gpas['2165']['gpa'] == 0.0
        assert gpas['2165']['unitsTakenForGpa'] == 0
        assert gpas['2178']['gpa'] == 3.0
        assert gpas['2178']['unitsTakenForGpa'] > 0

    def test_inner_get_registrations(self, app):
        oski_response = student_api._get_registrations(11667051)
        assert oski_response
        assert oski_response.status_code == 200
        registrations = oski_response.json()['apiResponse']['response']['any']['registrations']
        assert len(registrations) == 10

    def test_user_not_found(self, app, caplog):
        """Logs 404 for unknown user and returns informative message."""
        response = student_api._get_student(9999999)
        assert '404 Client Error' in caplog.text
        assert not response
        assert response.raw_response.status_code == 404
        assert response.raw_response.json()['message']

    def test_server_error(self, app, caplog):
        """Logs unexpected server errors and returns informative message."""
        api_error = MockResponse(500, {}, '{"message": "Internal server error."}')
        with register_mock(student_api._get_student, api_error):
            response = student_api._get_student(11667051)
            assert '500 Server Error' in caplog.text
            assert not response
            assert response.raw_response.status_code == 500
            assert response.raw_response.json()['message']
