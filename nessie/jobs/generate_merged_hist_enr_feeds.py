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

import json
import tempfile
from time import sleep

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError
from nessie.jobs.generate_merged_enrollment_term import GenerateMergedEnrollmentTerm
from nessie.lib.berkeley import current_term_id, future_term_id, future_term_ids, legacy_term_ids, reverse_term_ids
from nessie.lib.metadata import get_merged_enrollment_term_job_status, queue_merged_enrollment_term_jobs
from nessie.lib import queries
from nessie.lib.util import encoded_tsv_row
from nessie.merged.sis_profile import parse_merged_sis_profile
from nessie.merged.sis_profile_v1 import parse_merged_sis_profile_v1
from nessie.merged.student_terms import upload_student_term_maps, map_sis_enrollments
from nessie.models import student_schema

"""Logic to generate client-friendly merge of available data on non-current students."""

BATCH_QUERY_MAXIMUM = 5000

class GenerateMergedHistEnrFeeds(BackgroundJob):

    rds_schema = app.config['RDS_SCHEMA_STUDENT']
    rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'
    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self):
        app.logger.info(f'Starting merged non-advisee profile generation job.')

        app.logger.info('Cleaning up old data...')
        redshift.execute('VACUUM; ANALYZE;')

        status = self.generate_feeds()

        # Clean up the workbench.
        redshift.execute('VACUUM; ANALYZE;')
        app.logger.info(f'Vacuumed and analyzed.')

        return status

    def generate_feeds(self):

        self.successes = []

        # Process all unprocessed SIDS which have SIS Students API data.
        unmerged_sids = queries.get_non_advisee_unmerged_student_ids()

        self.generate_student_profile_table(unmerged_sids)
        self.generate_student_enrollments_table(unmerged_sids)

        return f'Generated {len(self.successes)} non-advisee profiles.'

    def generate_student_profile_table(self, unmerged_sids):
        with tempfile.TemporaryFile() as feed_file:
            # Work in batches so as not to overload memory.
            for i in range(0, len(unmerged_sids), BATCH_QUERY_MAXIMUM):
                sids = unmerged_sids[i:i + BATCH_QUERY_MAXIMUM]
                self.save_merged_profiles(sids, feed_file)
            if self.successes:
                student_schema.truncate_staging_table('student_profiles_hist_enr')
                student_schema.write_file_to_staging('student_profiles_hist_enr', feed_file, len(self.successes))

    def save_merged_profiles(self, sids, feed_file):
        sis_profile_feeds = queries.get_non_advisee_api_feeds(sids)
        for row in sis_profile_feeds:
            sid = row['sid']
            feed = row['feed']
            parsed_profile = parse_merged_sis_profile(feed, None, None)
            feed_file.write(encoded_tsv_row([
                sid,
                row['uid'],
                json.dumps(parsed_profile),
            ]) + b'\n')
            self.successes.append(sid)

    def generate_student_enrollments_table(self, unmerged_sids):
        with tempfile.TemporaryFile() as feed_file:
            # Work in batches so as not to overload memory.
            for i in range(0, len(unmerged_sids), BATCH_QUERY_MAXIMUM):
                sids = unmerged_sids[i:i + BATCH_QUERY_MAXIMUM]
                self.save_merged_profiles(sids, feed_file)
            if self.successes:
                student_schema.truncate_staging_table('student_profiles_hist_enr')
                student_schema.write_file_to_staging('student_profiles_hist_enr', feed_file, len(self.successes))


    def save_merged_enrollments(self, sids, feed_file):
        sis_enrollments = queries.get_non_advisee_sis_enrollments(sids)
        enrollments_map = map_sis_enrollments(sis_enrollments)
        # student_enrollments_map[term_id][sid] = term_enrollments



    def dummy(self):
        with tempfile.TemporaryFile() as feed_file:
            saved_sids, failure_count = self.load_concurrently(sids, feed_file)
            if saved_sids:
                student_schema.truncate_staging_table('sis_api_profiles_hist_enr')
                student_schema.write_file_to_staging('sis_api_profiles_hist_enr', feed_file, len(saved_sids))

        if saved_sids:
            staging_to_destination_query = resolve_sql_template_string(
                """
                DELETE FROM {redshift_schema_student}.sis_api_profiles_hist_enr WHERE sid IN
                    (SELECT sid FROM {redshift_schema_student}_staging.sis_api_profiles_hist_enr);
                INSERT INTO {redshift_schema_student}.sis_api_profiles_hist_enr
                    (SELECT * FROM {redshift_schema_student}_staging.sis_api_profiles_hist_enr);
                TRUNCATE {redshift_schema_student}_staging.sis_api_profiles_hist_enr;
                """,
            )
            if not redshift.execute(staging_to_destination_query):
                raise BackgroundJobError('Error on Redshift copy: aborting job.')

        return f'SIS student API non-advisee import job completed: {len(saved_sids)} succeeded, {failure_count} failed.'


        # Translation between canvas_user_id and UID/SID is needed to merge Canvas analytics data and SIS enrollment-based data.
        advisees_by_canvas_id = {}
        advisees_by_sid = {}
        self.successes = []
        self.failures = []
        profile_tables = self.generate_student_profile_tables(advisees_by_canvas_id, advisees_by_sid)
        if not profile_tables:
            raise BackgroundJobError('Failed to generate student profile tables.')

        feed_path = app.config['LOCH_S3_BOAC_ANALYTICS_DATA_PATH'] + '/feeds/'
        s3.upload_json(advisees_by_canvas_id, feed_path + 'advisees_by_canvas_id.json')

        upload_student_term_maps(advisees_by_sid)

        # Avoid processing Canvas analytics data for future terms and pre-CS terms.
        for term_id in (future_term_ids() + legacy_term_ids()):
            enrollment_term_map = s3.get_object_json(feed_path + f'enrollment_term_map_{term_id}.json')
            if enrollment_term_map:
                GenerateMergedEnrollmentTerm().refresh_student_enrollment_term(term_id, enrollment_term_map)

        canvas_integrated_term_ids = reverse_term_ids()
        app.logger.info(f'Will queue analytics generation for {len(canvas_integrated_term_ids)} terms on worker nodes.')
        result = queue_merged_enrollment_term_jobs(self.job_id, canvas_integrated_term_ids)
        if not result:
            raise BackgroundJobError('Failed to queue enrollment term jobs.')

        student_schema.refresh_all_from_staging(profile_tables)
        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS indexes.')

        app.logger.info('Profile generation complete; waiting for enrollment term generation to finish.')

        while True:
            sleep(1)
            enrollment_results = get_merged_enrollment_term_job_status(self.job_id)
            if not enrollment_results:
                raise BackgroundJobError('Failed to refresh RDS indexes.')
            any_pending_job = next((row for row in enrollment_results if row['status'] == 'created' or row['status'] == 'started'), None)
            if not any_pending_job:
                break

        app.logger.info('Exporting analytics data for archival purposes.')
        student_schema.unload_enrollment_terms([current_term_id(), future_term_id()])

        app.logger.info('Refreshing enrollment terms in RDS.')
        with rds.transaction() as transaction:
            if self.refresh_rds_enrollment_terms(None, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS enrollment terms.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Failed to refresh RDS enrollment terms.')

        status_string = f'Generated merged profiles ({len(self.successes)} successes, {len(self.failures)} failures).'
        errored = False
        for row in enrollment_results:
            status_string += f" {row['details']}"
            if row['status'] == 'error':
                errored = True

        student_schema.truncate_staging_table('student_enrollment_terms')
        if errored:
            raise BackgroundJobError(status_string)
        else:
            return status_string

    def generate_student_profile_tables(self, advisees_by_canvas_id, advisees_by_sid):


        # In-memory storage for generated feeds prior to TSV output.
        # TODO: store in Redis or filesystem to free up memory
        rows = {
            'student_profiles': [],
            'student_academic_status': [],
            'student_majors': [],
            'student_holds': [],
        }
        tables = ['student_profiles', 'student_academic_status', 'student_majors', 'student_holds']

        for table in tables:
            student_schema.truncate_staging_table(table)

        all_student_feeds = get_advisee_student_profile_feeds()
        if not all_student_feeds:
            app.logger.error(f'No profile feeds returned, aborting job.')
            return False
        count = len(all_student_feeds)
        app.logger.info(f'Will generate feeds for {count} students.')
        for index, student_feeds in enumerate(all_student_feeds):
            sid = student_feeds['sid']
            merged_profile = self.generate_student_profile_from_feeds(student_feeds, rows)
            if merged_profile:
                canvas_user_id = student_feeds['canvas_user_id']
                if canvas_user_id:
                    advisees_by_canvas_id[canvas_user_id] = {'sid': sid, 'uid': student_feeds['ldap_uid']}
                    advisees_by_sid[sid] = {'canvas_user_id': canvas_user_id}
                self.successes.append(sid)
            else:
                self.failures.append(sid)
        for table in tables:
            if rows[table]:
                student_schema.write_to_staging(table, rows[table])
        return tables

    def generate_student_profile_from_feeds(self, feeds, rows):
        sid = feeds['sid']
        uid = feeds['ldap_uid']
        if not uid:
            return
        if app.config['STUDENT_V1_API_PREFERRED']:
            sis_profile = parse_merged_sis_profile_v1(
                feeds.get('sis_profile_feed'),
                feeds.get('degree_progress_feed'),
            )
        else:
            sis_profile = parse_merged_sis_profile(
                feeds.get('sis_profile_feed'),
                feeds.get('degree_progress_feed'),
                feeds.get('last_registration_feed'),
            )
        demographics = feeds.get('demographics_feed') and json.loads(feeds.get('demographics_feed'))
        merged_profile = {
            'sid': sid,
            'uid': uid,
            'firstName': feeds.get('first_name'),
            'lastName': feeds.get('last_name'),
            'name': ' '.join([feeds.get('first_name'), feeds.get('last_name')]),
            'canvasUserId': feeds.get('canvas_user_id'),
            'canvasUserName': feeds.get('canvas_user_name'),
            'sisProfile': sis_profile,
            'demographics': demographics,
        }
        rows['student_profiles'].append(encoded_tsv_row([sid, json.dumps(merged_profile)]))

        if sis_profile:
            first_name = merged_profile['firstName'] or ''
            last_name = merged_profile['lastName'] or ''
            level = str(sis_profile.get('level', {}).get('code') or '')
            gpa = str(sis_profile.get('cumulativeGPA') or '')
            units = str(sis_profile.get('cumulativeUnits') or '')
            transfer = str(sis_profile.get('transfer') or False)
            expected_grad_term = str(sis_profile.get('expectedGraduationTerm', {}).get('id') or '')

            rows['student_academic_status'].append(
                encoded_tsv_row([sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term]),
            )

            for plan in sis_profile.get('plans', []):
                rows['student_majors'].append(encoded_tsv_row([sid, plan['description']]))
            for hold in sis_profile.get('holds', []):
                rows['student_holds'].append(encoded_tsv_row([sid, json.dumps(hold)]))

        return merged_profile

    def refresh_rds_indexes(self, sids, transaction):
        if not self._delete_rds_rows('student_academic_status', sids, transaction):
            return False
        if not self._refresh_rds_academic_status(transaction):
            return False
        if not self._delete_rds_rows('student_holds', sids, transaction):
            return False
        if not self._refresh_rds_holds(transaction):
            return False
        if not self._delete_rds_rows('student_names', sids, transaction):
            return False
        if not self._refresh_rds_names(transaction):
            return False
        if not self._delete_rds_rows('student_majors', sids, transaction):
            return False
        if not self._refresh_rds_majors(transaction):
            return False
        if not self._delete_rds_rows('student_profiles', sids, transaction):
            return False
        if not self._refresh_rds_profiles(transaction):
            return False
        return True

    def refresh_rds_enrollment_terms(self, sids, transaction):
        if not self._delete_rds_rows('student_enrollment_terms', sids, transaction):
            return False
        if not self._refresh_rds_enrollment_terms(transaction):
            return False
        if not self._index_rds_midpoint_deficient_grades(transaction):
            return False
        return True

    def _delete_rds_rows(self, table, sids, transaction):
        if sids:
            sql = f'DELETE FROM {self.rds_schema}.{table} WHERE sid = ANY(%s)'
            params = (sids,)
        else:
            sql = f'TRUNCATE {self.rds_schema}.{table}'
            params = None
        return transaction.execute(sql, params)

    def _refresh_rds_academic_status(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_academic_status (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, uid, first_name, last_name, level, gpa, units, transfer, expected_grad_term
                FROM {self.redshift_schema}.student_academic_status
              $REDSHIFT$)
            AS redshift_academic_status (
                sid VARCHAR,
                uid VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                level VARCHAR,
                gpa NUMERIC,
                units NUMERIC,
                transfer BOOLEAN,
                expected_grad_term VARCHAR
            ));""",
        )

    def _refresh_rds_holds(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_holds (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, feed
                    FROM {self.redshift_schema}.student_holds
              $REDSHIFT$)
            AS redshift_holds (
                sid VARCHAR,
                feed TEXT
            ));""",
        )

    def _refresh_rds_names(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_names (
            SELECT DISTINCT sid, unnest(string_to_array(
                regexp_replace(upper(first_name), '[^\w ]', '', 'g'),
                ' '
            )) AS name FROM {self.rds_schema}.student_academic_status
            UNION
            SELECT DISTINCT sid, unnest(string_to_array(
                regexp_replace(upper(last_name), '[^\w ]', '', 'g'),
                ' '
            )) AS name FROM {self.rds_schema}.student_academic_status
            );""",
        )

    def _refresh_rds_majors(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_majors (
            SELECT *
            FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                SELECT DISTINCT sid, major
                FROM {self.redshift_schema}.student_majors
              $REDSHIFT$)
            AS redshift_majors (
                sid VARCHAR,
                major VARCHAR
            ));""",
        )

    def _refresh_rds_profiles(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_profiles (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, profile
                    FROM {self.redshift_schema}.student_profiles
              $REDSHIFT$)
            AS redshift_profiles (
                sid VARCHAR,
                profile TEXT
            ));""",
        )

    def _refresh_rds_enrollment_terms(self, transaction):
        return transaction.execute(
            f"""INSERT INTO {self.rds_schema}.student_enrollment_terms (
            SELECT *
                FROM dblink('{self.rds_dblink_to_redshift}',$REDSHIFT$
                    SELECT sid, term_id, enrollment_term
                    FROM {self.redshift_schema}.student_enrollment_terms
              $REDSHIFT$)
            AS redshift_enrollment_terms (
                sid VARCHAR,
                term_id VARCHAR,
                enrollment_term TEXT
            ));""",
        )

    def _index_rds_midpoint_deficient_grades(self, transaction):
        return transaction.execute(
            f"""UPDATE {self.rds_schema}.student_enrollment_terms t1
            SET midpoint_deficient_grade = TRUE
            FROM {self.rds_schema}.student_enrollment_terms t2, json_array_elements(t2.enrollment_term::json->'enrollments') enr
            WHERE t1.sid = t2.sid
            AND t1.term_id = t2.term_id
            AND enr->>'midtermGrade' IS NOT NULL;""",
        )
