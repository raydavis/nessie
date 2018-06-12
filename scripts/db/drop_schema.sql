/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation
 * for educational, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby granted, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
 * Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
 * http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
 *
 * IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--

ALTER TABLE IF EXISTS ONLY public.student_athletes DROP CONSTRAINT IF EXISTS student_athletes_sid_fkey;
ALTER TABLE IF EXISTS ONLY public.student_athletes DROP CONSTRAINT IF EXISTS student_athletes_group_code_fkey;
ALTER TABLE IF EXISTS ONLY public.json_cache DROP CONSTRAINT IF EXISTS json_cache_pkey;
ALTER TABLE IF EXISTS ONLY public.json_cache DROP CONSTRAINT IF EXISTS json_cache_key_key;
ALTER TABLE IF EXISTS public.json_cache ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS ONLY public.athletics DROP CONSTRAINT IF EXISTS athletics_pkey;

--

ALTER TABLE IF EXISTS ONLY public.students DROP CONSTRAINT IF EXISTS students_pkey;
ALTER TABLE IF EXISTS ONLY public.student_athletes DROP CONSTRAINT IF EXISTS student_athletes_pkey;
ALTER TABLE IF EXISTS ONLY public.athletics DROP CONSTRAINT IF EXISTS athletics_pkey;

--

DROP TABLE IF EXISTS public.students;
DROP TABLE IF EXISTS public.student_athletes;
DROP SEQUENCE IF EXISTS public.json_cache_id_seq;
DROP TABLE IF EXISTS public.json_cache;
DROP TABLE IF EXISTS public.athletics;

--
