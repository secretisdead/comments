import sys
import unittest
import uuid
import time
from datetime import datetime, timezone

from ipaddress import ip_address
from sqlalchemy import create_engine

from testhelper import TestHelper, compare_base_attributes
from base64_url import base64_url_encode, base64_url_decode
from comments import Comments, Comment, parse_id

db_url = ''

class TestComments(TestHelper):
	def setUp(self):
		if db_url:
			engine = create_engine(db_url)
		else:
			engine = create_engine('sqlite:///:memory:')

		self.comments = Comments(
			engine,
			install=True,
			db_prefix=base64_url_encode(uuid.uuid4().bytes),
		)

	def tearDown(self):
		if db_url:
			self.comments.uninstall()

	def assert_non_comment_raises(self, f):
		# any non-comment object should raise
		for invalid_comment in [
				'string',
				1,
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				f(invalid_comment)

	def test_parse_id(self):
		for invalid_input in [
				'contains non base64_url characters $%^~',
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				id, id_bytes = parse_id(invalid_input)
		expected_bytes = uuid.uuid4().bytes
		expected_string = base64_url_encode(expected_bytes)
		# from bytes
		id, id_bytes = parse_id(expected_bytes)
		self.assertEqual(id_bytes, expected_bytes)
		self.assertEqual(id, expected_string)
		# from string
		id, id_bytes = parse_id(expected_string)
		self.assertEqual(id, expected_string)
		self.assertEqual(id_bytes, expected_bytes)

	# class instantiation, create, get, and defaults
	def test_comment_class_create_get_and_defaults(self):
		self.class_create_get_and_defaults(
			Comment,
			self.comments.create_comment,
			self.comments.get_comment,
			{
				'edit_time': 0,
				'subject_id': '',
				'remote_origin': ip_address('127.0.0.1'),
				'user_id': '',
				'body': '',
			},
		)

	#TODO assert properties that default to current time
	#TODO assert properties that default to uuid bytes

	# class instantiation and db object creation with properties
	# id properties
	def test_comment_id_property(self):
		self.id_property(Comment, self.comments.create_comment, 'id')

	def test_comment_subject_id_property(self):
		self.id_property(Comment, self.comments.create_comment, 'subject_id')

	def test_comment_user_id_property(self):
		self.id_property(Comment, self.comments.create_comment, 'user_id')

	# time properties
	def test_comment_creation_time_property(self):
		self.time_property(Comment, self.comments.create_comment, 'creation')

	def test_comment_edit_time_property(self):
		self.time_property(Comment, self.comments.create_comment, 'edit')

	# string properties
	def test_comment_body_property(self):
		self.string_property(
			Comment,
			self.comments.create_comment,
			'body',
		)

	# delete
	def test_delete_comment(self):
		self.delete(
			self.comments.create_comment,
			self.comments.get_comment,
			self.comments.delete_comment,
		)

	# id collision
	def test_comments_id_collision(self):
		self.id_collision(self.comments.create_comment)

	# unfiltered count
	def test_count_comments(self):
		self.count(
			self.comments.create_comment,
			self.comments.count_comments,
			self.comments.delete_comment,
		)

	# unfiltered search
	def test_search_comments(self):
		self.search(
			self.comments.create_comment,
			self.comments.search_comments,
			self.comments.delete_comment,
		)

	# sort order and pagination
	def test_search_comments_creation_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.comments.create_comment,
			'creation_time',
			self.comments.search_comments,
		)

	# search by id
	def test_search_comments_by_id(self):
		self.search_by_id(
			self.comments.create_comment,
			'id',
			self.comments.search_comments,
			'ids',
		)

	def test_search_comments_by_subject_id(self):
		self.search_by_id(
			self.comments.create_comment,
			'subject_id',
			self.comments.search_comments,
			'subject_ids',
		)

	def test_search_comments_by_user_id(self):
		self.search_by_id(
			self.comments.create_comment,
			'user_id',
			self.comments.search_comments,
			'user_ids',
		)

	# search by time
	def search_comments_by_creation_time(self):
		self.search_by_time_cutoff(
			self.comments.create_comment,
			'creation_time',
			self.comments.search_comments,
			'created',
		)

	def search_comments_by_edit_time(self):
		self.search_by_time_cutoff(
			self.comments.create_comment,
			'edit_time',
			self.comments.search_comments,
			'edited',
		)

	# search by string like
	def test_search_comments_by_body(self):
		self.search_by_string_like(
			self.comments.create_comment,
			'body',
			self.comments.search_comments,
			'body',
		)

	# search by remote origin
	def test_search_comments_by_remote_origin(self):
		self.search_by_remote_origin(
			self.comments.create_comment,
			'remote_origin',
			self.comments.search_comments,
			'remote_origins',
		)

	def test_update_comment(self):
		# update_comment instantiates a Comment object so anything that raises in
		# test_comment_class_create_get_and_defaults should raise
		comment = self.comments.create_comment()
		subject_id1, subject_id1_bytes = parse_id(uuid.uuid4().bytes)
		subject_id2, subject_id2_bytes = parse_id(uuid.uuid4().bytes)
		user_id1, user_id1_bytes = parse_id(uuid.uuid4().bytes)
		user_id2, user_id2_bytes = parse_id(uuid.uuid4().bytes)

		# update_comment can receive a base64_url string
		properties = {
			'creation_time': 1111111111,
			'edit_time': 1234567890,
			'subject_id': subject_id1,
			'remote_origin': ip_address('1.2.3.4'),
			'user_id': user_id1,
			'body': 'test1',
		}
		self.comments.update_comment(comment.id, **properties)
		comment = self.comments.get_comment(comment.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(comment, key), value)

		# update_comment can receive bytes-like
		properties = {
			'creation_time': 2222222222,
			'edit_time': 2345678901,
			'subject_id': subject_id2,
			'remote_origin': ip_address('2.3.4.5'),
			'user_id': user_id2,
			'body': 'test2',
		}
		self.comments.update_comment(comment.id_bytes, **properties)
		comment = self.comments.get_comment(comment.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(comment, key), value)

		self.assert_invalid_id_raises(self.comments.update_comment)

	# anonymization
	def test_anonymize_user_id(self):
		id = uuid.uuid4().bytes
		comment = self.comments.create_comment(user_id=id)

		count_methods_filter_fields = [
			(self.comments.count_comments, 'user_ids'),
		]
		for count, filter_field in count_methods_filter_fields:
			self.assertEqual(1, count(filter={filter_field: id}))

		new_id_bytes = self.comments.anonymize_id(id)

		for count, filter_field in count_methods_filter_fields:
			self.assertEqual(0, count(filter={filter_field: id}))

		# assert comments still exist, but with the new id as subject/object
		for count, filter_field in count_methods_filter_fields:
			self.assertEqual(1, count(filter={filter_field: new_id_bytes}))

	#TODO passing in an id to use for anonymization is allowed, so test it
	def test_anonymize_id_with_new_id(self):
		pass

	def test_anonymize_comment_origins(self):
		origin1 = '1.2.3.4'
		expected_anonymized_origin1 = '1.2.0.0'
		comment1 = self.comments.create_comment(remote_origin=origin1)

		origin2 = '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
		expected_anonymized_origin2 = '2001:0db8:85a3:0000:0000:0000:0000:0000'
		comment2 = self.comments.create_comment(remote_origin=origin2)

		comments = self.comments.search_comments()
		self.comments.anonymize_comment_origins(comments)

		anonymized_comment1 = self.comments.get_comment(comment1.id)
		anonymized_comment2 = self.comments.get_comment(comment2.id)

		self.assertEqual(
			expected_anonymized_origin1,
			anonymized_comment1.remote_origin.exploded,
		)
		self.assertEqual(
			expected_anonymized_origin2,
			anonymized_comment2.remote_origin.exploded,
		)

if __name__ == '__main__':
	if '--db' in sys.argv:
		index = sys.argv.index('--db')
		if len(sys.argv) - 1 <= index:
			print('missing db url, usage:')
			print(' --db "dialect://user:password@server"')
			quit()
		db_url = sys.argv[index + 1]
		print('using specified db: "' + db_url + '"')
		del sys.argv[index:]
	else:
		print('using sqlite:///:memory:')
	print(
		'use --db [url] to test with specified db url'
			+ ' (e.g. sqlite:///comments_tests.db)'
	)
	unittest.main()
