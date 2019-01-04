from hdfs.util import AsyncWriter, HdfsError
from collections import deque
from contextlib import contextmanager
from getpass import getuser
from itertools import repeat
from multiprocessing.pool import ThreadPool
from random import sample
from shutil import move, rmtree
from six import add_metaclass
from six.moves.urllib.parse import quote
from threading import Lock
import codecs
import logging as lg
import os
import os.path as osp
import posixpath as psp
import re
import requests as rq
import sys
import time
from hdfs.client import Client
from hdfs.client import _logger
from hdfs.client import _map_async

class MyClient(Client):

    def __init__(self, url, root=None, proxy=None, timeout=None, session=None):
        super().__init__(url, root, proxy, timeout, session)

    def newupload(self, hdfs_path, local_path, n_threads=1, temp_dir=None,
               chunk_size=2 ** 16, progress=None, cleanup=True, **kwargs):
        """Upload a file or directory to HDFS.

        :param hdfs_path: Target HDFS path. If it already exists and is a
          directory, files will be uploaded inside.
        :param local_path: Local path to file or folder. If a folder, all the files
          inside of it will be uploaded (note that this implies that folders empty
          of files will not be created remotely).
        :param n_threads: Number of threads to use for parallelization. A value of
          `0` (or negative) uses as many threads as there are files.
        :param temp_dir: Directory under which the files will first be uploaded
          when `overwrite=True` and the final remote path already exists. Once the
          upload successfully completes, it will be swapped in.
        :param chunk_size: Interval in bytes by which the files will be uploaded.
        :param progress: Callback function to track progress, called every
          `chunk_size` bytes. It will be passed two arguments, the path to the
          file being uploaded and the number of bytes transferred so far. On
          completion, it will be called once with `-1` as second argument.
        :param cleanup: Delete any uploaded files if an error occurs during the
          upload.
        :param \*\*kwargs: Keyword arguments forwarded to :meth:`write`. In
          particular, set `overwrite` to overwrite any existing file or directory.

        On success, this method returns the remote upload path.

        """
        if chunk_size <= 0:
            raise ValueError('Upload chunk size must be positive.')
        _logger.info('Uploading %r to %r.', local_path, hdfs_path)

        def _upload(_path_tuple):
            """Upload a single file."""
            _local_path, _temp_path = _path_tuple
            _logger.debug('Uploading %r to %r.', _local_path, _temp_path)

            def wrap(_reader, _chunk_size, _progress):
                """Generator that can track progress."""
                nbytes = 0
                while True:
                    chunk = _reader.read(_chunk_size)
                    if chunk:
                        if _progress:
                            nbytes += len(chunk)
                            _progress(_local_path, nbytes)
                        yield chunk
                    else:
                        break
                if _progress:
                    _progress(_local_path, -1)

            with open(_local_path, 'r', encoding="utf-8") as reader:
                self.write(_temp_path, wrap(reader, chunk_size, progress), **kwargs)

        # First, we gather information about remote paths.
        hdfs_path = self.resolve(hdfs_path)
        temp_path = None
        try:
            statuses = [status for _, status in self.list(hdfs_path, status=True)]
        except HdfsError as err:
            message = str(err)
            if 'not a directory' in message:
                # Remote path is a normal file.
                if not kwargs.get('overwrite'):
                    raise HdfsError('Remote path %r already exists.', hdfs_path)
            elif 'does not exist' in message:
                # Remote path doesn't exist.
                temp_path = hdfs_path
            else:
                # An unexpected error occurred.
                raise err
        else:
            # Remote path is a directory.
            suffixes = set(status['pathSuffix'] for status in statuses)
            local_name = osp.basename(local_path)
            hdfs_path = psp.join(hdfs_path, local_name)
            if local_name in suffixes:
                if not kwargs.get('overwrite'):
                    raise HdfsError('Remote path %r already exists.', hdfs_path)
            else:
                temp_path = hdfs_path
        if not temp_path:
            # The remote path already exists, we need to generate a temporary one.
            remote_dpath, remote_name = psp.split(hdfs_path)
            temp_dir = temp_dir or remote_dpath
            temp_path = psp.join(
                temp_dir,
                '%s.temp-%s' % (remote_name, int(time.time()))
            )
            _logger.debug(
                'Upload destination %r already exists. Using temporary path %r.',
                hdfs_path, temp_path
            )
        # Then we figure out which files we need to upload, and where.
        if osp.isdir(local_path):
            local_fpaths = [
                osp.join(dpath, fpath)
                for dpath, _, fpaths in os.walk(local_path)
                for fpath in fpaths
            ]
            if not local_fpaths:
                raise HdfsError('No files to upload found inside %r.', local_path)
            offset = len(local_path.rstrip(os.sep)) + len(os.sep)
            fpath_tuples = [
                (fpath, psp.join(temp_path, fpath[offset:].replace(os.sep, '/')))
                for fpath in local_fpaths
            ]
        elif osp.exists(local_path):
            fpath_tuples = [(local_path, temp_path)]
        else:
            raise HdfsError('Local path %r does not exist.', local_path)
        # Finally, we upload all files (optionally, in parallel).
        if n_threads <= 0:
            n_threads = len(fpath_tuples)
        else:
            n_threads = min(n_threads, len(fpath_tuples))
        _logger.debug(
            'Uploading %s files using %s thread(s).', len(fpath_tuples), n_threads
        )
        try:
            if n_threads == 1:
                for path_tuple in fpath_tuples:
                    _upload(path_tuple)
            else:
                _map_async(n_threads, _upload, fpath_tuples)
        except Exception as err:  # pylint: disable=broad-except
            if cleanup:
                _logger.exception('Error while uploading. Attempting cleanup.')
                try:
                    self.delete(temp_path, recursive=True)
                except Exception:
                    _logger.error('Unable to cleanup temporary folder.')
                finally:
                    raise err
            else:
                raise err
        else:
            if temp_path != hdfs_path:
                _logger.debug(
                    'Upload of %r complete. Moving from %r to %r.',
                    local_path, temp_path, hdfs_path
                )
                self.delete(hdfs_path, recursive=True)
                self.rename(temp_path, hdfs_path)
            else:
                _logger.debug(
                    'Upload of %r to %r complete.', local_path, hdfs_path
                )
        return hdfs_path

