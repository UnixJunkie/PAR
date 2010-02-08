#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Copyright (C) 2009, 2010, Zhang Initiative Research Unit,
Advance Science Institute, Riken
2-1 Hirosawa, Wako, Saitama 351-0198, Japan
If you use and like our software, please send us a postcard! ^^
---
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
---
"""

import logging, md5, os, random, socket, sys, stat, thread, time

import Pyro.core, Pyro.naming

from tarfile         import TarFile
from tempfile        import TemporaryFile

from MetaDataManager import MetaDataManager
from Pyro.errors     import NamingError

# FBR: * mget

#pyro_default_port     = 7766
data_manager_port      = 7767
meta_data_manager_port = 7768

class DataManager(Pyro.core.ObjBase):

    def __init__(self,
                 mdm_host = "localhost", mdm_port = meta_data_manager_port,
                 debug = False):

        Pyro.core.ObjBase.__init__(self)
        self.debug = debug
        # when compressing, we must compress before cuting into chunks so we
        # will have fewer chunks to transfer instead of having smaller ones
        # (better for network latency I think)
        ##################################
        # SYNCHRONIZE ACCES TO DATASTORE #
        ##################################
        self.data_store_lock       = thread.allocate_lock()
        self.chunk_server_lock     = thread.allocate_lock()
        self.CHUNK_SIZE            = 1024*1024
        self.data_store            = None
        self.hostname              = socket.getfqdn()
        self.storage_file          = ("/tmp/dfs_" + os.getlogin() +
                                      "_at_" + self.hostname)
        self.local_chunks          = {}
        self.pyro_daemon_loop_cond = True
        self.mdm = None
        try:
            # The TarFile interface forces us to use a temporary file.
            # Maybe it makes some unnecessary data copy, however having
            # our chunks kept in the standard tar format is too cool
            # to be changed
            self.data_store_lock.acquire()
            if self.debug: print "self.data_store_lock ACK"
            self.data_store = TarFile(self.storage_file, 'w')
            os.chmod(self.data_store.fileobj.name,
                     stat.S_IRUSR | stat.S_IWUSR) # <=> chmod 600 ...
            self.data_store_lock.release()
            if self.debug: print "self.data_store_lock REL"
            temp_file = TemporaryFile()
            temp_file.write("DFS_STORAGE_v00\n")
            temp_file.flush()
            temp_file.seek(0)
            self.add_local_chunk(0, "STORAGE_INITIALIZED", temp_file)
            temp_file.close()
        except:
            logging.exception("can't create or write to: " + self.storage_file)
            sys.exit(0)
        self.use_remote_mdm(mdm_host, mdm_port)

    # change MetaDataManager
    def use_remote_mdm(self, host, port = meta_data_manager_port):
        mdm_URI = "PYROLOC://" + host + ":" + str(port) + "/meta_data_manager"
        self.mdm = Pyro.core.getProxyForURI(mdm_URI)
        try:
            is_remote_up = self.mdm.started()
        except Pyro.errors.ConnectionClosedError:
            is_remote_up = False
        return is_remote_up

    # change MetaDataManager to default one
    def use_local_mdm(self):
        self.use_remote_mdm("localhost", meta_data_manager_port)

    def get_chunk_name(self, chunk_number, dfs_path):
        if dfs_path.startswith('/'):
            dfs_path = dfs_path[1:]
        return str(chunk_number) + '/' + dfs_path

    def chunk_name_to_index(self, chunk_name):
        end = chunk_name.index('/')
        return int(chunk_name[0:end])

    def chunk_name_to_dfs_path(self, chunk_name):
        begin = chunk_name.index('/')
        return chunk_name[begin+1:]

    def decode_chunk_name(self, chunk_name):
        middle = chunk_name.index('/')
        return (int(chunk_name[0:middle]), chunk_name[middle+1:])

    def add_local_chunk(self, chunk_number, dfs_path, tmp_file):
        chunk_name = self.get_chunk_name(chunk_number, dfs_path)
        self.data_store_lock.acquire()
        if self.debug: print "self.data_store_lock ACK"
        tar_info = self.data_store.gettarinfo (arcname = chunk_name,
                                               fileobj = tmp_file)
        self.data_store.addfile(tar_info, tmp_file)
        self.data_store.fileobj.flush()
        self.local_chunks[chunk_name] = True
        self.data_store_lock.release()
        if self.debug: print "self.data_store_lock REL"

    # return (False, None) if busy
    #        (True***,  None) if not busy but chunk was not found
    #                         WARNING: client must update meta data info then?
    #        (True***,  c)    if not busy and chunk found
    # ***IMPORTANT: call got_chunk just after on client side if True was
    #               returned as first in the pair
    # FBR: all this stuff is only useful if the Pyro daemon running it
    #      is multithread... If not, then calls are blocking until chunk
    #      download is finished, exactly what we want to avoid :(
    def get_chunk(self, chunk_name):
        res = (False, None)
        ready = self.chunk_server_lock.acquire(False)
        if ready:
            # we acquired the lock to forbid simultaneous transfers
            # because we don't want the bandwidth to be shared between clients
            if self.local_chunks.get(chunk_name) == None:
                res = (True, None)
            else:
                self.data_store_lock.acquire()
                if self.debug: print "self.data_store_lock ACK"
                read_only_data_store = TarFile(self.storage_file, 'r')
                untared_file = read_only_data_store.extractfile(chunk_name)
                self.data_store_lock.release()
                if self.debug: print "self.data_store_lock REL"
                if untared_file == None:
                    logging.fatal("could not extract " + chunk_name +
                                  " from local store despite it was listed" +
                                  " in self.local_chunks")
                    res = (True, None)
                else:
                    chunk = untared_file.read()
                    res = (True, chunk)
        return res

    # notify transfer finished so that other can download chunks from this
    # DataManager
    def got_chunk(self):
        self.chunk_server_lock.release()

    # publish a local file into the DFS
    # the verify parameter controls usage of checksums
    def put(self, filename, dfs_path = None, verify = True):
        if not os.path.isfile(filename):
            logging.error("no such file: " + filename)
        else:
            if dfs_path == None:
                dfs_path = filename
            # compression of added file hook should be here
            checksums = None
            if verify:
                checksums = []
            input_file = open(filename, 'rb')
            try:
                file_size = 0
                chunk_index = 0
                read_buff = input_file.read(self.CHUNK_SIZE)
                while read_buff != '':
                    file_size += len(read_buff)
                    temp_file = TemporaryFile()
                    temp_file.write(read_buff)
                    temp_file.flush()
                    temp_file.seek(0)
                    if verify:
                        md5_sum = md5.new(read_buff)
                        checksums.append(md5_sum.hexdigest())
                    self.add_local_chunk(chunk_index, dfs_path, temp_file)
                    temp_file.close()
                    chunk_index += 1
                    read_buff = input_file.read(self.CHUNK_SIZE)
                self.mdm.publish_meta_data(dfs_path, self.hostname,
                                           file_size, chunk_index, checksums)
            except:
                logging.exception("problem while reading " + filename)
            input_file.close()

    # multiple put (recursive put, for directories)
    def mput(self, directory, dfs_path = None, verify = True):
        if not os.path.isdir(directory):
            logging.error("no such directory: " + directory)
        else:
            if dfs_path == None:
                dfs_path = directory
            for root, dirs, files in os.walk(directory):
                for f in files:
                    self.put(os.path.join(root, f),
                             os.path.join(root.replace(directory, dfs_path, 1),
                                          f),
                             verify)

    def download_chunks(self, chunk_and_sums, only_peek = False):
        res = True
        while len(chunk_and_sums) > 0:
            (c, c_sum) = chunk_and_sums.pop(0)
            c_sources = self.mdm.resolve(c)
            # shuffle is here to increase pipelining and parallelization
            # of transfers
            random.shuffle(c_sources)
            downloaded = False
            for source in c_sources:
                remote_dm_URI = ("PYROLOC://" + source + ":" +
                                 str(data_manager_port) + "/data_manager")
                remote_dm = Pyro.core.getProxyForURI(remote_dm_URI)
                try:
                    (request_was_processed, data) = remote_dm.get_chunk(c)
                    if request_was_processed:
                        remote_dm.got_chunk() # unlock the server
                    else:
                        logging.debug("busy source: " + source)
                    if data:
                        # store chunk locally
                        verif = None
                        if c_sum:
                            verif = md5.new(data).hexdigest()
                        if verif == c_sum:
                            downloaded = True
                            temp_file = TemporaryFile()
                            temp_file.write(data)
                            temp_file.flush()
                            temp_file.seek(0)
                            (idx, dfs_path) = self.decode_chunk_name(c)
                            self.add_local_chunk(idx, dfs_path, temp_file)
                            temp_file.close()
                            if not only_peek:
                                self.mdm.update_add_node(dfs_path, idx,
                                                         self.hostname)
                            break
                        else:
                            logging.error("md5 differ for chunk: "
                                          + c + " from: " + source +
                                          " must be: " + c_sum +
                                          " but is: " + verif)
                    else:
                        logging.error("chunk: " + c + " not there: " + source)
                except:
                    logging.exception("problem with " + remote_dm_URI)
            if not downloaded:
                logging.debug("could not download: " + c)
                res = False
        return res

    def find_remote_chunks(self, meta_info):
        remote_chunks = []
        self.data_store_lock.acquire()
        if self.debug: print "self.data_store_lock ACK"
        for (c, c_sum) in meta_info.get_chunk_name_and_sums():
            if self.local_chunks.get(c) == None:
                remote_chunks.append((c, c_sum))
        self.data_store_lock.release()
        if self.debug: print "self.data_store_lock REL"
        return remote_chunks

    # download a DFS file and dump it to a local file
    def get(self, dfs_path, fs_output_path, append_mode = False,
            nb_trials = 3, only_peek = False):
        res = True
        if fs_output_path == None:
            fs_output_path = dfs_path
        meta_info = self.mdm.get_meta_data(dfs_path)
        if meta_info == None:
            logging.error("no such file: " + dfs_path)
        else:
            all_chunks_available = False
            for trial in range(nb_trials): # try hard to get it
                if self.debug: print "trial:" + str(trial)
                remote_chunks = self.find_remote_chunks(meta_info)
                if self.debug: print remote_chunks
                # shuffle is here to increase pipelining and parallelization
                # of transfers
                random.shuffle(remote_chunks)
                all_chunks_available = self.download_chunks(remote_chunks,
                                                            only_peek)
                if self.debug: print all_chunks_available
                if all_chunks_available:
                    break
            if all_chunks_available:
                # dump all chunks from local store to fs_output_path and
                # in the right order please
                if append_mode:
                    output_file = open(fs_output_path, 'ab')
                else:
                    output_file = open(fs_output_path, 'wb')
                try:
                    self.data_store_lock.acquire()
                    if self.debug: print "self.data_store_lock ACK"
                    read_only_data_store = TarFile(self.storage_file, 'r')
                    self.data_store_lock.release()
                    if self.debug: print "self.data_store_lock REL"
                    for (c, _) in meta_info.get_chunk_name_and_sums():
                        self.data_store_lock.acquire()
                        if self.debug: print "self.data_store_lock ACK"
                        untared_file = read_only_data_store.extractfile(c)
                        self.data_store_lock.release()
                        if self.debug: print "self.data_store_lock REL"
                        if untared_file == None:
                            logging.fatal("could not extract " + c +
                                          " from local store")
                        else:
                            output_file.write(untared_file.read())
                except:
                    logging.exception("problem while writing " +
                                      dfs_path + " to " + fs_output_path)
                read_only_data_store.close()
                output_file.close()
            else:
                res = False
                logging.error("could not get complete file: " + dfs_path)
        return res

    def ls_files(self):
        return self.mdm.ls_files()

    def ls_nodes(self):
        return self.mdm.ls_nodes()

    def ls_local_chunks(self):
        res = []
        self.data_store_lock.acquire()
        if self.debug: print "self.data_store_lock ACK"
        res = list(self.local_chunks.keys())
        self.data_store_lock.release()
        if self.debug: print "self.data_store_lock REL"
        return res

    def ls_all_chunks(self):
        return self.mdm.ls_chunks()

    def ls_all_chunk_and_sums(self):
        return self.mdm.ls_chunk_and_sums()

    def started(self):
        return True

    def stop(self):
        self.pyro_daemon_loop_cond = False

    def stop_local_mdm(self):
        self.use_local_mdm()
        self.mdm.stop()
