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

from MetaDataManager import MetaDataManager
from Pyro.errors     import NamingError

#pyro_default_port     = 7766
data_manager_port      = 7767
meta_data_manager_port = 7768

# rindex of sub in super, -1 if not found
def rfind(sub, super):
    try:
        return super.rindex(sub)
    except:
        return -1

class DataManager(Pyro.core.ObjBase):

    def __init__(self,
                 mdm_host = "localhost", mdm_port = meta_data_manager_port,
                 debug = False, profiling = False):

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
        self.storage_file          = ("/tmp/dfs_" + os.getenv("USER") +
                                      "_at_" + self.hostname)
        self.local_chunks          = {}
        self.pyro_daemon_loop_cond = True
        self.mdm = None
        try:
            self.data_store_lock.acquire() # ACQ
            self.data_store = open(self.storage_file, 'wb')
            os.chmod(self.storage_file,
                     stat.S_IRUSR | stat.S_IWUSR) # <=> chmod 600
            self.data_store_lock.release() # REL
            self.add_local_chunk(0, "STORAGE_INITIALIZED", "DFS_STORAGE_v00\n")
        except:
            logging.exception("can't create or write to: " + self.storage_file)
            sys.exit(0)
        if profiling:
            self.mdm = MetaDataManager()
        else:
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

    def add_local_chunk(self, chunk_number, dfs_path, data):
        chunk_name = self.get_chunk_name(chunk_number, dfs_path)
        self.data_store_lock.acquire() # ACQ
        chunk_desc = (self.data_store.tell(), len(data))
        self.data_store.write(data)
        self.data_store.flush()
        self.data_store_lock.release() # REL
        self.local_chunks[chunk_name] = chunk_desc

    # return (False, None) if busy
    #        (True***,  None) if not busy but chunk was not found
    #                         WARNING: client must update meta data info then?
    #        (True***,  c)    if not busy and chunk found
    # ***IMPORTANT: call got_chunk just after on client side if True was
    #               returned as first in the pair
    def get_chunk(self, chunk_name):
        res = (False, None)
        ready = self.chunk_server_lock.acquire(False) # ACQ
        if ready:
            # we acquired the lock to forbid simultaneous transfers
            # because we don't want the bandwidth to be shared between clients
            lookup = self.local_chunks.get(chunk_name)
            if lookup == None:
                res = (True, None)
            else:
                read_only_data_store = open(self.storage_file, 'rb')
                (chunk_offset, chunk_size) = lookup
                read_only_data_store.seek(chunk_offset)
                data     = read_only_data_store.read(chunk_size)
                if data == None or len(data) != chunk_size:
                    logging.fatal("could not extract " + chunk_name +
                                  " from local store despite it was listed" +
                                  " in self.local_chunks")
                    res = (True, None)
                else:
                    res = (True, data)
        return res

    # notify transfer finished so that other can download chunks from this
    # DataManager
    def got_chunk(self):
        self.chunk_server_lock.release() # REL

    # publish a local file into the DFS
    # the verify parameter controls usage of checksums
    def put(self, filename, dfs_path = None, verify = True,
            remote_mdm = None):
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
                file_size   = 0
                chunk_index = 0
                read_buff   = input_file.read(self.CHUNK_SIZE)
                while read_buff != '':
                    file_size += len(read_buff)
                    if verify:
                        md5_sum = md5.new(read_buff)
                        checksums.append(md5_sum.hexdigest())
                    self.add_local_chunk(chunk_index, dfs_path, read_buff)
                    chunk_index += 1
                    read_buff = input_file.read(self.CHUNK_SIZE)
                if remote_mdm:
                    remote_mdm.publish_meta_data(dfs_path, self.hostname,
                                                 file_size,
                                                 chunk_index, checksums)
                else:
                    self.mdm.publish_meta_data(dfs_path, self.hostname,
                                               file_size, chunk_index,
                                               checksums)
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

    # multiple get
    def mget(self, dfs_directory, local_directory = None, only_peek = False):
        if local_directory == None:
            local_directory = dfs_directory
        # list all files whose name begins with dfs_directory
        wanted_files = []
        for f in self.ls_files():
            if f.startswith(dfs_directory + '/'):
                wanted_files.append(f)
        if len(wanted_files) == 0:
            logging.error("no such directory: " + dfs_directory)
        else:
            # get them
            for f in wanted_files:
                print "f:" + f
                last_slash = rfind('/', f)
                dirname = ""
                if last_slash == -1:
                    dirname = local_directory
                else:
                    dirname = f[0:last_slash]
                    dirname = dirname.replace(dfs_directory,
                                              local_directory, 1)
                basename = f[last_slash+1:]
                if not os.path.isdir(dirname):
                    os.makedirs(dirname)
                self.get(f, dirname + '/' + basename, False, only_peek)

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
                            (idx, dfs_path) = self.decode_chunk_name(c)
                            self.add_local_chunk(idx, dfs_path, data)
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
        for (c, c_sum) in meta_info.get_chunk_name_and_sums():
            if self.local_chunks.get(c) == None:
                remote_chunks.append((c, c_sum))
        return remote_chunks

    # download a DFS file and dump it to a local file
    def get(self, dfs_path, fs_output_path, append_mode = False,
            only_peek = False):
        res = True
        if fs_output_path == None:
            fs_output_path = dfs_path
        meta_info = self.mdm.get_meta_data(dfs_path)
        if meta_info == None:
            logging.error("no such file: " + dfs_path)
        else:
            all_chunks_available = False
            trial_num = 0
            while not all_chunks_available:
                if self.debug: print "trial:" + str(trial_num)
                remote_chunks = self.find_remote_chunks(meta_info)
                if self.debug: print remote_chunks
                # shuffle is here to increase pipelining and parallelization
                # of transfers
                random.shuffle(remote_chunks)
                all_chunks_available = self.download_chunks(remote_chunks,
                                                            only_peek)
                trial_num += 1
            if all_chunks_available:
                # dump all chunks from local store to fs_output_path and
                # in the right order please
                if append_mode:
                    output_file = open(fs_output_path, 'ab')
                else:
                    output_file = open(fs_output_path, 'wb')
                try:
                    read_only_data_store = open(self.storage_file, 'rb')
                    for (c, _) in meta_info.get_chunk_name_and_sums():
                        chunk_read = None
                        (chunk_offset, chunk_size) = self.local_chunks[c]
                        try:
                            read_only_data_store.seek(chunk_offset)
                            chunk_read = read_only_data_store.read(chunk_size)
                        except:
                            logging.exception("")
                        if chunk_read == None or len(chunk_read) != chunk_size:
                            logging.fatal("could not extract " + c +
                                          " from local store")
                        else:
                            output_file.write(chunk_read)
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
        return list(self.local_chunks.keys())

    # describes the local data store file
    def desc_local_chunks(self):
        res = "#name:offset:size\n"
        for k in self.local_chunks.keys():
            (offset, size) = self.local_chunks[k]
            res += k + ':' + str(offset) + ':' + str(size) + '\n'
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
