# coding=utf-8
# Hello World File System.

__author__  = 'Frolov Evgeniy (profisphantom@gmail.com)'
__license__ = 'GNU GPL'
__version__ = '0.1'


from sys import path
path.append('..')

from pydokan import Dokan
from pydokan.struct import DOKAN_OPTION_KEEP_ALIVE, DOKAN_OPTION_REMOVABLE,\
    LPWSTR, WIN32_FIND_DATAW, PWIN32_FIND_DATAW, LPSTR
from pydokan.wrapper.file import AccessMode, ShareMode, CreationDisposition, FlagsAndAttributes
from pydokan.wrapper.dokan import DokanFileInfo
from pydokan.wrapper.security import SecurityInfo
from pydokan.win32con import ERROR_FILE_NOT_FOUND, FILE_ATTRIBUTE_DIRECTORY,\
    ERROR_INVALID_HANDLE, FILE_CASE_SENSITIVE_SEARCH, FILE_UNICODE_ON_DISK,\
    FILE_SUPPORTS_ENCRYPTION, FILE_SUPPORTS_REMOTE_STORAGE, FILE_ATTRIBUTE_NORMAL,\
    ERROR_FILE_EXISTS
from pydokan.utils import wrap, log, DateTimeConvertor, SizeConvert
from pydokan.debug import disable as disable_debug, force_breakpoint

from datetime import datetime
from threading import Thread, Lock
import traceback, logging, logging.handlers
from ctypes import memmove, string_at
from couchdb import Server

import subprocess
import os
import json


DATABASE = "cozy-files"
server = Server("http://localhost:5984/")

def _normalize_path_win_to_DB(path):
    if path is '\\':
        return ''
    else:
        return path.replace('\\', '/')

def _normalize_path_DB_to_win(path):
    return path.replace('/', '\\')

def _path_split(path):
    '''
    '''
    path = path.replace('\\', '/')
    (folder_path, name) = os.path.split(path)
    if folder_path[-1:] == '/':
        folder_path = folder_path[:-(len(name)+1)]
    return (folder_path, name)

class HelloWorldFS(Dokan, Thread):
    
    files = {'\\hello_world.txt': b'Hello World',
             '\\ReadMe.txt': b'To unmount the disk, enter the path H:\\exit'}
    
    def __init__(self, app, mount_point, options, threads_count, version=600):
        self.app = app
        Dokan.__init__(self, mount_point, options, threads_count, version)
        Thread.__init__(self)
        self.mount_code = 0
        self.serial_number = 0x19831116
        # Lock for decorator @log. Callback`s loging...
        self.log_lock = Lock()
        self.counter = 1
        self.db = server[DATABASE]
    
    def run(self):
        self.mount_code = 0
        self.mount_code = self.main()
    
    def log_exception(self):
        lines = traceback.format_exc().splitlines()
        self.log_lock.acquire()
        try:
            log = self.app.log
            for line in lines:
                log.error(line)
        finally:
            self.log_lock.release()
    
    @wrap(None, AccessMode, ShareMode, CreationDisposition, FlagsAndAttributes, DokanFileInfo)
    #@log('path', 'access', 'share_mode', 'disposition', 'flags', 'info')
    def create_file(self, path, access, share_mode, disposition, flags, file_info):
        #print(file_info)
        #print('create_file')
        #print(path)
        (file_path, name) = _path_split(path)
        #print(file_path)
        #print(name)
        new_binary = {"docType": "Binary"}
        #binary_id = self.db.create(new_binary)
        #print(binary_id)
        #self.db.put_attachment(self.db[binary_id], '', filename="file")
        #print(self.db[binary_id])

        #rev = self.db[binary_id]["_rev"]
        #print(rev)
        #newFile = {
        #    "name": name,
        #    "path": file_path,
        #    "binary": {
        #        "file": {
        #            "id": binary_id,
        #            "rev": rev
        #        }
        #    },
        #    "docType": "File"
        #}
        #print(newFile)
        #self.db.create(newFile)
        #file_info.context = self.counter
        #"self.counter += 1
        return 0          
    disable_debug('create_file')

    @wrap(None, DokanFileInfo)
    #@log('path', 'info')
    def create_directory(self, path, file_info):
        # Unmount disk if enter h:\exit path
        print(path)
        print('create_directory')
        (folder_path, name) = _path_split(path)
        res = self.db.view('folder/byFullPath', key=folder_path+'/'+name)
        if len(res) > 0 :
            return -ERROR_FILE_EXISTS
        else:
            res = self.db.view('file/byFullPath', key=folder_path+'/'+name)
            if len(res) > 0 :
                return -ERROR_FILE_EXISTS
            else:
                self.db.create({
                    "name": name,
                    "path": folder_path,
                    "docType": "Folder"
                    })
                return 0           
    disable_debug('create_directory')
    
    @wrap(None, DokanFileInfo)
    #@log('path', 'info')
    def open_directory(self, path, file_info):
        print("open_directory")
        print(path)
        path = _normalize_path_win_to_DB(path)
        res = self.db.view('folder/byFullPath', key=path)
        print(len(res))
        if len(res) is not 0:
            return 0
        else:
            return -ERROR_FILE_NOT_FOUND
    disable_debug('open_directory')
    
    @wrap(None, None, DokanFileInfo)
    #@log('path', 'buf', 'info')
    def get_info(self, path, buffer, file_info):
        print('get_info')
        #print(path)
        path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=path)
        if len(res) > 0:                
            buffer[0].dwFileAttributes = FILE_ATTRIBUTE_NORMAL
            win_size = SizeConvert(1).convert()
            buffer[0].nFileSizeHigh = win_size[0]
            buffer[0].nFileSizeLow = win_size[1]
            buffer[0].nNumberOfLinks = 1
            win_index = SizeConvert(2).convert()
            buffer[0].nFileIndexHigh = win_index[0]
            buffer[0].nFileIndexLow = win_index[1]
        else:
            res = self.db.view('folder/byFullPath', key=path)
            if len(res) > 0:
                buffer[0].dwFileAttributes = FILE_ATTRIBUTE_DIRECTORY
                buffer[0].nFileSizeHigh = 0
                buffer[0].nFileSizeLow = 0
                buffer[0].nNumberOfLinks = 1
                win_index = SizeConvert(1).convert()
                buffer[0].nFileIndexHigh = win_index[0]
                buffer[0].nFileIndexLow = win_index[1] 
                print("ok")
            else:
                return -ERROR_FILE_NOT_FOUND 
        dt_converter = DateTimeConvertor(datetime.today())
        dc = dt_converter.convert()
        buffer[0].ftCreationTime = dc
        buffer[0].ftLastAccessTime = dc
        buffer[0].ftLastWriteTime = dc
        buffer[0].dwVolumeSerialNumber = self.serial_number
        #print(buffer[0])
        return 0 
    disable_debug('get_info')
    
    @wrap(None, DokanFileInfo)
    #@log('path', 'info')
    def cleanup(self, path, file_info):
        print("cleanup")
        #print(file_info)
        if file_info.delete_on_close:
            return -1
        file_info.context = 0
        return 0
    disable_debug('cleanup')  
    
    @wrap(None, DokanFileInfo)
    #@log('path', 'info')
    def close(self, path, file_info):
        if file_info.context:
            print("ERROR: File not cleanupped?")
            file_info.context = 0
            return 0
        else:
            return 0
    disable_debug('close')
    
    @wrap(None, None, None, None, None, None, None, DokanFileInfo)
    #@log('name', 'name_size', 'serial', 'max_component_len', \
    #     'fs_flags', 'fs_name', 'fs_name_size', 'info')
    def get_volume_info(self, name_buff, name_size, sn, max_comonent_len, \
                        fs_flags, fs_name_buff, fs_name_size, file_info):
        name = 'Hello World Device'
        fname = 'HelloWorldFS'
        memmove(name_buff, LPWSTR(name), (len(name) + 1) * 2)
        memmove(fs_name_buff, LPWSTR(fname), (len(fname) + 1) * 2)
        sn[0] = self.serial_number
        max_comonent_len[0] = 255
        
        flags = FILE_SUPPORTS_ENCRYPTION | FILE_UNICODE_ON_DISK | \
            FILE_SUPPORTS_REMOTE_STORAGE | FILE_CASE_SENSITIVE_SEARCH
        
        fs_flags[0] = flags
        return 0
    disable_debug('get_volume_info')
    
    @wrap(None, None, None, DokanFileInfo)
    #@log('avail', 'total', 'free', 'info')
    def get_free_space(self, free_bytes, total_bytes, total_free_bytes, file_info):
        free_bytes[0] = 1048576 - len(self.files['\\hello_world.txt'])
        total_bytes[0] = 1048576
        total_free_bytes[0] = 1048576
        return 0
    #disable_debug('get_free_space')
    
    @wrap(None, None, None, DokanFileInfo)
    #@log('path', 'pattern', 'func', 'info')
    def find_files_with_pattern(self, path, pattern, func, file_info):
        print('find_files_with_pattern')
        file_info_raw = file_info.raw()
        print(path)
        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFolder', key=new_path)
        for i in res:
            i = i.value
            if self.name_in_expression(pattern, i['name'], False):
                dc = DateTimeConvertor(datetime.today()).convert()
                #if self.name_in_expression(pattern, i['name'], False):
                win_size = SizeConvert(1).convert()
                info = WIN32_FIND_DATAW(
                    FILE_ATTRIBUTE_NORMAL, dc, dc, dc, win_size[0],
                    win_size[1], 0, 0, i['name'], ''
                )
                func(PWIN32_FIND_DATAW(info), file_info_raw)
                print(i)

        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('folder/byFolder', key=new_path)
        for i in res:
            i = i.value
            print(pattern)
            print(i['name'])
            print(self.name_in_expression(pattern, i['name'], False))
            if self.name_in_expression(pattern, i['name'], False):
                print(i)
                dc = DateTimeConvertor(datetime.today()).convert()
                #if self.name_in_expression(pattern, i['name'], False):
                #folder_path = i['path'].replace('/','\\')
                win_size = SizeConvert(1).convert()
                info = WIN32_FIND_DATAW(
                    FILE_ATTRIBUTE_DIRECTORY, dc, dc, dc, win_size[0],
                    win_size[1], 0, 0, i['name'], ''
                )
                print('FILE_INFO_RAW')
                print(file_info)
                func(PWIN32_FIND_DATAW(info), file_info_raw)
        return 0
        #return -ERROR_INVALID_HANDLE
    #disable_debug('find_files_with_pattern')
    
    @wrap(None, None, None, None, None, DokanFileInfo)
    @log('path', 'buf', 'length', 'length', 'offset', 'info')
    def read(self, path, buffer, length, buff_length, offset, file_info):
        #if file_info.context:
        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=new_path)
        if len(res) > 0:
            for file_info in res:
                file_info = file_info.value
                #print(file_info)
                try:
                    bin_id = file_info['binary']['file']['id']
                except e:
                    return -ERROR_INVALID_HANDLE
                commande = ['curl', '-H', 'Content-Type: text/html','http://localhost:5984/cozy-files/%s/file' %bin_id]
                process = subprocess.Popen(commande, shell = True, stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE)
                content, err = process.communicate()
                #print(content)
                #print(err)
                #print(stdout)
                #binary_attachment = self.db.get_attachment(bin_id, "file")
                #if binary_attachment is None:
                #if '_attachments' not in sdout:
                #    return ''
                #else:
                #content = binary_attachment.read()
                content = content.read()
                content_length = len(content)
                print(content_length)
                if offset < content_length:
                    print('if')
                    if offset + length > content_length:
                        pritn('if')
                        length = content_length - offset
                        print(length)
                    buff = content[offset:offset+length]
                else:
                    print('else')
                    buff = ''
                #print(buff)
                memmove(buffer, LPSTR(buff), len(buff))
                buff_length[0] = len(buff)
                print(buff_length[0])
                return 0
        else:
            return -ERROR_INVALID_HANDLE        
    disable_debug('read')
    
    @wrap(None, None, None , DokanFileInfo)
    @log('path', 'path', 'bool', 'info')
    def move(self, src, dst, replace, file_info):
        def move_file(src, dst, doc):
            pathfrom = _normalize_path_win_to_DB(src)
            (pathto, nameto) = _path_split(dst)
            doc.update({"name":nameto, "path": pathto})
            self.db.save(doc) 

        def move_folder(src, dst, doc):
            pathfrom = _normalize_path_win_to_DB(src)
            (pathto, nameto) = _path_split(dst)
            doc.update({"name":nameto, "path": pathto})
            self.db.save(doc) 
            # Rename all subfiles
            for res in self.db.view('file/byFolder', key=pathfrom):
                filepathfrom = res.value['path'] + '/' + res.value['name']
                filepathto = pathto + '/' + nameto + '/' + res.value['name']
                move_file(filepathfrom, filepathto, res.value) 
            # Rename all subfolders
            for res in self.db.view('folder/byFolder', key=pathfrom):
                folderpathfrom = res.value['path'] + '/' + res.value['name']
                folderpathto = pathto + '/' + nameto + '/' + res.value['name']
                move_folder(folderpathfrom, folderpathto, res.value)
            return 0

        pathfrom = _normalize_path_win_to_DB(src)
        res = self.db.view('file/byFullPath', key=pathfrom)
        if len(res) > 0:
            for doc in res:
                doc = doc.value
                move_file(src, dst, doc)
        else:
            res = self.db.view('folder/byFullPath', key=pathfrom)
            if len(res) > 0:
                for doc in res:
                    doc = doc.value
                    move_folder(src, dst, doc)
            else:
                return -ERROR_INVALID_HANDLE
        return 0
    disable_debug('move')

    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def delete_file(self, path, file_info):
        print('DELLLETTTE  FILLE')
        path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=path)
        if len(res) > 0:
            for doc in res:
                doc = doc.value
                binary = self.db[doc['binary']['file']['id']]
                self.db.delete(binary)
                self.db.delete(self.db[doc['_id']])
            return 0
        else:
            return -ERROR_INVALID_HANDLE
    disable_debug('delete_file')

    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def delete_directory(self, path, file_info):
        def delete_folder(folder):        
            self.db.delete(self.db[folder['_id']])
            # Remove all subfiles
            res = self.db.view('file/byFolder', key=path)
            for sub_file in res:
                delete_file(sub_file.value)
            # Remove all subfolders
            res = self.db.view('folder/byFolder', key=path)
            for sub_folder in res:
                delete_folder(sub_folder.value)
            return 0

        def delete_file(doc):
            binary = self.db[doc['binary']['file']['id']]
            self.db.delete(binary)
            self.db.delete(self.db[doc['_id']])

        path = _normalize_path_win_to_DB(path)
        res = self.db.view('folder/byFullPath', key=path)
        if len(res) > 0:
            for folder in res:
                folder = folder.value
                delete_folder(folder)
            return 0
        else:
            return -ERROR_INVALID_HANDLE
    disable_debug('delete_directory')

    @wrap(None, None, None, None, None, DokanFileInfo)
    @log('path', 'buf', 'length', 'written', 'offset', 'info')
    def write(self, path, buffer, length, writen_length, offset, file_info):
        if file_info.context:
            buffer = string_at(buffer, length)
            self.files[path] = self.files[path][:offset] + buffer + self.files[path][offset+length:]
            return 0
        else:
            return -ERROR_INVALID_HANDLE
            
    disable_debug('write')


class App():
    
    def __init__(self):
        self.log = self.get_logger()
    
    def get_logger(self):
        logger = logging.getLogger('vdisk')
        
        level = logging.DEBUG
        logger.setLevel(level)
        
        format = '[%(asctime)s] [%(thread)d] %(levelname)s: %(message)s'
        formatter = logging.Formatter(format)
        
        log_path = './logs/vdisk.log'
        Handler = logging.handlers.TimedRotatingFileHandler
        handler = Handler(log_path, when='D', interval=1, backupCount=5)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        return logger


def main():
    app = App()
    app.hwfs = HelloWorldFS(app, 'H', DOKAN_OPTION_KEEP_ALIVE | DOKAN_OPTION_REMOVABLE, 5)
    app.hwfs.start()

if __name__ == '__main__':
    main()