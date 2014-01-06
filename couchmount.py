from sys import path
path.append('..')

from pydokan import Dokan
from pydokan.struct import DOKAN_OPTION_KEEP_ALIVE, DOKAN_OPTION_REMOVABLE,\
    LPWSTR, WIN32_FIND_DATAW, PWIN32_FIND_DATAW, LPSTR
from pydokan.wrapper.file import AccessMode, ShareMode, CreationDisposition, 
    FlagsAndAttributes
from pydokan.wrapper.dokan import DokanFileInfo
from pydokan.wrapper.security import SecurityInfo
from pydokan.win32con import ERROR_FILE_NOT_FOUND, FILE_ATTRIBUTE_DIRECTORY,\
    ERROR_INVALID_HANDLE, FILE_CASE_SENSITIVE_SEARCH, FILE_UNICODE_ON_DISK,\
    FILE_SUPPORTS_ENCRYPTION, FILE_SUPPORTS_REMOTE_STORAGE, \
    FILE_ATTRIBUTE_NORMAL, ERROR_FILE_EXISTS, CREATE_NEW
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
import ctypes


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

class Couchmount(Dokan, Thread):
        
    def __init__(self, app, mount_point, options, threads_count, version=600):
        '''
        Initialize Couchmount class
        '''
        self.app = app
        Dokan.__init__(self, mount_point, options, threads_count, version)
        Thread.__init__(self)
        self.mount_code = 0
        self.serial_number = 0x19831116
        # Lock for decorator @log. Callback`s loging...
        self.log_lock = Lock()
        self.counter = 1
        self.db = server[DATABASE]
        self.currentFile = b''
    
    def run(self):
        self.mount_code = 0
        self.mount_code = self.main()
    
    def log_exception(self):
        '''
        Log exception
        '''
        lines = traceback.format_exc().splitlines()
        self.log_lock.acquire()
        try:
            log = self.app.log
            for line in lines:
                log.error(line)
        finally:
            self.log_lock.release()
    
    @wrap(None, AccessMode, ShareMode, CreationDisposition, FlagsAndAttributes, DokanFileInfo)
    @log('path', 'access', 'share_mode', 'disposition', 'flags', 'info')
    def create_file(self, path, access, share_mode, disposition, flags, file_info):
        '''
        Create file if disposition flag is CREATE_NEW
           path {string}: file path
           access {AccessMode}: file permissions
           share_mode {ShareMode}: 
           disposition {CreationDisposition}: Flags 
           file_info {Object}: file information
        '''
        (file_path, name) = _path_split(path)
        if disposition == CreationDisposition(CREATE_NEW):
            new_binary = {"docType": "Binary"}
            binary_id = self.db.create(new_binary)
            self.db.put_attachment(self.db[binary_id], '', filename="file")

            rev = self.db[binary_id]["_rev"]

            newFile = {
                "name": name,
                "path": file_path,
                "binary": {
                    "file": {
                        "id": binary_id,
                        "rev": rev
                    }
                },
                "docType": "File"
            }
            self.db.create(newFile)
            file_info.context = self.counter
            self.counter += 1
        return 0          
    disable_debug('create_file')

    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def create_directory(self, path, file_info):
        '''
        Create new directory
            path {string}: directory path
            file_info {Object}: directory information
        '''
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
    @log('path', 'info')
    def open_directory(self, path, file_info):
        '''
        Open directory
            path {string}: directory path
            file_info {Object}: directory information
        '''
        path = _normalize_path_win_to_DB(path)
        res = self.db.view('folder/byFullPath', key=path)
        if len(res) is not 0:
            return 0
        else:
            return -ERROR_FILE_NOT_FOUND
    disable_debug('open_directory')
    
    @wrap(None, None, DokanFileInfo)
    @log('path', 'buf', 'info')
    def get_info(self, path, buffer, file_info):
        '''
        Get file or directory information
            path {string}: directory path
            buffer {buffer}: buffer to store file/directory information
            file_info {Object}: directory information            
        '''
        path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=path)
        if len(res) > 0:  
            buffer[0].dwFileAttributes = FILE_ATTRIBUTE_NORMAL
            win_size = SizeConvert(144).convert()
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
            else:
                return -ERROR_FILE_NOT_FOUND 
        dt_converter = DateTimeConvertor(datetime.today())
        dc = dt_converter.convert()
        buffer[0].ftCreationTime = dc
        buffer[0].ftLastAccessTime = dc
        buffer[0].ftLastWriteTime = dc
        buffer[0].dwVolumeSerialNumber = self.serial_number
        return 0 
    disable_debug('get_info')
    
    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def cleanup(self, path, file_info):
        '''
        Clean file: Cleanup is invoked when the function CloseHandle in Windows 
        API is executed
            path {string}: file/directory path
            file_info {Object}: file/directory information
        '''

        if file_info.delete_on_close:
            return -1
        file_info.context = 0
        return 0
    disable_debug('cleanup')  
    
    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def close(self, path, file_info):
        '''
        Close file
            path {string}: file/directory path
            file_info {Object}: file/directory information
        '''
        if file_info.context:
            print("ERROR: File not cleanupped?")
            file_info.context = 0
            return 0
        else:
            return 0
    disable_debug('close')
    
    @wrap(None, None, None, None, None, None, None, DokanFileInfo)
    @log('name', 'name_size', 'serial', 'max_component_len', \
         'fs_flags', 'fs_name', 'fs_name_size', 'info')
    def get_volume_info(self, name_buff, name_size, sn, max_comonent_len, \
                        fs_flags, fs_name_buff, fs_name_size, file_info):
        '''
        TODOS
        '''
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
    @log('avail', 'total', 'free', 'info')
    def get_free_space(self, free_bytes, total_bytes, total_free_bytes, file_info):
        '''
        TODOS
        '''
        free_bytes[0] = 1048576 - len(self.files['\\hello_world.txt'])
        total_bytes[0] = 1048576
        total_free_bytes[0] = 1048576
        return 0
    disable_debug('get_free_space')
    
    @wrap(None, None, None, DokanFileInfo)
    @log('path', 'pattern', 'func', 'info')
    def find_files_with_pattern(self, path, pattern, func, file_info):
        '''
        Find files with a specific pattern        
            path {string}: file/directory path
            pattern {string}: parttern to find
            func {function}: function to call with PWIN32_FIND_DATAW
            file_info {Object}: file/directory information
        '''
        found = False
        file_info_raw = file_info.raw()
        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFolder', key=new_path)
        for i in res:
            i = i.value
            if self.name_in_expression(pattern, i['name'], False):
                found = True
                dc = DateTimeConvertor(datetime.today()).convert()
                win_size = SizeConvert(1).convert()
                info = WIN32_FIND_DATAW(
                    FILE_ATTRIBUTE_NORMAL, dc, dc, dc, win_size[0],
                    win_size[1], 0, 0, i['name'], ''
                )
                func(PWIN32_FIND_DATAW(info), file_info_raw)
                return 0
        if not found:
            res = self.db.view('folder/byFolder', key=new_path)
            for i in res:
                i = i.value
                if self.name_in_expression(pattern, i['name'], False):
                    found = True
                    dc = DateTimeConvertor(datetime.today()).convert()
                    win_size = SizeConvert(1).convert()
                    info = WIN32_FIND_DATAW(
                        FILE_ATTRIBUTE_DIRECTORY, dc, dc, dc, win_size[0],
                        win_size[1], 0, 0, i['name'], ''
                    )
                    func(PWIN32_FIND_DATAW(info), file_info_raw)
                    return 0
            if not found:
                return -ERROR_INVALID_HANDLE
    disable_debug('find_files_with_pattern')
    
    @wrap(None, None, None, None, None, DokanFileInfo)
    @log('path', 'buf', 'length', 'length', 'offset', 'info')
    def read(self, path, buffer, length, buff_length, offset, file_info):
        '''
        Read file
            path {string}: file/directory path
            buffer {buffer}: buffer to store file content 
            length {integer}: max length to read
            buff_length{LP_u_long}: pointer to buffer length
            offser {integer}: file reading offset
            file_info {Object}: file/directory information
        '''
        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=new_path)
        if len(res) > 0:
            for file_info in res:
                file_info = file_info.value
                try:
                    bin_id = file_info['binary']['file']['id']
                except Exception:
                    return -ERROR_INVALID_HANDLE
                commande = ['curl', '-H', 'Content-Type: text/html',
                            'http://localhost:5984/cozy-files/%s/file' %bin_id]
                process = subprocess.Popen(commande, 
                                           shell = True, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE)
                content, err = process.communicate()
                try:
                    json.loads(content)
                    if content['error']:
                        content = ""
                except Exception:
                    content_length = len(content)
                    if offset < content_length:
                        if offset + length > content_length:
                            length = content_length - offset
                        buff = ctypes.create_string_buffer(content[offset:offset+length])
                    else:
                        buff = ''
                    memmove(buffer, content[offset:offset+length], length)
                    buff_length[0] = length
                    return 0
        else:
            return -ERROR_INVALID_HANDLE        
    disable_debug('read')
    
    @wrap(None, None, None , DokanFileInfo)
    @log('path', 'path', 'bool', 'info')
    def move(self, src, dst, replace, file_info):
        '''
        Move file or directory
            src {string}: source file/directory path
            dst {string}: destination file/directory path
            replace {boolean}:
            file_info {Object}: file/directory information
        '''
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
        '''
        Delete file 
            path {string}: file path
            file_info {Object}: file information
        '''
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
        '''
        Delete directory:

            path {string}: directory path
            file_info {Object}: directory information
        '''
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
        '''
        Write content in file         
            path {string}: file path
            buffer {buffer}: content to write
            length {integer}: max content length
            written_length {LP_u_long}: content length to write
            offset {integer}: file offset
            file_info {Object}: file information
        '''
        new_path = _normalize_path_win_to_DB(path)
        res = self.db.view('file/byFullPath', key=new_path)
        if len(res) > 0:    
            buffer = string_at(buffer, length)
            self.currentFile = self.currentFile[:offset] + buffer + self.currentFile[offset+length:]
            if writen_length[0] < length:
                for doc in res:
                    doc = doc.value
                    binary_id = doc["binary"]["file"]["id"]
                    self.db.put_attachment(self.db[binary_id],
                                           self.currentFile,
                                           filename = "file")
                    binary = self.db[binary_id]
                    doc['binary']['file']['rev'] = binary['_rev']
                    self.db.save(doc)
                    self.currentFile = b''
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
    app.hwfs = Couchmount(app, 'H', DOKAN_OPTION_KEEP_ALIVE | DOKAN_OPTION_REMOVABLE, 5)
    app.hwfs.start()

if __name__ == '__main__':
    main()