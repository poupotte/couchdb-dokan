from sys import path
path.append('..')

from pydokan import Dokan
from pydokan.struct import DOKAN_OPTION_KEEP_ALIVE, DOKAN_OPTION_REMOVABLE,\
    LPWSTR, WIN32_FIND_DATAW, PWIN32_FIND_DATAW, LPSTR
from pydokan.wrapper.file import AccessMode, ShareMode, CreationDisposition, \
    FlagsAndAttributes
from pydokan.wrapper.dokan import DokanFileInfo
from pydokan.wrapper.security import SecurityInfo
from pydokan.win32con import ERROR_FILE_NOT_FOUND, FILE_ATTRIBUTE_DIRECTORY,\
    ERROR_INVALID_HANDLE, FILE_CASE_SENSITIVE_SEARCH, FILE_UNICODE_ON_DISK,\
    FILE_SUPPORTS_ENCRYPTION, FILE_SUPPORTS_REMOTE_STORAGE, \
    FILE_ATTRIBUTE_NORMAL, ERROR_FILE_EXISTS, CREATE_NEW, ERROR_ALREADY_EXISTS,\
    CREATE_ALWAYS, OPEN_ALWAYS, FILE_ATTRIBUTE_TEMPORARY  
from pydokan.utils import wrap, log, DateTimeConvertor, SizeConvert
from pydokan.debug import disable as disable_debug, force_breakpoint

from datetime import datetime
from threading import Thread, Lock
import traceback, logging, logging.handlers
from ctypes import memmove, string_at
from couchdb import Server
from replication import replicate_from_local_ids

import subprocess
import os
import json
import ctypes


DATABASE = "cozy-files"
server = Server("http://localhost:5984/")

def _normalize_path_win_to_DB_lower(path):
    if path is '\\':
        return ''
    else:
        return path.replace('\\', '/').lower()

def _normalize_path_DB_to_win_lower(path):
    return path.replace('/', '\\').lower()

def _path_split_lower(path):
    '''
    '''
    path = path.replace('\\', '/')
    (folder_path, name) = os.path.split(path)
    if folder_path[-1:] == '/':
        folder_path = folder_path[:-(len(name)+1)]
    return (folder_path.lower(), name.lower())

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
        # Lock for decorator #@log. Callback`s loging...
        self.log_lock = Lock()
        self.counter = 1
        self.db = server[DATABASE]
        self.currentFile = b''
        self.read_current_file = {}
    
    
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

        def file_creation():
            (file_path, name) = _path_split(path)
            fileName, ext = os.path.splitext(name)

            today = datetime.today()

            new_binary = {"docType": "Binary"}
            binary_id = self.db.create(new_binary)
            self.db.put_attachment(self.db[binary_id], '', filename="file")
            rev = self.db[binary_id]["_rev"]
            if ext == ".txt" or ext == ".doc" or ext == ".pdf" or \
                ext == ".ppt" or ext == ".odt" or ext == ".rtf":
                file_class = "document"
            elif ext == ".png" or ext == ".jpeg" or ext == ".jpg":
                file_class = "image"
            elif ext == ".mp3" or ext == ".wav":
                file_class = "music"
            elif ext == ".mp4" or ext == ".avi":
                file_class = "video"
            else:
                file_class = "file"
            newFile = {
                "name": name,
                "path": file_path,
                "binary": {
                    "file": {
                        "id": binary_id,
                        "rev": rev
                    }
                },
                "size": 0,
                "class": file_class,
                "creationDate": today.strftime('%a %b %d %Y %H:%M:%S'),
                "lastModification": today.strftime('%a %b %d %Y %H:%M:%S'),
                "docType": "File"
            }
            self.db.create(newFile)
            file_info.context = self.counter
            file_info.is_directory = False
            self.counter += 1
            replicate_from_local_ids([binary_id])

        def delete_old_file(doc):                        
            binary = self.db[doc['binary']['file']['id']]
            self.db.delete(binary)
            self.db.delete(self.db[doc['_id']])

        (file_path, name) = _path_split_lower(path)

        ## Create file
        if disposition == CreationDisposition(CREATE_NEW) or disposition == CreationDisposition(CREATE_ALWAYS) :
            res = self.db.view('file/byFullPath', key=file_path+'/'+name)
            if len(res) > 0 :
                # File already exists
                if disposition == CreationDisposition(CREATE_NEW):
                    # Return an error if disposition is CREATE_NEW
                    return -ERROR_ALREADY_EXISTS
                else:            
                    # Delete old file and create a new file if disposition is CREATE_ALWAYS    
                    for doc in res:
                        delete_old_file(doc.value)
                    file_creation()
            else:
                # File doesn't exist
                file_creation()
        
        ## Open file
        else:
            res = self.db.view('folder/byFullPath', key=file_path+'/'+name)
            if len(res) > 0 or path == '\\':
                    file_info.is_directory = True
            else:
                res = self.db.view('file/byFullPath', key=file_path+'/'+name)
                if len(res) > 0 :
                    file_info.is_directory = False
                else:
                    return -ERROR_FILE_NOT_FOUND   
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
        (folder_path, name) = _path_split_lower(path)
        # Check if folder already exists
        res = self.db.view('folder/byFullPath', key=folder_path+'/'+name)
        if len(res) > 0 :
            return -ERROR_FILE_EXISTS
        else:
            res = self.db.view('file/byFullPath', key=folder_path+'/'+name)
            if len(res) > 0 :
                return -ERROR_FILE_EXISTS
            else:
                # Create folder
                (folder_path, name) = _path_split(path)
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
        folder_path = _normalize_path_win_to_DB_lower(path)
        res = self.db.view('folder/byFullPath', key=folder_path)
        if len(res) > 0 or path == '\\':
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
        path = _normalize_path_win_to_DB_lower(path)
        res = self.db.view('file/byFullPath', key=path)
        if len(res) > 0:  
            # File 
            for doc in res:
                if path.find('~') == -1:
                    # Standard file
                    buffer[0].dwFileAttributes = FILE_ATTRIBUTE_NORMAL
                else:
                    # Temporary file
                    buffer[0].dwFileAttributes = FILE_ATTRIBUTE_TEMPORARY                  
                if 'size' in doc.value:
                    win_size = SizeConvert(doc.value['size']).convert()
                else:
                    win_size = SizeConvert(1).convert()
                buffer[0].nFileSizeHigh = win_size[0]
                buffer[0].nFileSizeLow = win_size[1]
                buffer[0].nNumberOfLinks = 1
                win_index = SizeConvert(2).convert()
                buffer[0].nFileIndexHigh = win_index[0]
                buffer[0].nFileIndexLow = win_index[1]
                lastModification = doc.value['lastModification'].split(' GMT')[0]
                dt_converter = DateTimeConvertor(datetime.strptime(lastModification, '%a %b %d %Y %H:%M:%S')) 
                dc = dt_converter.convert()
                if 'creationDate' in doc.value:
                    creationDate = doc.value['creationDate'].split(' GMT')[0]
                    dt_converter_creation = DateTimeConvertor(datetime.strptime(creationDate, '%a %b %d %Y %H:%M:%S'))
                    buffer[0].ftCreationTime = dt_converter_creation.convert()
                else:
                    buffer[0].ftCreationTime = dc
                buffer[0].ftLastAccessTime = dc
                buffer[0].ftLastWriteTime = dc
        else:
            res = self.db.view('folder/byFullPath', key=path)
            if len(res) > 0 or path is '\\':
                # Folder
                buffer[0].dwFileAttributes = FILE_ATTRIBUTE_DIRECTORY
                buffer[0].nFileSizeHigh = 0
                buffer[0].nFileSizeLow = 0
                buffer[0].nNumberOfLinks = 1
                win_index = SizeConvert(1).convert()
                buffer[0].nFileIndexHigh = win_index[0]
                buffer[0].nFileIndexLow = win_index[1]
                dt_converter = DateTimeConvertor(datetime.today())
                dc = dt_converter.convert()
                buffer[0].ftCreationTime = dc
                buffer[0].ftLastAccessTime = dc
                buffer[0].ftLastWriteTime = dc
            else:
                # Document doesn't exist
                return -ERROR_FILE_NOT_FOUND 
        buffer[0].dwVolumeSerialNumber = self.serial_number
        return 0 
    disable_debug('get_info')

    
    @wrap(None, DokanFileInfo)
    @log('path', 'info')
    def cleanup(self, path, file_info):
        path = _normalize_path_win_to_DB_lower(path)
        '''
        Clean file: Cleanup is invoked when the function CloseHandle in Windows 
        API is executed
            path {string}: file/directory path
            file_info {Object}: file/directory information
        '''

        if file_info.delete_on_close: 
            def delete_file(doc):      
                binary = self.db[doc['binary']['file']['id']]
                self.db.delete(binary)
                self.db.delete(self.db[doc['_id']])
            res = self.db.view('file/byFullPath', key=path)
            if len(res) > 0:
                for doc in res:
                    doc = doc.value
                    delete_file(doc)                
            else: 
                return -ERROR_INVALID_HANDLE 
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
        name = 'Digidisk'
        fname = 'couchmontFS'
        memmove(name_buff, LPWSTR(name), (len(name) + 1) * 2)
        memmove(fs_name_buff, LPWSTR(fname), (len(fname) + 1) * 2)
        sn[0] = self.serial_number
        max_comonent_len[0] = 255000
        
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
        free_bytes[0] = 1048576 - len('\\hello_world.txt')
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
        file_info_raw = file_info.raw()
        new_path = _normalize_path_win_to_DB_lower(path)

        # Files
        res = self.db.view('file/byFolder', key=new_path)
        for doc in res:
            doc = doc.value
            if self.name_in_expression(pattern, doc['name'], False):
                if 'size' in doc:
                    win_size = SizeConvert(doc['size']).convert()
                else:
                    win_size = SizeConvert(1).convert()  

                lastModification = doc['lastModification'].split(' GMT')[0]
                dt_converter = DateTimeConvertor(datetime.strptime(lastModification, '%a %b %d %Y %H:%M:%S')) 
                dc = dt_converter.convert()
                if 'creationDate' in doc:
                    creationDate = doc['creationDate'].split(' GMT')[0]
                    dt_converter_creation = DateTimeConvertor(datetime.strptime(creationDate, '%a %b %d %Y %H:%M:%S'))
                    dc_creation = dt_converter.convert()
                else:
                    dc_creation = dc     
                if path.find('~') == -1:
                    info = WIN32_FIND_DATAW(
                        FILE_ATTRIBUTE_NORMAL, dc_creation, dc, dc, 
                        win_size[0], win_size[1], 0, 0, doc['name'], ''
                    )
                else:
                    info = WIN32_FIND_DATAW(
                        FILE_ATTRIBUTE_TEMPORARY, dc_creation, dc, dc,
                        win_size[0], win_size[1], 0, 0, doc['name'], ''
                    )        
                func(PWIN32_FIND_DATAW(info), file_info_raw)

        # Folders
        res = self.db.view('folder/byFolder', key=new_path)
        for doc in res:
            doc = doc.value
            if self.name_in_expression(pattern, doc['name'], False):
                dc = DateTimeConvertor(datetime.today()).convert()
                win_size = SizeConvert(1).convert()
                info = WIN32_FIND_DATAW(
                    FILE_ATTRIBUTE_DIRECTORY, dc, dc, dc, win_size[0],
                    win_size[1], 0, 0, doc['name'], ''
                )
                func(PWIN32_FIND_DATAW(info), file_info_raw)
        return 0
    disable_debug('find_files_with_pattern')


    @wrap(None, None, None, None, DokanFileInfo)
    def set_time(self, path, creation, last_access, last_write, file_info):
        return 0
    disable_debug('set_time')

    @wrap(None, None, DokanFileInfo)
    def set_allocation_size(self, path, alloc, file_info):
        return 0    
    disable_debug('set_allocation_size')

    @wrap(None, None, None, None, None, DokanFileInfo)
    def get_security_info(self, path, info, descr, length, needed_length, file_info):
        return 0    
    disable_debug('get_security_info')

    @wrap(None, None, None, None, DokanFileInfo)
    def set_security_info(self, path, info, descr, length, file_info):
        return 0    
    disable_debug('set_security_info')

    @wrap(None, None, DokanFileInfo)
    def set_attributes(self, path, attributes, file_info ):
        return 0
    disable_debug('set_attributes')

    @wrap(None, None, DokanFileInfo)
    def set_end_of_file(self, path, length, file_info):
        return 0
    disable_debug('set_end_of_file')

    @wrap(None, None, None, DokanFileInfo)
    def lock_file(self, path, offset, length, file_info):
        return 0
    disable_debug('lock_file')

    @wrap(None, None, None, DokanFileInfo)
    def unlock_file(self, path, offset, length, file_info):
        return 0
    disable_debug('unlock_file')

    @wrap(None, DokanFileInfo)
    def flush(self, path, file_info):
        return 0
    disable_debug('flush')
    

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
        new_path = _normalize_path_win_to_DB_lower(path)
        if file_info.context and new_path in self.read_current_file:
            content = self.read_current_file[new_path]
        else:
            res = self.db.view('file/byFullPath', key=new_path)
            if len(res) > 0:
                for doc in res:
                    doc = doc.value
                    # Recover binary
                    try:
                        bin_id = doc['binary']['file']['id']
                    except Exception:
                        return -ERROR_INVALID_HANDLE
                    commande = ['curl', '-H', 'Content-Type: text/html',
                                'http://localhost:5984/cozy-files/%s/file' %bin_id]
                    process = subprocess.Popen(commande, 
                                               shell = True, 
                                               stdout=subprocess.PIPE, 
                                               stderr=subprocess.PIPE)
                    content, err = process.communicate()
                    # Save binary in read_current_file
                    self.read_current_file[new_path] = content
                    file_info.context = 1
            else:
                return -ERROR_INVALID_HANDLE 
            content_length = len(content)
            if offset < content_length:
                if offset + length > content_length:
                    length = content_length - offset
                    # Remove binary in read_current_file
                    del self.read_current_file[new_path]
            else:
                length = 0
        memmove(buffer, content[offset:offset+length], length)
        buff_length[0] = length
        return 0       
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
            (pathto, nameto) = _path_split(dst)
            doc.update({"name":nameto, "path": pathto})
            self.db.save(doc) 

        def move_folder(src, dst, doc):
            pathfrom = _normalize_path_win_to_DB_lower(src)
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

        def move_doc():
            pathfrom = _normalize_path_win_to_DB_lower(src) 
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


        (pathto, name) = _path_split_lower(dst)
        res = self.db.view('folder/byFullPath', key=pathto+'/'+name)
        if len(res) > 0:
            if not replace:
                return -ERROR_ALREADY_EXISTS
            else:
                for doc in res:
                    self.db.delete(doc.value['_id'])
                move_doc()
                return 0
        else:
            res = self.db.view('file/byFullPath', key=pathto+'/'+name)
            if len(res) > 0:  
                if not replace:
                    return -ERROR_ALREADY_EXISTS
                else:
                    for doc in res:
                        doc = doc.value
                        binary = self.db[doc['binary']['file']['id']]
                        self.db.delete(binary)
                        self.db.delete(self.db[doc['_id']])
                    move_doc()
                    return 0
            else:
                move_doc()
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
        path = _normalize_path_win_to_DB_lower(path)
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

        path = _normalize_path_win_to_DB_lower(path)
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
            length {integer}: content length to write
            written_length {LP_u_long}: content length written
            offset {integer}: file offset
            file_info {Object}: file information
        '''
        new_path = _normalize_path_win_to_DB_lower(path)
        res = self.db.view('file/byFullPath', key=new_path)
        if len(res) > 0:   
            for doc in res:
                # Recover binary
                doc = doc.value
                binary_id = doc["binary"]["file"]["id"]
                buffer = string_at(buffer, length)
                commande = ['curl', '-H', 'Content-Type: text/html',
                            'http://localhost:5984/cozy-files/%s/file' %binary_id]
                process = subprocess.Popen(commande, 
                                           shell = True, 
                                           stdout=subprocess.PIPE, 
                                           stderr=subprocess.PIPE)
                currentFile, err = process.communicate()
                # Add changes in binary
                currentFile = currentFile[:offset] + buffer + currentFile[offset:]
                self.db.put_attachment(self.db[binary_id],
                                       currentFile,
                                       filename = "file")
                binary = self.db[binary_id]
                doc['binary']['file']['rev'] = binary['_rev']
                doc['size'] = len(currentFile)
                today = datetime.today()
                doc["lastModification"] = today.strftime('%a %b %d %Y %H:%M:%S')
                self.db.save(doc)
                writen_length[0] = length
                replicate_from_local_ids([binary_id])
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