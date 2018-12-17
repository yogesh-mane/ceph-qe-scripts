import os, sys

sys.path.append(os.path.abspath(os.path.join(__file__, "../../../")))
import v2.utils.log as log
from v2.utils.utils import FileOps

IO_INFO_FNAME = 'io_info.yaml'

EXEC_INFO_STRUCTURE = {'obj': None,
                       'resource': None,
                       'kwargs': None,
                       'args': None,
                       'extra_info': None}


class BasicIOInfoStructure(object):
    def __init__(self):
        self.initial = lambda: {'users': list()}
        self.user = lambda **args: {'user_id': args['user_id'],
                                    'access_key': args['access_key'],
                                    'secret_key': args['secret_key'],
                                    'bucket': list()
                                    }
        self.bucket = lambda **args: {'name': args['name'], 'properties': list(), 'keys': list()}
        self.key = lambda **args: {'name': args['name'],
                                   'size': args['size'],
                                   'md5_local': args['md5_local'],
                                   'upload_type': args['upload_type'],
                                   'properties': list()
                                   }


class ExtraIOInfoStructure(object):
    def __init__(self):
        self.op_code = lambda op_code: {'op_code': op_code}
        self.version_count = lambda version_count: {'version_count': version_count}


class TenantInfo(object):
    def __init__(self):
        self.tenant = lambda tenant: {'tenant': tenant}


class AddIOInfo(object):
    def __init__(self, yaml_fname=IO_INFO_FNAME):
        self.yaml_fname = yaml_fname
        self.file_op = FileOps(self.yaml_fname, type='yaml')


class IOInfoInitialize(AddIOInfo):
    def __init__(self):
        super(IOInfoInitialize, self).__init__()

    def initialize(self, data):
        log.info('initial_data: %s' % (data))
        self.file_op.add_data(data)


class AddUserInfo(AddIOInfo):
    def __init__(self):
        super(AddUserInfo, self).__init__()

    def add_user_info(self, user):
        log.info('got user info structure: %s' % user)
        yaml_data = self.file_op.get_data()
        log.info('got yaml data %s' % yaml_data)
        yaml_data['users'].append(user)
        log.info('data to add: %s' % yaml_data)
        self.file_op.add_data(yaml_data)


class BucketIoInfo(AddIOInfo):
    def __init__(self):
        super(BucketIoInfo, self).__init__()

    def add_bucket_info(self, access_key, bucket_info):
        yaml_data = self.file_op.get_data()
        indx = None
        for i, k in enumerate(yaml_data['users']):
            if k['access_key'] == access_key:
                indx = i
                break
        yaml_data['users'][indx]['bucket'].append(bucket_info)
        self.file_op.add_data(yaml_data)

    def add_properties(self, access_key, bucket_name, properties):
        yaml_data = self.file_op.get_data()
        access_key_indx = None
        bucket_indx = None
        for i, k in enumerate(yaml_data['users']):
            if k['access_key'] == access_key:
                access_key_indx = i
                break
        for i, k in enumerate(yaml_data['users'][access_key_indx]['bucket']):
            if k['name'] == bucket_name:
                bucket_indx = i
                break
        yaml_data['users'][access_key_indx]['bucket'][bucket_indx]['properties'].append(properties)
        self.file_op.add_data(yaml_data)


class KeyIoInfo(AddIOInfo):
    def __init__(self):
        super(KeyIoInfo, self).__init__()

    def add_keys_info(self, access_key, bucket_name, key_info):
        yaml_data = self.file_op.get_data()
        access_key_indx = None
        bucket_indx = None
        for i, k in enumerate(yaml_data['users']):
            if k['access_key'] == access_key:
                access_key_indx = i
                break
        for i, k in enumerate(yaml_data['users'][access_key_indx]['bucket']):
            if k['name'] == bucket_name:
                bucket_indx = i
                break
        yaml_data['users'][access_key_indx]['bucket'][bucket_indx]['keys'].append(key_info)
        self.file_op.add_data(yaml_data)

    def add_properties(self, access_key, bucket_name, key_name, properties):
        yaml_data = self.file_op.get_data()
        access_key_indx = None
        bucket_indx = None
        key_indx = None
        for i, k in enumerate(yaml_data['users']):
            if k['access_key'] == access_key:
                access_key_indx = i
                break
        for i, k in enumerate(yaml_data['users'][access_key_indx]['bucket']):
            if k['name'] == bucket_name:
                bucket_indx = i
                break
        for i, k in enumerate(yaml_data['users'][access_key_indx]['bucket'][bucket_indx]):
            if k['name'] == key_name:
                key_indx = i
                break
        yaml_data['users'][access_key_indx]['bucket'][bucket_indx]['keys'][key_indx].append(properties)
        self.file_op.add_data(yaml_data)


def logioinfo(func):
    def write(exec_info):
        log.info('in write')
        log.info(exec_info)
        ret_val = func(exec_info)
        if ret_val is False:
            return ret_val
        gen_basic_io_info_structure = BasicIOInfoStructure()
        gen_extra_io_info_structure = ExtraIOInfoStructure()
        write_bucket_info = BucketIoInfo()
        write_key_info = KeyIoInfo()
        obj = exec_info['obj']
        resource_name = exec_info['resource']
        extra_info = exec_info.get('extra_info', None)
        print 'obj_name :%s' % obj
        if 's3.Bucket' == type(obj).__name__:
            if resource_name == 'create':
                access_key = extra_info['access_key']
                log.info('adding io info of create bucket')
                bucket_info = gen_basic_io_info_structure.bucket(**{'name': obj.name})
                write_bucket_info.add_bucket_info(access_key, bucket_info)
            if resource_name == 'upload_file':
                access_key = extra_info['access_key']
                log.info('adding io info of upload objects')
                key_upload_info = gen_basic_io_info_structure.key(
                    **{'name': extra_info['name'], 'size': extra_info['size'],
                       'md5_local': extra_info['md5'],
                       'upload_type': 'normal'})
                write_key_info.add_keys_info(access_key, obj.name, key_upload_info)
        print 'writing log for %s' % resource_name
        return ret_val

    return write


"""

if __name__ == '__main__':
    # test data

    user_data1 = {'user_id': 'batman',
                  'access_key': '235sff34',
                  'secret_key': '87324skfs',
                  }

    user_data2 = {'user_id': 'heman',
                  'access_key': 'sfssf',
                  'secret_key': '87324skfs',
                  }

    user_data3 = {'user_id': 'antman',
                  'access_key': 'fwg435',
                  'secret_key': '87324skfs',
                  }

    key_info1 = {'name': 'k1', 'size': 374, 'md5_on_s3': 'sfsf734', 'upload_type': 'normal', 'test_op_code': 'create'}
    key_info2 = {'name': 'k2', 'size': 242, 'md5_on_s3': 'sgg345', 'upload_type': 'normal', 'test_op_code': 'create'}
    key_info3 = {'name': 'k3', 'size': 3563, 'md5_on_s3': 'sfy4hfd', 'upload_type': 'normal', 'test_op_code': 'create'}

    key_info4 = {'key_name': 'k4', 'size': 2342, 'md5_on_s3': 'sfsf3534', 'upload_type': 'normal',
                 'test_op_code': 'create'}

    from v2.lib.s3.gen_io_info_structure import BasicIOInfoStructure, ExtraIOInfoStructure


    basic_io_struct = BasicIOInfoStructure()
    extened_io_struct = ExtraIOInfoStructure()

    io_init = IOInfoInitialize()

    write_user_info = AddUserInfo()
    write_bucket_io_info = BucketIoInfo()
    write_key_io_info = KeyIoInfo()

    io_init.initialize(basic_io_struct.initial())

    # generate io the structure

    u1 = basic_io_struct.user(**user_data1)
    b1 = basic_io_struct.bucket(**{'name': 'b3', 'test_op_code': 'create'})

    # b1_extened_info = dict(b1, **extened_io_struct.version_count('5'))

    k1 = basic_io_struct.key(**key_info1)

    # write the io structure to yaml file

    write_user_info.add_user_info(u1)
    write_bucket_io_info.add_bucket_info(access_key='235sff34', bucket_info=b1)
    write_key_io_info.add_keys_info(access_key='235sff34', bucket_name=b1['name'], key_info=k1)

    #io_info.add_keys_info(access_key='235sff34', bucket_name='b3', **key_info3)
    #io_info.add_keys_info(access_key='235sff34', bucket_name='b3', **key_info4)
    
"""
