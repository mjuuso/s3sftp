#!/usr/bin/python

import sys
import os
import time
import datetime
import socket
import optparse
import paramiko
import boto3
import botocore
import stat
import multiprocessing

class S3Server(paramiko.ServerInterface):
    def __init__(self, s3bucket):
        self.s3bucket = s3bucket

    def check_auth_password(self, username, password):
        # all are allowed
        return paramiko.AUTH_SUCCESSFUL

    def check_auth_publickey(self, username, key):
        # all are allowed
        return paramiko.AUTH_SUCCESSFUL

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED

class S3SFTPHandle(paramiko.SFTPHandle):
    s3obj = None
    def stat(self):
        attr = paramiko.SFTPAttributes()
        attr.st_mode = (stat.S_IFREG | stat.S_IREAD | stat.S_IWRITE)
        return attr

    def chattr(self, attr):
        # python doesn't have equivalents to fchown or fchmod, so we have to
        # use the stored filename
        try:
            SFTPServer.set_file_attr(self.filename, attr)
            return paramiko.SFTP_OK
        except OSError as e:
            return SFTPServer.convert_errno(e.errno)

    def read(self, offset, length):
        return self.s3obj['Body'].read(length)

    def write(self):
        pass

    def close(self):
        pass


class S3SFTPServer(paramiko.SFTPServerInterface):
    def __init__(self, s3server, bucket):
        self.s3bucket = bucket

    def _path2prefix(self, path):
        # remove prepended slash
        if path.startswith('/'):
            path = path[1:]

        # prefix should have a trailing slash
        if not path.endswith('/'):
            path = path + '/'

        # root should be empty
        if path == '/':
            path = ''

        return path

    def list_folder(self, path):
        out = []
        prefix = self._path2prefix(path)
        result = self.s3bucket.meta.client.list_objects(Bucket=self.s3bucket.name, Delimiter='/', Prefix=prefix)

        directories = result.get('CommonPrefixes')
        if directories:
            for directory in directories:
                attr = paramiko.SFTPAttributes()
                attr.filename = directory.get('Prefix')
                attr.filename = attr.filename[len(prefix):] # remove the prepended prefix path
                attr.filename = attr.filename[:-1] # prefixes always come with trailing slashes
                attr.st_mode = (stat.S_IFDIR | stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
                attr.st_atime = 946684800
                attr.st_mtime = attr.st_atime
                out.append(attr)

        for f in self.s3bucket.objects.filter(Delimiter='/', Prefix=prefix):
            attr = paramiko.SFTPAttributes()
            attr.filename = f.key
            attr.filename = attr.filename[len(prefix):] # remove the prepended prefix path
            attr.st_mode = (stat.S_IFREG | stat.S_IREAD | stat.S_IWRITE)
            attr.st_atime = int(((f.last_modified.replace(tzinfo=None) - f.last_modified.utcoffset()) - datetime.datetime(1970, 1, 1)).total_seconds())
            attr.st_mtime = attr.st_atime
            attr.st_size = f.size
            out.append(attr)
        return out

    def stat(self, path):
        prefix = self._path2prefix(path)[:-1]
        try:
            obj = self.s3bucket.meta.client.get_object(Bucket=self.s3bucket.name, Key=prefix)
            # it's a file
            attr = paramiko.SFTPAttributes()
            attr.filename = path
            attr.st_mode = (stat.S_IFREG | stat.S_IREAD | stat.S_IWRITE)
            attr.st_mtime = obj['LastModified']
            return attr
        except botocore.exceptions.ClientError:
            # it doesn't exist, so it's likely a prefix
            attr = paramiko.SFTPAttributes()
            attr.filename = path
            attr.st_mode = (stat.S_IFDIR | stat.S_IREAD | stat.S_IWRITE | stat.S_IEXEC)
            attr.st_atime = 946684800
            attr.st_mtime = attr.st_atime
            return attr

    def lstat(self, path):
        return self.stat(path)

    def open(self, path, flags, attr):
        prefix = self._path2prefix(path)[:-1]
        obj = self.s3bucket.meta.client.get_object(Bucket=self.s3bucket.name, Key=prefix)
        fobj = S3SFTPHandle(flags)
        fobj.filename = prefix
        fobj.s3obj = obj
        return fobj

    def remove(self, path):
        pass

    def mkdir(self, path, attr):
        path = self._realpath(path)
        try:
            os.mkdir(path)
            if attr is not None:
                SFTPServer.set_file_attr(path, attr)
        except OSError as e:
            return SFTPServer.convert_errno(e.errno)
        return SFTP_OK

    def rmdir(self, path):
        return paramiko.SFTP_OP_UNSUPPORTED

    def chattr(self, path, attr):
        return paramiko.SFTP_OP_UNSUPPORTED

    def symlink(self, target_path, path):
        return paramiko.SFTP_OP_UNSUPPORTED

    def readlink(self, path):
        return paramiko.SFTP_OP_UNSUPPORTED

    def rename(self, oldpath, newpath):
        return paramiko.SFTP_OP_UNSUPPORTED

def S3SFTPServerProcess(conn, options, s3bucket):
    host_key = paramiko.RSAKey.from_private_key_file(options.keyfile)
    transport = paramiko.Transport(conn)
    transport.add_server_key(host_key)
    transport.set_subsystem_handler('sftp', paramiko.SFTPServer, S3SFTPServer, s3bucket)

    server = S3Server(s3bucket)
    transport.start_server(server=server)

    channel = transport.accept()
    while transport.is_active():
        time.sleep(1)

def serve(options, s3bucket):
    paramiko_level = getattr(paramiko.common, options.loglevel.upper())
    paramiko.common.logging.basicConfig(level=paramiko_level)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
    server_socket.bind((options.host, options.port))
    server_socket.listen(5)

    servers = []

    while True:
        conn, addr = server_socket.accept()
        s = multiprocessing.Process(target=S3SFTPServerProcess, args=(conn, options, s3bucket))
        s.start()
        servers.append(s)

def main():
    parser = optparse.OptionParser()
    parser.add_option('-H', '--host', dest='host', default="localhost",
            help='listen on HOST (defaults to %default)')
    parser.add_option('-P', '--port', dest='port', type='int', default="22",
            help='listen on PORT (defaults to %default)')
    parser.add_option('-l', '--log-level', dest='loglevel', default='INFO',
            help='Log level (defaults to %default)')
    parser.add_option('-m', '--maxconns', dest='maxconns', type='int', default="10",
                        help='allow MAXCONNS concurrent connections (defaults to %default)')
    parser.add_option('-b', '--bucket', dest='bucket',
            help='AWS S3 bucket name')
    parser.add_option('-u', '--access-key-id', dest='accesskey',
            help='AWS Access Key ID (or set AWS_ACCESS_KEY_ID)')
    parser.add_option('-s', '--secret-access-key', dest='secretkey',
            help='AWS Secret Access Key (or set AWS_SECRET_ACCESS_KEY)')
    parser.add_option('-r', '--default-region', dest='region', default="eu-west-1", help='AWS Region (defaults to %default, or set AWS_DEFAULT_REGION)')
    parser.add_option('-k', '--keyfile', dest='keyfile', default='id_rsa',
                        help='RSA server key file (defaults to %default, generated if missing)')
    opt, args = parser.parse_args()

    if (opt.bucket is None) or \
    (os.getenv('AWS_ACCESS_KEY_ID', opt.accesskey) is None) or \
    (os.getenv('AWS_SECRET_ACCESS_KEY', opt.secretkey) is None):
        parser.print_help()
        sys.exit(-1)

    session = boto3.Session(aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID', opt.accesskey),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY', opt.secretkey),
            region_name=os.getenv('AWS_DEFAULT_REGION', opt.region))
    s3 = session.resource('s3')
    s3bucket = s3.Bucket(opt.bucket)

    if not os.path.exists(opt.keyfile):
        print 'Host key "%s" not found, generating 2048-bit RSA key...' % opt.keyfile
        rsa_key = paramiko.RSAKey.generate(bits=2048)
        rsa_key.write_private_key_file(opt.keyfile)
        del rsa_key

    serve(options=opt, s3bucket=s3bucket)

if __name__ == '__main__':
    main()
