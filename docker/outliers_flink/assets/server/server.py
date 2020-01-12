#!/usr/bin/env python
"""
Very simple HTTP server in python (Updated for Python 3.7)

Usage:

    ./dummy-web-server.py -h
    ./dummy-web-server.py -l localhost -p 8000

Send a GET request:

    curl http://localhost:8000

Send a HEAD request:

    curl -I http://localhost:8000

Send a POST request:

    curl -d "foo=bar&bin=baz" http://localhost:8000

"""
import re
import json
import argparse
import os
import subprocess
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

jobs = []


class S(BaseHTTPRequestHandler):
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        
    def do_GET(self):
        root = os.path.dirname(os.path.realpath(__file__))
        if self.path == '/':
            filename = root + '/index.html'
        else:
            filename = root + self.path
 
        self.send_response(200)
        if filename[-4:] == '.css':
            self.send_header('Content-type', 'text/css')
        elif filename[-5:] == '.json':
            self.send_header('Content-type', 'application/javascript')
        elif filename[-3:] == '.js':
            self.send_header('Content-type', 'application/javascript')
        elif filename[-4:] == '.ico':
            self.send_header('Content-type', 'image/x-icon')
        else:
            self.send_header('Content-type', 'text/html')
        self.end_headers()
        with open(filename, 'rb') as fh:
            html = fh.read()
            self.wfile.write(html)
            
    def do_POST(self):
        global jobs
        content_len = int(self.headers.get('Content-Length'))
        if self.path == '/start':
            tmp_body = self.rfile.read(content_len).decode()
            body = json.loads(tmp_body)
            cmd = 'flink run -d $ARTIFACTS/$JOB_NAME --space ' + body["space"] + ' --algorithm ' + body["algorithm"] + ' --W "' + body["w"] + '" --S "' + body["s"] + '" --k "' + body["k"] + '" --R "' + body["r"] + '" --dataset ' + body["dataset"] + ' --partitioning ' + body["partitioning"]
            if body['treeNumber']:
                cmd += ' --tree_init ' + str(body["treeNumber"])
            cmd += ' && flink run -d -c custom_source.Custom_source $ARTIFACTS/$JOB_NAME --dataset ' + body["dataset"]
            result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
            jobs = [r.split()[1] for r in re.findall("JobID \w*", result)]
        elif self.path == '/stop':
            for job in jobs:
                cmd = 'flink cancel ' + job
                result = subprocess.run([cmd], shell=True, stdout=subprocess.PIPE).stdout.decode('utf-8')
        self.send_response(200)
        self.end_headers()
        response = BytesIO()
        self.wfile.write(response.getvalue())


def run(server_class=HTTPServer, handler_class=S, addr="0.0.0.0", port=8000):
    server_address = (addr, port)
    httpd = server_class(server_address, handler_class)

    print("Starting httpd server")
    httpd.serve_forever()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Run a simple HTTP server")
    parser.add_argument(
        "-l",
        "--listen",
        default="0.0.0.0",
        help="Specify the IP address on which the server listens",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8000,
        help="Specify the port on which the server listens",
    )
    args = parser.parse_args()
    run(addr=args.listen, port=args.port)
