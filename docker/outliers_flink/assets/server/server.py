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
            cmd = 'flink run --parallelism 16 -d $ARTIFACTS/$JOB_NAME '
            cmd += ' --algorithm ' + str(body["algorithm"])
            cmd += ' --W "' + str(body["w"]) + '"'
            cmd += ' --S "' + str(body["s"]) + '"'
            cmd += ' --k "' + str(body["k"]) + '"'
            cmd += ' --R "' + str(body["r"]) + '"'
            cmd += ' --dataset ' + str(body["dataset"])
            cmd += ' --partitioning ' + str(body["partitioning"])
            cmd += ' --sample_size ' + str(body["treeNumber"])
            cmd += ' --partitions "' + str(body["partitions"]) + '"'
            cmd += ' --distance "' + str(body["distance"]) + '"'
            cmd += ' --policy ' + str(body["adaptivity"])
            if body["adaptivity"] != "static":
                cmd += ' --adapt_range ' + str(body["range"])
                cmd += ' --adapt_over ' + str(body["overload"])
                cmd += ' --adapt_cost ' + str(body["cost_function"])
                cmd += ' --buffer_period ' + str(body["buffer_period"])
                if body["adaptivity"] == "advanced":
                    cmd += ' --adapt_queue ' + str(body["queue"])
                    cmd += ' --adapt_under ' + str(body["underload"])
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
