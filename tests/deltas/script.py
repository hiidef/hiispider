#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Script to manage delta test files."""

import simplejson

def read(filename):
    with open(filename) as f:
        content = f.read()
    return content

def main():
    opts, args = parse_args()
    if opts.view:
        return view(*args)
    if opts.encode:
        return encode(*args)
    if opts.compare:
        return compare(*args)

def view(*filenames):
    """View an encoded file."""
    for filename in filenames:
        if len(filenames) > 1:
            print "File: %s\n------%s" % (filename, '-' * len(filename))
        content = read(filename)
        print simplejson.dumps(simplejson.loads(content.decode('base64')), indent=2)
        if len(filenames) > 1: print ''

def encode(*filenames):
    """Encodes a file.  If it is base64, it does nothing.  If it's json, it
    loads it, dumps it using some default indent, then saves the encoded file.
    If that fails, it assumes it's a python literal to be encoded into json
    and then encoded for the file."""
    def encode_data(data):
        """Encodes python data."""
        return simplejson.dumps(data, indent=2).encode('base64')
    for filename in filenames:
        content = read(filename)
        # try to decode first
        try:
            content.decode('base64')
            continue
        except:
            pass
        # try to load json
        try:
            data = simplejson.loads(content)
            with open(filename, 'w') as f:
                f.write(encode_data(data))
            continue
        except:
            pass
        try:
            data = eval(content)
            with open(filename, 'w') as f:
                f.write(encode_data(data))
            continue
        except:
            raise Exception("Could not determine filetype for %s." % filename)

def compare(new, old):
    from hiispider import delta
    new = simplejson.loads(read(new).decode("base64"))
    old = simplejson.loads(read(old).decode("base64"))
    print simplejson.dumps(delta.autogenerate(new, old), indent=2)

class ArgumentException(Exception):
    pass

def parse_args():
    import optparse
    parser = optparse.OptionParser(usage='./%prog [options] [files]', version='1.0')
    parser.add_option('-v', '--view', action='store_true', help='view json representation of file')
    parser.add_option('-e', '--encode', action='store_true', help='encode files')
    parser.add_option('-c', '--compare', action='store_true', help='compare files w/ delta.autocompare')
    opts, args = parser.parse_args()
    if opts.view and opts.encode:
        raise ArgumentException("Incompatible options --view and --encode.")
    if opts.compare and not len(args) == 2:
        raise ArgumentException("--compare requires 2 (two) file arguments.")
    return opts, args

if __name__ == '__main__':
    try:
        main()
    except ArgumentException, ex:
        print str(ex)
    except KeyboardInterrupt:
        pass

