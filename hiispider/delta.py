import marshal
import cPickle
from collections import Hashable, Iterable
from copy import copy
from itertools import chain

class AutogenerateException(Exception):
    pass


def _shift(a):
    return [x[1:] for x in a]


def _sort(a, ignores, includes):
    if isinstance(a, basestring):
        # Don't try to sort strings.
        return a
    elif isinstance(a, dict):
        # If there are include paths, iterate through keys in these paths.
        included_keys = [x[0] for x in includes if len(x) > 0]
        if len(included_keys) > 0:
            keys = set(a.keys()).intersection(included_keys)
        else:
            # Disregard keys at the top level of the ignore paths.
            ignored_keys = [x[0] for x in ignores if len(x) == 1]
            keys = set(a.keys()).difference(ignored_keys)
        # Return a sorted list of (key, value) tuples with the included
        # or ignored keys.
        return sorted(
            [(x, _sort(a[x], _shift(ignores), _shift(includes))) for x in keys], 
            key=lambda x: x[0])
    elif isinstance(a, Iterable):
        return sorted([_sort(x, ignores, includes) for x in a])
    return a


def _hash(a, ignores, includes):
     # Item is hashable no need to serialize.
    if isinstance(a, Hashable):
        return hash(a)
    # As dictionaries and other types of iterables don't have order,
    # marshalling then hashing identical dictionaries won't provide
    # the same hash. To remedy this we convert dictionaries and other
    # iterables into lists and sort the lists prior to hashing.
    a = _sort(a, ignores, includes)
    try:
        # Marshal is fast. Try it first.
        return hash(marshal.dumps(a))
    except ValueError:
        # Try cPickle.
        return hash(cPickle.dumps(a))


def _convert_to_hashed_dict(a, ignores, includes):
    """
    Convert a list into a dictionary suitable for comparison of structured
    data types.
    """
    return dict([(_hash(x, ignores, includes), x) for x in a])


def _narrow_list(a, path):
    """
    Recursively remove keys not in path from dictonaries that are ancestors
    of list 'a'
    """
    # If the path is empty, no need to narrow any further.
    if len(path) == 0:
        return a
    # Recurse through elements in the list, keeping the same path level.
    elif isinstance(a, list):
        return [_narrow_list(x, path) for x in a if x]
    # If path exists in the dictionary, return a new dictionary of
    # that key and a recursively narrowed value, moving one path level.
    elif isinstance(a, dict):
        key = path[0]
        if key in a:
            return {key: _narrow_list(a[key], path[1:])}
    # If a path is specified and a is not a list or a dict, return an empty
    # list. As this function is only initially called on lists, this should
    # not cause problems.
    return []


def _compare_lists(a, b, ignores, includes, path=None):
    """
    Compare two lists composed of objects.

    Returns a list of objects in 'a' not contained in 'b' or in the event
    there are no common elements, an empty list.
    """
    if path:
        a = _narrow_list(a, path)
        b = _narrow_list(b, path)
    try:
        # We don't need to worry about includes/ignores here
        # as dicts aren't hashable.
        return list(set(a) - set(b))
    except TypeError:
        # A and b are not hashable.
        pass
    hashed_a = _convert_to_hashed_dict(a, ignores, includes)
    hashed_b = _convert_to_hashed_dict(b, ignores, includes)
    # Compare the hashes to get a list of items in a that are not in b.
    return [hashed_a[x] for x in set(hashed_a.keys()) - set(hashed_b.keys())]


def _compare_dicts(a, b, ignores, includes):
    """
    Compare two dictonaries composed of objects.

    Returns a list of (key, value) pairs in 'a' not contained in 'b' or in the
    event there are no common elements, an empty list.
    """ 
    # If there are include paths, iterate through keys in these paths.
    included_keys = [x[0] for x in includes if len(x) > 0]
    if len(included_keys) > 0:
        keys = set(a.keys()).intersection(included_keys)
    else:
        # Disregard keys at the top level of the ignore paths.
        ignored_keys = [x[0] for x in ignores if len(x) == 1]
        keys = set(a.keys()).difference(ignored_keys)
    values = []
    for key in keys:
        if key not in b:
            values.append({key:a[key]})
            continue
        x, y = a[key], b[key]
        if x == y:
            pass
        elif isinstance(x, Hashable) and isinstance(y, Hashable):
            # Values are hashable, but not equal.
            values.append({key:x})
        # Moving beyond the built-in comparisons.
        elif x.__class__ != y.__class__:
            # Values are different types.
            values.append({key:x})
        elif isinstance(x, list):
            # Return new items in lists at key.
            deltas = _compare_lists(x, y, _shift(ignores), _shift(includes))
            if len(deltas) > 0:
                values.append({key:deltas})
        elif isinstance(x, dict):
            # Return new items in dicts at key.
            deltas = _compare_dicts(x, y, _shift(ignores), _shift(includes))
            if len(deltas) > 0:
                delta = {}
                for z in deltas:
                    delta.update(z)
                values.append({key:delta})
        elif _hash(x, ignores, includes) != _hash(y, ignores, includes):
            values.append({key:x})
    return values


class Autogenerator(object):
    def __init__(self, paths=None, ignores=None, includes=None):
        paths = self._parse_paths(paths)
        includes = self._parse_paths(includes)
        ignores = self._parse_paths(ignores)
        if len(paths) == 0:
            for ignore in ignores:
                for include in includes:
                    if include.startswith(ignore):
                        raise AutogenerateException("%s ignore supersedes"
                            " %s include." % (ignore, include))
            self.paths = [{
                "path":[],
                "includes":includes,
                "ignores":ignores}]
        else:
            self.paths = []
            for path in paths:
                path_parameters = {
                    "path": path,
                    "includes": [],
                    "ignores": []}
                for include in includes:
                    if include.startswith(path):
                        path_parameters["includes"].append(include)
                for ignore in ignores:
                    if ignore.startswith(path):
                        path_parameters["ignores"].append(ignore)
                    if path.startswith(ignore):
                        raise AutogenerateException("%s ignore supersedes"
                            " %s path." % (ignore, path))
                    for include in includes:
                        if include.startswith(ignore):
                            raise AutogenerateException("%s ignore supersedes"
                                " %s include." % (ignore, include))
                self.paths.append(path_parameters)
        
    def __call__(self, a, b):
        """
        Compare dictionaries or lists of objects. Returns a list.
        """
        # http://docs.python.org/library/itertools.html#itertools.chain
        # Joins a list of lists of deltas together.
        return list(chain(*[self._call(a, b, p) for p in self.paths]))

    def _call(self, a, b, pathdata):
        path = pathdata["path"]
        ignores = pathdata["ignores"]
        includes = pathdata["includes"]
        while len(path) > 0:
            key = path[0]
            if isinstance(a, dict) and isinstance(b, dict):
                if key in a and key in b:
                    a, b = a[key], b[key]
                    path = path[1:]
                    ignores, includes = _shift(ignores), _shift(includes)
                    continue
                else:
                    # If the specified path doesn't exist in both objects,
                    # don't try to make a comparison.
                    return []
            else:
                break
        # Native Python comparison. Should be well optimized across VMs.
        if a == b:
            return []
        # Type comparison. If they aren't the same thing, not much point in
        # going forward.
        elif a.__class__ != b.__class__:
            raise TypeError("Cannot generate delta from %s to %s." % (
                a.__class__,
                b.__class__))
        # Ignore strings at this depth, otherwise we're generating a
        # string diff.
        elif isinstance(a, basestring):
            raise TypeError("Cannot generate delta from strings.")
        elif isinstance(a, list):
            return _compare_lists(a, b, ignores, includes, path)
        elif isinstance(a, dict):
            return _compare_dicts(a, b, ignores, includes)
        else:
            raise TypeError("Cannot generate delta from %s." % a.__class__)

    def _parse_paths(self, paths):
        if paths is None:
            return []
        if isinstance(paths, basestring):
            paths = [paths]
        elif not isinstance(paths, list):
            raise TypeError("Parameter must be str, unicode, or list.")
        # Filters a list of paths split on '/' to remove empty strings.
        return [[x for x in path.split("/") if x] for path in paths]
