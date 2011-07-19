import marshal
import cPickle
from collections import Hashable, Iterable


class AutogenerateException(Exception):
    pass
    

def _recursive_hash_sort(x, ignores, includes):
    if isinstance(x, basestring):
        # Don't try to sort strings.
        return x
    elif isinstance(x, dict):
        values = []
        included_keys = [y[0] for y in includes if len(y) > 0]
        ignored_keys = [y[0] for y in ignores if len(y) == 1]
        # Recurse through all items.
        for key in x:
            if key in included_keys:
                values.append((key, 
                    _recursive_hash_sort(x[key], 
                        [y[1:] for y in ignores],
                        [y[1:] for y in includes])))
            elif len(included_keys) == 0 and key not in ignored_keys:
                values.append((key, 
                    _recursive_hash_sort(x[key], 
                        [y[1:] for y in ignores],
                        [y[1:] for y in includes])))
        # Convert into a list, sorted by key.
        return sorted(values, key=lambda x:x[0])
    elif isinstance(x, Iterable):
        return sorted([_recursive_hash_sort(y, ignores, includes) for y in x])
    return x
    

def _structured_hash(x, ignores, includes):
     # Item is hashable no need to serialize.
    if isinstance(x, Hashable):
        return hash(x)
    # As dictionaries and other types of iterables don't have order,
    # marshalling then hashing identical dictionaries won't provide
    # the same hash. To remedy this we convert dictionaries and other
    # iterables into lists and sort the lists prior to hashing.
    x = _recursive_hash_sort(x, ignores, includes)
    try:
        # Marshal is fast. Try it first.
        return hash(marshal.dumps(x))
    except ValueError:
        # Try cPickle.
        return hash(cPickle.dumps(x))


def _convert_to_hashed_dict(a, ignores, includes):
    """
    Convert a list into a dictionary suitable for comparison of structured
    data types.
    """
    return dict([(_structured_hash(x, ignores, includes), x) for x in a])


def _narrow_list(a, path):
    """
    Narrow a list of dictionaries to a list of key:value pairs indicated by path.
    """
    if len(path) == 0:
        return a
    if isinstance(a, list):
        return [_narrow_list(x, path) for x in a if x]
    elif isinstance(a, dict):
        key = path[0]
        if key in a:
            return {key:_narrow_list(a[key], path[1:])}
    return []
    
    
def _compare_lists(a, b, path, ignores, includes):
    """
    Compare two lists composed of objects.

    Returns a list of objects in 'a' not contained in 'b' or in the event
    there are no common elements, an empty list.
    """
 
    a = _narrow_list(a, path)
    b = _narrow_list(b, path)
    try:
        return list(set(a) - set(b))
    except TypeError, e:
        # A and b are not hashable.
        # We don't need to worry about ignores here
        # as dicts aren't hashable anyway.
        pass
    hashed_a = _convert_to_hashed_dict(a, ignores, includes)
    hashed_b = _convert_to_hashed_dict(b, ignores, includes)
    # Compare the hashes to get a list of items in a that are not in b.
    return [hashed_a[x] for x in set(hashed_a.keys()) - set(hashed_b.keys())]


def _compare_dicts(a, b, path, ignores, includes):
    """
    Compare two dictonaries composed of objects.

    Returns a list of (key, value) pairs in 'a' not contained in 'b' or in the
    event there are no common elements, an empty list.
    """
    values = []
    included_keys = [x[0] for x in includes if len(x) > 0]
    ignored_keys = [x[0] for x in ignores if len(x) == 1]
    for key in a:
        if len(path) > 0:
            if key != path[0]:
                continue
        if len(included_keys) > 0 and key not in included_keys:
            continue
        if key in ignored_keys:
            continue
        if key in b:
            if a[key] == b[key]:
                # Values are equal.
                continue
            elif isinstance(a[key], Hashable) and isinstance(b[key], Hashable):
                # Values are hashable, like strings, but not equal.
                values.append({key:a[key]})
                continue
            # Moving beyond the built-in comparisons.
            if a[key].__class__ != b[key].__class__:
                # Values are different types.
                values.append({key:a[key]})
                continue
            elif isinstance(a[key], list):
                # Return new items in lists at key.
                deltas = _compare_lists(
                    a[key], 
                    b[key], 
                    path[1:], 
                    [x[1:] for x in ignores],
                    [x[1:] for x in includes])
                if len(deltas) > 0:
                    values.append({key:deltas})
                continue
            elif isinstance(a[key], dict):
                # Return new items in dicts at key.
                delta = {}
                deltas = _compare_dicts(
                    a[key], 
                    b[key], 
                    path[1:],
                    [x[1:] for x in ignores],
                    [x[1:] for x in includes])
                for x in deltas:
                    delta.update(x)
                if len(delta) > 0:
                    values.append({key:delta})
                continue
            if _structured_hash(a[key], ignores, includes) != _structured_hash(b[key], ignores, includes):
                values.append({key:a[key]})
        else:
            values.append({key:a[key]})
    return values

class Autogenerator(object):
    
    def __init__(self, paths=None, ignores=None, includes=None):
        paths = self.parse_paths(paths)
        includes = self.parse_paths(includes)
        ignores = self.parse_paths(ignores)
        if len(paths) == 0:
            self.paths = [{
                "path":[],
                "includes":includes,
                "ignores":ignores}]
        else:
            self.paths = []
            for path in paths:
                path_parameters = {
                    "path":path,
                    "includes":[],
                    "ignores":[]}
                for include in includes:
                    if include[0:len(path)] == path:
                        path_parameters["includes"].append(include)
                for ignore in ignores:
                    if ignore[0:len(path)] == path:
                        path_parameters["ignores"].append(ignore)
                    if path[0:len(ignore)] == ignore:
                        raise AutogenerateException("%s ignore supersedes"
                            " %s path." % (ignore, path))
                    for include in includes:
                        if include[0:len(ignore)] == ignore:
                            raise AutogenerateException("%s ignore supersedes"
                                " %s include." % (ignore, path))
                self.paths.append(path_parameters)
        
    def __call__(self, a, b):
        """
        Compare dictionaries or lists of objects. Returns a list.
        """
        deltas = []
        for path in self.paths:
            deltas += self.call(
                a, 
                b, 
                path)
        return deltas
    
    def call(self, a, b, path):
        # Native Python comparison. Should be well optimized across VMs.
        if a == b:
            return []
        # Type comparison. If they aren't the same thing, not much point in
        # going forward.
        if a.__class__ != b.__class__:
            raise TypeError("Cannot generate delta from %s to %s." % (
                a.__class__,
                b.__class__))
        if isinstance(a, basestring):
            raise TypeError("Cannot generate delta from strings.")
        if isinstance(a, list):
            return _compare_lists(
                a, 
                b,
                path["path"],
                path["ignores"],
                path["includes"])
        if isinstance(a, dict):
            return _compare_dicts(
                a, 
                b,
                path["path"],
                path["ignores"],
                path["includes"])
        raise TypeError("Cannot generate delta from %s." % a.__class__)

    def parse_paths(self, paths):
        if paths is None:
            return []
        if isinstance(paths, basestring):
            paths = [paths]
        elif not isinstance(paths, list):
            raise TypeError("Parameter must be str, unicode, or list.")
        # Filters a list of paths split on '/' to remove empty strings.
        return [[x for x in path.split("/") if x] for path in paths]
        
        