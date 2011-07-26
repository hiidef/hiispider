import marshal
import cPickle
from itertools import chain


class AutogenerateException(Exception):
    pass


def _all_list(a):
    return all([isinstance(x, list) for x in a])


def _included(includes):
    return [x[0] for x in includes if len(x) > 0]


def _ignored(ignores):
    return [x[0] for x in ignores if len(x) == 1]


def _shift(a):
    return [x[1:] for x in a]


def _simplesort(a):
    if type(a) is dict:
        return [(x, _simplesort(a[x])) for x in sorted(a)]
    elif type(a) is list:
        return sorted([_simplesort(x) for x in a])
    return a


def _sort(a, ignores, includes):
    if not ignores and not includes:
        return _simplesort(a)
    if type(a) is dict:
        # If there are include paths, iterate through keys in these paths.
        included_keys = _included(includes)
        if included_keys:
            return [(x, _sort(a[x], _shift(ignores), _shift(includes))) 
                for x in sorted(included_keys) if x in a]
        else:
            # Disregard keys at the top level of the ignore paths.
            ignored_keys = _ignored(ignores)
            return [(x, _sort(a[x], _shift(ignores), _shift(includes))) 
                for x in sorted(a) if x not in ignored_keys]
    elif type(a) is list:
        try:
            return sorted([_sort(x, ignores, includes) for x in a])
        except ValueError:
            return a
    return a


def _hash(a, ignores, includes):
     # Item is hashable no need to serialize.
    try:
        return hash(a)
    except:
        pass
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


def _compare_lists(a, b, ignores, includes):
    """
    Compare two lists composed of objects.

    Returns a list of objects in 'a' not contained in 'b' or in the event
    there are no common elements, an empty list.
    """
    try:
        # We don't need to worry about includes/ignores here
        # as dicts aren't hashable.
        return list(set(a) - set(b))
    except TypeError:
        # A and b are not hashable.
        pass
    hashed_a = dict([(_hash(x, ignores, includes), x) for x in a])
    hashed_b = dict([(_hash(x, ignores, includes), x) for x in b])
    # Compare the hashes to get a list of items in a that are not in b.
    return [hashed_a[x] for x in set(hashed_a) - set(hashed_b)]


def _narrow(a, b, path):
    """
    Recursively remove keys not in path. Combine lists of lists if 
    indicated by the path.
    """
    # If the path is empty, no need to narrow any further.
    # If there is nothing to narrow, no need to narrow further.
    if not path or (not a and not b):
        return a, b
    elif a.__class__ != b.__class__:
        raise TypeError("Cannot generate delta from %s to %s." % (
            a.__class__,
            b.__class__))
    key = path[0]
    if type(a) is list:
        a_dicts = [x[key] for x in a if type(x) is dict and key in x]
        b_dicts = [x[key] for x in b if type(x) is dict and key in x]
        if a_dicts or b_dicts:
            a, b = a_dicts, b_dicts
            if _all_list(a) and _all_list(b):
                a = list(chain(*a))
                b = list(chain(*b))
            return _narrow(a, b, path[1:])
        a = list(chain(*[x for x in a if type(x) is list]))
        b = list(chain(*[x for x in b if type(x) is list]))
        return _narrow(a, b, path)
    # If path exists in the dictionary, return a new dictionary of
    # that key and a recursively narrowed value, moving one path level.
    elif type(a) is dict:
        a = a.get(key) or []
        b = b.get(key) or []
        return _narrow(a, b, path[1:])
    # If a path is specified and a is not a list or a dict, return an empty
    # list. As this function is only initially called on lists, this should
    # not cause problems.
    return [], []


class Autogenerator(object):
    def __init__(self, paths=None, ignores=None, includes=None, return_new_keys=False):
        paths = self._parse_paths(paths)
        includes = self._parse_paths(includes)
        ignores = self._parse_paths(ignores)
        self.return_new_keys = return_new_keys
        if len(paths) == 0:
            # Make sure the ignores don't supersede the includes.
            for ignore in ignores:
                for include in includes:
                    if include[0:len(ignore)] == ignore:
                        raise AutogenerateException("%s ignore supersedes"
                            " %s include." % (ignore, include))
            self.paths = [{
                "path":[],
                "includes":includes,
                "ignores":ignores}]
        else:
            self.paths = []
            # Trim the include and ignores to remove items on eacb path.
            for path in paths:
                path_parameters = {"path": path}
                _includes = includes
                _ignores = ignores
                for key in path:
                    _includes = [x[1:] for x in _includes if x and x[0] == key]
                    _ignores = [x[1:] for x in _ignores if x and x[0] == key]
                # Make sure the ignores don't supersede the includes.
                for ignore in _ignores:
                    for include in _includes:
                        if include[0:len(ignore)] == ignore:
                            raise AutogenerateException("%s ignore supersedes"
                                " %s include." % (ignore, include))
                path_parameters["includes"] = _includes
                path_parameters["ignores"] = _ignores
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
        a, b = _narrow(a, b, path)
        # Native Python comparison. Should be well optimized across VMs.
        if a == b:
            return []
        # Type comparison. If they aren't the same thing, not much point in
        # going forward.
        elif a.__class__ != b.__class__:
            raise TypeError("Cannot generate delta from %s to %s." % (
                a.__class__,
                b.__class__))
        elif type(a) is list:
            return _compare_lists(a, b, ignores, includes)
        elif type(a) is dict:
            if self.return_new_keys:
                return [{x:a[x]} for x in set(a) - set(b)]
            elif _hash(a, ignores, includes) != _hash(b, ignores, includes):
                return [a]
            else:
                return []
        else:
            return [a]

    def _parse_paths(self, paths):
        if paths is None:
            return []
        if isinstance(paths, basestring):
            paths = [paths]
        elif not isinstance(paths, list):
            raise TypeError("Parameter must be str, unicode, or list.")
        # Filters a list of paths split on '/' to remove empty strings.
        return [[x for x in path.split("/") if x] for path in paths]
