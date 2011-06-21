import marshal
import cPickle
from collections import Hashable


def _convert_to_hashed_dict(a):
    """
    Convert a list into a dictionary suitable for comparison of structured
    data types.
    """
    hashed = {}
    for x in a:
        # Item is hashable. No need to serialize.
        if isinstance(x, Hashable):
            hashed[hash(x)] = x
        else:
            try:
                # Marshal is fast. Try it first.
                hashed[hash(marshal.dumps(x))] = x
            except ValueError:
                # Try cPickle.
                hashed[hash(cPickle.dumps(x))] = x
    return hashed


def _compare_lists(a, b):
    """
    Compare two lists composed of objects.
    
    Returns a list of objects in 'a' not contained in 'b' or in the event
    there are no common elements, an empty list.
    """
    try:
        return list(set(a) - set(b))
    except TypeError, e:
        # A and b are not hashable.
        pass
    hashed_a = _convert_to_hashed_dict(a)
    hashed_b = _convert_to_hashed_dict(b)
    # Compare the hashes to get a list of items in a that are not in b.
    return [hashed_a[x] for x in set(hashed_a.keys()) - set(hashed_b.keys())]


def _compare_dicts(a, b):
    """
    Compare two dictonaries composed of objects.
    
    Returns a list of (key, value) pairs in 'a' not contained in 'b' or in the
    event there are no common elements, an empty list.
    """
    values = []
    for key in a:
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
                deltas = _compare_lists(a[key], b[key])
                if len(deltas) > 0:  
                    values.append({key:deltas})
                continue
            elif isinstance(a[key], dict):
                # Return new items in dicts at key.
                delta = {}
                for x in _compare_dicts(a[key], b[key]):
                    delta.update(x)
                if len(delta) > 0:
                    values.append({key:delta})
                continue
            if hash(cPickle.dumps(a[key])) != hash(cPickle.dumps(b[key])):
                values.append({key:a[key]})       
        else:
            values.append({key:a[key]})
    return values
    
def autogenerate(a, b):
    """
    Compare dictionaries or lists of objects. Returns a list.
    """
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
        return _compare_lists(a, b)
    if isinstance(a, dict):
        return _compare_dicts(a, b)
    raise TypeError("Cannot generate delta from %s." % a.__class__)
