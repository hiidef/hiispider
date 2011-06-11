
import simplejson


class Delta(object):
    """
    Delta base class. Stores arbitrary kwargs and provides an interface
    for the spider.
    """
    def __init__(self, **kwargs):
        self.data = kwargs
      
    def dumps(self):
        return simplejson.dumps(kwargs)
        
        
class MP3(Delta):
    """ MP3 Delta. Stores data related to audio files."""
    def __init__(self, 
            mp3_url,
            title=None,
            description=None,
            image_url=None):
        kwargs = {"mp3_url":mp3_url}
        if title is not None:
            kwargs["title"] = title
        if description is not None:
            kwargs["description"] = description
        if image_url is not None:
            kwargs["image_url"] = image_url
        Delta.__init__(self, **kwargs)

 
class SWF(Delta):
    """ SWF Delta. Stores data related to SWF files. """
    def __init__(self,
            swf_url
            title=None,
            description=None,
            width=None,
            height=None):
        kwargs = {"mp3_url":mp3_url}
        if title is not None:
            kwargs["title"] = title
        if description is not None:
            kwargs["description"] = description
        if width is not None:
            kwargs["width"] = width
        if height is not None:
            kwargs["height"] = height
        Delta.__init__(self, **kwargs)


class Status(Delta):
    """ Status Delta. Stores a description. """
    def __init__(self, description):
        Delta.__init__(self, description=description)

    
class Post(Delta):
    """ Post Delta. """
    def __init__(self, title, description):
        Delta.__init__(self, title=title, description=description)


class Image(Delta):
    """ Image Delta. """
    def __init__(self, 
            image_url,
            title=None,
            description=None,
            width=None,
            height=None):
        kwargs = {"image_url":image_url}
        if title is not None:
            kwargs["title"] = title
        if description is not None:
            kwargs["description"] = description
        if width is not None:
            kwargs["width"] = width
        if height is not None:
            kwargs["height"] = height
        Delta.__init__(self, **kwargs)
        