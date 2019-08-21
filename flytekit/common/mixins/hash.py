from __future__ import absolute_import


class HashOnReferenceMixin(object):
    def __hash__(self):
        return hash(id(self))
