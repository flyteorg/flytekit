class HashOnReferenceMixin(object):
    def __hash__(self):
        return hash(id(self))
