import sys
__all__ = ['Const']


class Const:
    class ConstError(TypeError):
        pass

    def __setattr__(self, key, value):
        # self.__dict__
        if key in self.__dict__:
            raise self.ConstError("constant reassignment error!")
        self.__dict__[key] = value


# sys.modules[__name__] = Const()


