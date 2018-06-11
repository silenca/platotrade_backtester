from app.services.decorators.signleton import SingletonDecorator

@SingletonDecorator
class MacdDict():
    def __init__(self):
        self.platos = dict()

    def getAll(self):
        return self.platos

    def add(self, plato):
        if plato.key() not in self.platos:
            self.platos[plato.key()] = plato

        return self;

    def remove(self, key):
        if self.has(key):
            del self.platos[key]
        return self

    def get(self, key):
        return self.platos[key] if self.has(key) else None

    def has(self, key):
        return key in self.platos
