class Bookstore:
    instances = 0
    def __init__(self, attrib1, attrib2):
        print("constructor called")
        self.attrib1 = attrib1
        self.attrib2 = attrib2
        Bookstore.instances += 1



b = Bookstore(2, 3)
b1 = Bookstore(5, 6)

print("Bookstore insatnces ", Bookstore.instances)