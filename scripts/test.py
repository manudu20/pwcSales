import os
print(os.path.abspath(os.getcwd()))
#print(os.path.pardir(os.getcwd()))
print(os.path.join(os.path.abspath(os.path.join(os.getcwd(), os.pardir)),'tests'))