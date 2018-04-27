#--*-- coding:utf-8 --*--
from PIL import Image
im=Image.open('1.jpg')
print im.format
print im.size
print im.mode
im.show()