#--*-- coding:utf-8 --*--
import time

INTERVALS=6
class Dream(object):
    def __init__(self):
        pass

    def run(self):
        while True:
            time.sleep(INTERVALS)
            print 'your dream is running'


def main():
    filenum=15
    threadnum=10
    cou=filenum%threadnum
    print cou

if __name__ == '__main__':
    main()