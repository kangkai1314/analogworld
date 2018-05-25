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
            print 'this is your best chance '
            print 'just keep it ,hurry up'




def main():

    d=Dream()
    d.run()

if __name__ == '__main__':
    main()