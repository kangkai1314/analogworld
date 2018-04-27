#--*-- coding:utf-8 --*--
'''
code version control
use git cmd to get newest version
current version  is not newlst version
prompt user to check version
'''
class Version():
    version_code=None
    def __init__(self):
        self._version=None
        self.update_time=None
        self.executor=None
        self.updateFlag=False


    def check_version(self):
        version=self.get_version()
        if self.updateFlag:
            print 'code is not lastest curr version is {},newlst version is {}'.format(self._version,version)

    def get_version(self):
        return self.version_code


    def update_code(self):
        pass


def main():
    list1=[2,2,3,4,6,7]
    list2=[1,2,3,4,5]
    print (list1 and list2)

if __name__ == '__main__':
    main()




