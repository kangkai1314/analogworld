#--*-- coding:utf-8 --*--
import tornado.ioloop
import tornado.web

class IndexHandler(tornado.web.RequestHandler):
    def get(self, *args, **kwargs):
        self.write('this is your first page')


if __name__ == '__main__':
    app=tornado.web.Application([(r'/',IndexHandler)])
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
