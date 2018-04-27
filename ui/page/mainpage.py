#--*-- coding:utf-8 --*--

from Tkinter import *
from module.client import *

def windows_center(width, height, scr_width, scr_height):
    return '%dx%d+%d+%d' % (width, height, (scr_width - width) / 2, (scr_height - height) / 2)
GlobalVal=[]

__all__ = ['ScrolledText']

from Tkinter import Frame, Text, Scrollbar, Pack, Grid, Place
from Tkconstants import RIGHT, LEFT, Y, BOTH

class ScrolledText(Text):
    def __init__(self, master=None, **kw):
        self.frame = Frame(master)
        self.vbar = Scrollbar(self.frame)
        self.vbar.pack(side=RIGHT, fill=Y)

        kw.update({'yscrollcommand': self.vbar.set})
        Text.__init__(self, self.frame, **kw)
        self.pack(side=LEFT, fill=BOTH, expand=True)
        self.vbar['command'] = self.yview

        # Copy geometry methods of self.frame without overriding Text
        # methods -- hack!
        text_meths = vars(Text).keys()
        methods = vars(Pack).keys() + vars(Grid).keys() + vars(Place).keys()
        methods = set(methods).difference(text_meths)

        for m in methods:
            if m[0] != '_' and m != 'config' and m != 'configure':
                setattr(self, m, getattr(self.frame, m))

    def __str__(self):
        return str(self.frame)





class MainGui(Tk):
    def __init__(self):
        Tk.__init__(self)
        self.title('AnalogWorld')
        self.iconbitmap('main.ico')
        self.resizable(0, 0)
        self.setLoginUI()
        # self.protocol("WM_DELETE_WINDOW", self.onClose)

        self.client=Client('127.0.0.1')
        self.client.run()

        self.mainloop()

    def setLoginUI(self):
        self.geometry(windows_center(400, 300, self.winfo_screenwidth(), self.winfo_screenheight()))
        self.frame_top = Frame(width=400, height=150, bg='#fdf5e6')
        self['bg'] = '#fdf5e6'
        self.frame_top.grid(row=0)
        self.frame_top.propagate(0)
        logo = PhotoImage(file='login_logo.gif')
        logo_label = Label(self.frame_top, image=logo)
        logo_label.image = logo
        logo_label.place(x=0,y=0, width=400, height=150)

        self.frame_center = Frame(width=400, height=60, bg = '#fdf5e6')
        self.frame_center.grid(row=1)
        self.frame_center.propagate(0)
        self.user_label = Label(self.frame_center, text='账号', bg = '#fdf5e6')
        self.user_label.place(x=70, y=10)
        self.user_Entry = Entry(self.frame_center)
        self.user_Entry.bind('<Return>', self.login)
        self.user_Entry.place(x=110, y=10, width=220)
        self.pwd_label = Label(self.frame_center, text='密码', bg = '#fdf5e6')
        self.pwd_label.place(x=70, y=35)
        self.pwd_Entry = Entry(self.frame_center, show='*')
        self.pwd_Entry.bind('<Return>', self.login)
        self.pwd_Entry.place(x=110, y=35, width=220)

        self.frame_bottom = Frame(width=400, height=75, bg='#fdf5e6')
        self.frame_bottom.grid(row=2)
        self.frame_bottom.propagate(0)
        self.tip_text = StringVar()
        self.tip_label = Label(self.frame_bottom, fg='red', textvariable=self.tip_text, bg='#fdf5e6')
        self.tip_label.place(x=100, y=0, width=200)
        self.login_button = Button(self.frame_bottom, text='登录', command=self.login, width=12,
                                   bg='#636363', fg='white')
        self.login_button.place(x=110, y=30, width=80)
        self.register_button = Button(self.frame_bottom, text='注册', command=self.register, width=12,
                                      bg='#636363', fg='white')
        self.register_button.place(x=210, y=30, width=80)

    def clearLogInGui(self):
        self.frame_top.grid_remove()
        self.frame_center.grid_remove()
        self.frame_bottom.grid_remove()


    def login(self, event=None):
        self.login_button['state'] = 'disabled'
        if self.user_Entry.get() == '':
            self.tip_text.set('账号不能为空')
        elif self.pwd_Entry.get() == '':
            self.tip_text.set('密码不能为空')
        else:
            self.tip_text.set('')
            username = self.user_Entry.get()
            pwd = self.pwd_Entry.get()
            try:
                self.client.send_msg_once('01', '%s %s' % (username, pwd))
            except:
                self.tip_text.set('服务器未响应')
        self.login_button['state'] = 'normal'

    def reactLogin(self):
        data='login '
        username = self.user_Entry.get()
        pwd = self.pwd_Entry.get()
        if not data:
            self.tip_text.set('服务器未响应')
        elif data == "login successfully":
            self.setLobbyUI()
        elif data == ("%s already online" % GlobalVal.user_name):
            self.tip_text.set('该用户已上线')
        elif data == "password incorrect":
            self.tip_text.set('密码错误')
        elif data[:2] == 's@':
            GlobalVal.sockt.send('01', '%s %s' % (username, pwd))
        else:
            print data
            self.tip_text.set('账号不存在')

    def recvOneMsg(self):
        GlobalVal.sockt.runOnce()
        self.after(100, self.recvOneMsg)

    def setLobbyUI(self):
        self.clearLogInGui()
        self['bg'] = '#708090'
        self.title('VV聊天大厅')
        self.geometry(windows_center(830, 600, self.winfo_screenwidth(), self.winfo_screenheight()))
        self.press_send = False
        self.frame_left_top = Frame(self, width=650, height=460)
        self.frame_left_top.grid(row=0, column=0, padx=2, pady=2)
        self.frame_left_top.grid_propagate(0)
        self.text_msglist = ScrolledText(self.frame_left_top, borderwidth=1, highlightthickness=0,
                                         state=DISABLED, relief='flat', bg='#fffff0')
        self.text_msglist.tag_config('myTitle', foreground='#008B00', justify='right', font=("Times, 11"))
        self.text_msglist.tag_config('myContent', justify='right', font=("Courier, 11"))
        self.text_msglist.tag_config('otherTitle', foreground='blue', justify='left', font=("Times, 11"))
        self.text_msglist.tag_config('otherContent', justify='left', font=("Courier, 11"))
        self.text_msglist.tag_config('notice', foreground='gray', justify='center', font=("Times, 9"))
        self.text_msglist.place(x=0, y=0, width=650, height=460)

        self.frame_left_center = Frame(self, width=650, height=100)
        self.frame_left_center.grid(row=1, column=0, padx=2, pady=2)
        self.text_msg = Text(self.frame_left_center, highlightthickness=0, relief='flat', bg='#fffff0')
        self.text_msg.place(x=0, y=0, width=650, height=100)
        #self.text_msg.grid(sticky='WE')
        self.text_msg.bind('<Shift-Return>', self.sendMsg)
        self.text_msg.bind('<KeyRelease-Return>', self.clearMsg)

        self.frame_left_bottom = Frame(self, width=650, height=30, bg = '#708090')
        self.frame_left_bottom.grid(row=2, column=0)
        self.frame_left_bottom.grid_propagate(0)
        self.send_msg_btn = Button(self.frame_left_bottom, text='发送(shift+enter)', bg = '#636363', fg='white')
        self.send_msg_btn.bind('<Button-1>', self.sendMsg)
        self.send_msg_btn.bind('<ButtonRelease-1>', self.clearMsg)
        self.send_msg_btn.place(x=540, y=2, height=23, width=110)
        self.send_msg_label = Label(self.frame_left_bottom, text='', fg='white', bg = '#708090')
        self.send_msg_label.place(x=10, y=5, height=20, width=200)

        self.frame_right = Frame(width=180, height=600, bg = '#708090')
        self.frame_right.grid(row=0, column=1, rowspan=3)
        self.frame_right.grid_propagate(0)
        self.time_label = Label(self.frame_right, bg='gray', justify='left')
        self.time_label.place(x=0, y=0, width=180)
        self.tip_label = Label(self.frame_right, justify='left', text='大厅用户 (0)', bg='#708090', fg='#fffafa')
        self.tip_label.place(x=50, y=100)
        self.user_list = StringVar()
        self.user_list_box = Listbox(self.frame_right, borderwidth=1, highlightthickness=0,
                                     relief='flat', bg='#ededed', listvariable=self.user_list)
        self.user_list_box.place(x=5, y=125, height=470, width=165)
        self.user_list_box.bind('<Double-Button-1>', self.privateChat)
        GlobalVal.sockt.send('09', '')
        self.add_room_btn = Button(self.frame_right, text='创建房间', command=self.createRoom, bg = '#636363', fg='white')
        self.add_room_btn.place(x=15, y=65)
        self.enter_room_btn = Button(self.frame_right, text='加入房间', command=self.enterRoom, bg = '#636363', fg='white')
        self.enter_room_btn.place(x=100, y=65)

    def sendMsg(self, event=None):
        if self.press_send:
            self.text_msg.delete(0.0, END)
        else:
            self.press_send = True
            msg = self.text_msg.get('0.0', END)
            if msg == '\n':
                self.send_msg_label['text'] = "发送内容不能为空，请重新输入"
            else:
                msg_title = '%s (%s)\n' % (GlobalVal.user_name, time.strftime("%m-%d %H:%M:%S", time.localtime()))
                self.text_msglist.config(state=NORMAL)
                self.text_msglist.insert(END, msg_title, 'myTitle')
                GlobalVal.sockt.send('02', msg)
                self.text_msglist.insert(END, msg, 'myContent')
                self.text_msglist.see(END)
                self.text_msg.delete(0.0, END)
                self.text_msglist.config(state=DISABLED)

    def clearMsg(self, event=None):
        if self.press_send:
            self.press_send = False
            self.text_msg.delete(0.0, END)
            self.send_msg_label['text'] = ""

    def recvMsg(self, username, msg):
        msg_title = '(%s) %s\n' % (time.strftime("%m-%d %H:%M:%S", time.localtime()), username)
        self.text_msglist.config(state=NORMAL)
        self.text_msglist.insert(END, msg_title, 'otherTitle')
        self.text_msglist.insert(END, msg + '\n', 'otherContent')
        self.text_msglist.see(END)
        self.text_msglist.config(state=DISABLED)

    def recvNotice(self, msg):
        try:
            self.text_msglist.config(state=NORMAL)
            self.text_msglist.insert(END, msg + '\n', 'notice')
            self.text_msglist.see(END)
            self.text_msglist.config(state=DISABLED)
        except Exception, e:
            pass

    def updateTimeLabel(self, cur, total):
        try:
            self.time_label['text'] = '用户名:  %s\n本次在线时长:  %s\n总在线时长:  %s' %\
                                      (GlobalVal.user_name, cur, total)
        except Exception, e:
            pass

    def updateUserList(self, user_list):
        user_list = tuple(user_list)
        self.tip_label['text'] = '大厅用户 (%d)' % len(user_list)
        self.user_list.set(user_list)


def main():
    MainGui()


if __name__ == '__main__':
    main()