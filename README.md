# zmqmgr
zeromq  router服务端消息处理封装
# 接口介绍
  
  func (this *ZmqMgr) Start(cmAddr string) (err error)  
  func (this *ZmqMgr) SendMsg(sMsg *SNetData) error  //发送消息  
  func (this *ZmqMgr) Reg(cmd int32, netDataProc INetDataProc) //注册回调函数
# 依赖
  
  "github.com/alecthomas/log4go"
  
  "github.com/pebbe/zmq4"
  
  "gw.com.cn/dzhyun/dzhyun.git" //消息类型，可用interface{}代替
