package zmqmgr

import (
	"errors"
	"fmt"
	l4g "github.com/alecthomas/log4go"
	pkgzmq "github.com/pebbe/zmq4"
	dzhyun "gw.com.cn/dzhyun/dzhyun.git"
	"sync"
	"syscall"
	"time"
)

/*****************************变量********************************/
const (
	Default_Chan_size       = 1024
	Default_Reactor_timeout = time.Millisecond * 100
)

type SNetData struct {
	Addr []byte
	Data []byte
}

type RNetData struct {
	Addr []byte
	Msg  *dzhyun.ExchInnerReqMsg
}

type INetDataProc interface {
	Proc(netData *RNetData) error
}

type GenRNetDataProc func(addr,data []byte) (*RNetData, error)


/*********************************DefaultZmqMgr***************************/
var DefaultZmqMgr ZmqMgr

func Init() error {
	err := DefaultZmqMgr.Init()
	return err
}

func Start(cmAddr string) error {
	err := DefaultZmqMgr.Start(cmAddr)
	return err
}

func Stop() error {
	err := DefaultZmqMgr.Stop()
	return err
}

func SendMsg(msg *SNetData) error {
	err := DefaultZmqMgr.SendMsg(msg)
	return err
}

func Reg(cmd int32, netDataProc INetDataProc) {
	DefaultZmqMgr.Reg(cmd, netDataProc)
}

func UnReg(cmd int32) {
	DefaultZmqMgr.UnReg(cmd)
}

func RegRNetDataGen(p GenRNetDataProc){
	DefaultZmqMgr.RegRNetDataGen(p)
}

func UseRevChan() chan *RNetData {
	ch := DefaultZmqMgr.UseRevChan()
	return ch
}

func UnUseRevChan() {
	DefaultZmqMgr.UnUseRevChan()
}

/******************************ZmqMgr*******************************/
type ZmqMgr struct {
	mRSoc      *pkgzmq.Socket //Router socket
	mReactor   *pkgzmq.Reactor
	mRcvChan   chan *RNetData                  //暴露给上层的数据接收chan
	mSndChan   chan interface{}                //内部发送chan
	mDataProc  map[int32]INetDataProc //网络Msg的处理函数
	mRNetDataGen   GenRNetDataProc         //网络数据转换成RNetData
	mRegFlag   bool                            //上层是否使用mRcvChan
	mRunFlag   bool                            //运行标志
	mThrowFlag bool                            //chan满时丢弃标志
	mWaitGroup sync.WaitGroup
}

func (this *ZmqMgr) Init() (err error) {
	this.mRcvChan = make(chan *RNetData, Default_Chan_size)
	this.mSndChan = make(chan interface{}, Default_Chan_size)
	this.mDataProc = make(map[int32]INetDataProc, 0)
	if this.mRSoc, err = pkgzmq.NewSocket(pkgzmq.ROUTER); err != nil {
		return err
	}
	return nil
}

func (this *ZmqMgr) Start(cmAddr string) (err error) {
	l4g.Info("ZmqMgr BindAddr[%s]", cmAddr)
	if this.mRNetDataGen == nil {
		return errors.New("nil RNetDataGen")
	}
	if err := this.setSocketProperty(this.mRSoc); err != nil {
		return err
	}
	if err := this.mRSoc.Bind(cmAddr); err != nil {
		return err
	}
	if err := this.run(); err != nil {
		return err
	}
	this.mRunFlag = true
	l4g.Info("ZmqMgr Start Ok")
	return nil
}

func (this *ZmqMgr) Stop() error {
	l4g.Info("ZmqMgr Stop")
	this.mRegFlag = false
	if this.mSndChan != nil {
		this.mSndChan <- nil
	}
	if this.mRSoc != nil {
		this.mRSoc.Close()
	}
	if this.mRunFlag {
		this.mWaitGroup.Wait()
	}
	if this.mRcvChan != nil {
		close(this.mRcvChan)
		this.mRcvChan = nil
	}
	if this.mSndChan != nil {
		close(this.mSndChan)
		this.mSndChan = nil
	}
	this.mRunFlag = false
	l4g.Info("ZmqMgr Stop Ok")
	return nil
}

func (this *ZmqMgr) SendMsg(sMsg *SNetData) error {
	if !this.mThrowFlag {
		this.mSndChan <- sMsg
		return nil
	}
	select {
	case this.mSndChan <- sMsg:
	default:
		return fmt.Errorf("SndChan full so throw Msg( Addr:%s)",string(sMsg.Addr))
	}
	return nil
}

func (this *ZmqMgr) Reg(cmd int32, netDataProc INetDataProc) {
	this.mDataProc[cmd] = netDataProc
}

func (this *ZmqMgr) UnReg(cmd int32) {
	delete(this.mDataProc, cmd)
}

func (this *ZmqMgr) RegRNetDataGen(p GenRNetDataProc){
	this.mRNetDataGen = p
}

func (this *ZmqMgr) UseRevChan() chan *RNetData {
	this.mRegFlag = true
	return this.mRcvChan
}

func (this *ZmqMgr) UnUseRevChan() {
	this.mRegFlag = false
}

/********************************内部接口********************************/
func (this *ZmqMgr) run() error {
	this.mReactor = pkgzmq.NewReactor()
	if this.mReactor == nil {
		return errors.New("zmq NewReactor nil")
	}
	this.mReactor.AddSocket(this.mRSoc, pkgzmq.POLLIN, this.dealRSocket)
	this.mReactor.AddChannel(this.mSndChan, 0, this.dealSndChan)

	go func() {
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		for {
			if err := this.mReactor.Run(Default_Reactor_timeout); err != nil {
				l4g.Info("Stop Reactor Because Run Err[%s]", err)
				switch pkgzmq.AsErrno(err) {
				case pkgzmq.Errno(syscall.EINTR): //被信号打断
					l4g.Info("ZmqMgr run err:%s", err.Error())
					continue
				case pkgzmq.Errno(syscall.EAGAIN): //资源不可用
					l4g.Info("ZmqMgr run err:%s", err.Error())
					continue
				}
				break
			}
		}
	}()
	return nil
}

func (this *ZmqMgr) dealSndChan(interf interface{}) error {
	switch interf.(type) {
	case nil:
		return errors.New("SndChan Recv nil")
	}
	netData, ok := interf.(*SNetData)
	if !ok {
		return errors.New("SndChan Recv Not SNetData")
	}
	if _, err := SendSocketNoEInter(this.mRSoc, []byte(netData.Addr), pkgzmq.SNDMORE); err != nil {
		l4g.Error("Send Router Soc err[%s]", err)
		return nil
	}
	if _, err := SendSocketNoEInter(this.mRSoc, netData.Data, pkgzmq.DONTWAIT); err != nil {
		l4g.Error("Send Router Soc err[%s]", err)
		return nil
	}
	return nil
}

func (this *ZmqMgr) dealRSocket(events pkgzmq.State) error {
	addr, aerr := RecvSocketNoEInter(this.mRSoc, pkgzmq.DONTWAIT)
	if aerr != nil {
		return aerr
	}
	data, derr := RecvSocketNoEInter(this.mRSoc, pkgzmq.DONTWAIT)
	if derr != nil {
		return derr
	}
	if len(data) == 0 {
		return nil
	}
	
	newMsg,err := this.mRNetDataGen(addr, data);
	if  err != nil{
		l4g.Error("RNetDataGen err[%s]", err)
		return nil
	}
	
	if this.mRegFlag { //不使用回调函数，直接返回给上层
		if !this.mThrowFlag {
			this.mRcvChan <- newMsg
			return nil
		}
		select {
		case this.mRcvChan <- newMsg:
		default:
			l4g.Warn("RcvChan full so throw Msg(Addr:%s)" ,string(newMsg.Addr))
		}
		return nil
	}
	
	iProc, pok := this.mDataProc[newMsg.Msg.Cmd]
	if !pok || iProc == nil {
		l4g.Error("Unknown Msg[Cmd:%v] err", newMsg.Msg.Cmd)
		return nil
	}
	if err := iProc.Proc(newMsg); err != nil {
		l4g.Error("Proc Msg(%v) err[%s]", newMsg.Msg.Cmd, err)
	}
	return nil
}

//设置zmqsocket属性 后续加入验证功能
func (this *ZmqMgr) setSocketProperty(socket *pkgzmq.Socket) error {
	if err := socket.SetLinger(0); err != nil {
		return err
	}
	if err := socket.SetRcvhwm(0); err != nil {
		return err
	}
	if err := socket.SetSndhwm(0); err != nil {
		return err
	}
	if err := socket.SetRcvtimeo(0); err != nil {
		return err
	}
	if err := socket.SetSndtimeo(0); err != nil {
		return err
	}
	if err := this.socMonitor(socket, "ZmqMgr"); err != nil {
		return err
	}
	return nil
}

func (this *ZmqMgr) socMonitor(monSocket *pkgzmq.Socket, socName string) error {
	addr := fmt.Sprintf("inproc://%s.monitor.inproc", socName)
	if err := monSocket.Monitor(addr, pkgzmq.EVENT_ACCEPT_FAILED|pkgzmq.EVENT_ACCEPTED|pkgzmq.EVENT_BIND_FAILED|pkgzmq.EVENT_LISTENING|pkgzmq.EVENT_CLOSED|pkgzmq.EVENT_DISCONNECTED|pkgzmq.EVENT_MONITOR_STOPPED); err != nil {
		return err
	}
	s, err := pkgzmq.NewSocket(pkgzmq.PAIR)
	if err != nil {
		return err
	}
	err = s.Connect(addr)
	if err != nil {
		return err
	}
	go func() {
		defer s.Close()
		this.mWaitGroup.Add(1)
		defer this.mWaitGroup.Done()
		for {
			a, b, c, err := s.RecvEvent(0)
			if err != nil {
				errno1 := pkgzmq.AsErrno(err)
				switch errno1 {
				case pkgzmq.Errno(syscall.EAGAIN):
					continue
				case pkgzmq.Errno(syscall.EINTR):
					continue
				default:
					l4g.Debug("zmq RecvEvent Get err %v, %d!", errno1, errno1)
				}
			}

			if c == 0 {
				continue
			}

			switch a {
			case pkgzmq.EVENT_LISTENING:
				l4g.Info("%s monitor event[%d][%s][%d] LISTENING", socName, a, b, c)
			case pkgzmq.EVENT_BIND_FAILED:
				l4g.Info("%s monitor event[%d][%s][%d] BIND_FAILED", socName, a, b, c)
				return
			case pkgzmq.EVENT_ACCEPTED:
				l4g.Info("%s monitor event[%d][%s][%d] ACCEPTED", socName, a, b, c)
			case pkgzmq.EVENT_ACCEPT_FAILED:
				l4g.Info("%s monitor event[%d][%s][%d] ACCEPT_FAILED", socName, a, b, c)
			case pkgzmq.EVENT_CONNECTED:
				l4g.Info("%s monitor event[%d][%s][%d] CONNECTED", socName, a, b, c)
			case pkgzmq.EVENT_DISCONNECTED:
				l4g.Info("%s monitor event[%d][%s][%d] DISCONNECTED", socName, a, b, c)
			case pkgzmq.EVENT_CLOSED:
				l4g.Info("%s monitor event[%d][%s][%d] CLOSED", socName, a, b, c)
				return
			case pkgzmq.EVENT_MONITOR_STOPPED:
				l4g.Info("%s monitor event[%d][%s][%d] MONITOR_STOPPED", socName, a, b, c)
				return
			default:
				l4g.Debug("%s monitor unknow event[%d][%s][%d]", socName, a, b, c)
			}
		}

	}()
	return nil
}

func SendSocketNoEInter(soc *pkgzmq.Socket, datas []byte, flags pkgzmq.Flag) (int, error) {
GOTOEINTER:
	size, err := soc.SendBytes(datas, flags)
	if err != nil {
		switch pkgzmq.AsErrno(err) {
		case pkgzmq.Errno(syscall.EINTR):
			l4g.Debug("SendSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.ENOTSOCK:
			l4g.Debug("SendSocketNoEInter err:%s", err)
			return 0, nil
			//		case pkgzmq.Errno(syscall.EAGAIN):
			//			l4g.Debug("SendSocketNoEInter err:%s", err)
			//			goto GOTOEINTER
		default:
			l4g.Info("SendSocketNoEInter send err:%s", err)
		}
	}
	return size, err
}

func RecvSocketNoEInter(soc *pkgzmq.Socket, flags pkgzmq.Flag) ([]byte, error) {
GOTOEINTER:
	datas, err := soc.RecvBytes(flags)
	if err != nil {
		switch pkgzmq.AsErrno(err) {
		case pkgzmq.Errno(syscall.EINTR): //被信号打断
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.Errno(syscall.EAGAIN): //资源不可用
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			goto GOTOEINTER
		case pkgzmq.ENOTSOCK: //socket关闭了
			l4g.Debug("RecvSocketNoEInter err:%s", err)
			return nil, nil
		default:
			l4g.Info("RecvSocketNoEInter recv err:%s", err)
		}
	}
	return datas, err
}
