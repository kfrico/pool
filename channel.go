package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// 配置連接池相關配置
type Config struct {
	// 連接池中擁有的最小連接數
	InitialCap int
	// 連接池中擁有的最大的連接數
	MaxCap int
	// 生成連接的方法
	Factory func() (interface{}, error)
	// 關閉連接的方法
	Close func(interface{}) error
	// 檢查連接是否有效的方法
	Ping func(interface{}) error
	// 連接最大最大值時間，超過該事件則將無效
	IdleTimeout time.Duration
}

// channelPool存放連接信息
type channelPool struct {
	mu          sync.Mutex
	conns       chan *idleConn
	factory     func() (interface{}, error)
	close       func(interface{}) error
	ping        func(interface{}) error
	idleTimeout time.Duration
}

type idleConn struct {
	conn interface{}
	t    time.Time
}

// NewChannelPool初始化連接
func NewChannelPool(poolConfig *Config) (Pool, error) {
	if poolConfig.InitialCap < 0 || poolConfig.MaxCap <= 0 || poolConfig.InitialCap > poolConfig.MaxCap {
		return nil, errors.New("invalid capacity settings")
	}

	if poolConfig.Factory == nil {
		return nil, errors.New("invalid factory func settings")
	}

	if poolConfig.Close == nil {
		return nil, errors.New("invalid close func settings")
	}

	c := &channelPool{
		conns:       make(chan *idleConn, poolConfig.MaxCap),
		factory:     poolConfig.Factory,
		close:       poolConfig.Close,
		idleTimeout: poolConfig.IdleTimeout,
	}

	if poolConfig.Ping != nil {
		c.ping = poolConfig.Ping
	}

	for i := 0; i < poolConfig.InitialCap; i++ {
		conn, err := c.factory()
		if err != nil {
			c.Release()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &idleConn{conn: conn, t: time.Now()}
	}

	return c, nil
}

// getConns獲取所有連接
func (c *channelPool) getConns() chan *idleConn {
	c.mu.Lock()
	conns := c.conns
	c.mu.Unlock()

	return conns
}

// 獲取從池中取一個連接
func (c *channelPool) Get() (interface{}, error) {
	conns := c.getConns()
	if conns == nil {
		return nil, ErrClosed
	}

	for {
		select {
		case wrapConn := <-conns:
			if wrapConn == nil {
				return nil, ErrClosed
			}
			// 判斷是否超時，超時則最大化
			if timeout := c.idleTimeout; timeout > 0 {
				if wrapConn.t.Add(timeout).Before(time.Now()) {
					// 展開並關閉該連接
					c.Close(wrapConn.conn)
					continue
				}
			}
			// 判斷是否存在錯誤，是否可以替換，如果用戶沒有設置ping方法，就不檢查
			if c.ping != nil {
				if err := c.Ping(wrapConn.conn); err != nil {
					fmt.Println("conn is not able to be connected: ", err)
					continue
				}
			}

			return wrapConn.conn, nil
		default:
			c.mu.Lock()
			if c.factory == nil {
				c.mu.Unlock()
				continue
			}

			conn, err := c.factory()
			c.mu.Unlock()

			if err != nil {
				return nil, err
			}

			return conn, nil
		}
	}
}

// 將將連接放回pool中
func (c *channelPool) Put(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()

	if c.conns == nil {
		c.mu.Unlock()
		return c.Close(conn)
	}

	select {
	case c.conns <- &idleConn{conn: conn, t: time.Now()}:
		c.mu.Unlock()
		return nil
	default:
		c.mu.Unlock()
		// 連接池已滿，直接關閉該連接
		return c.Close(conn)
	}
}

// 關閉關閉單條連接
func (c *channelPool) Close(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.close == nil {
		return nil
	}

	return c.close(conn)
}

// Ping檢查單條連接是否有效
func (c *channelPool) Ping(conn interface{}) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	return c.ping(conn)
}

// 發布釋放連接池中所有連接
func (c *channelPool) Release() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	c.factory = nil
	c.ping = nil
	closeFun := c.close
	c.close = nil
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)

	for wrapConn := range conns {
		_ = closeFun(wrapConn.conn)
	}
}

// Len連接池中已有的連接
func (c *channelPool) Len() int {
	return len(c.getConns())
}
