package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
)

// proxy2TiDBConn represents a connection from the proxy to a TiDB server.
type proxy2TiDBConn struct {
	addr      string
	conn      net.Conn
	pkg       *PacketIO
	resultBuf []byte
}

func newProxy2TiDBConn(addr string) (*proxy2TiDBConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &proxy2TiDBConn{
		addr: addr,
		conn: conn,
		pkg:  NewPacketIO(conn),
	}, nil
}

func (c *proxy2TiDBConn) Close() error {
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
		c.conn = nil
	}
	return nil
}

func (c *proxy2TiDBConn) forward(data []byte) (result []byte, err error) {
	if err = c.writePacket(data); err != nil {
		return nil, err
	}
	return c.readResult(c.resultBuf)
}

func (c *proxy2TiDBConn) readResult(resultBuf []byte) ([]byte, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	resultBuf = resultBuf[:0]
	if data[0] == OK_HEADER {
		return c.handleOKPacket(data, resultBuf)
	} else if data[0] == ERR_HEADER {
		return c.handleErrorPacket(data, resultBuf)
	} else if data[0] == LocalInFile_HEADER {
		return nil, errors.New("local-infile not supported")
	}

	return c.readResultset(data, resultBuf)
}

func (c *proxy2TiDBConn) handleOKPacket(data, resultBuf []byte) ([]byte, error) {
	resultBuf = append(resultBuf, data[1:]...)
	return resultBuf, nil
}

func (c *proxy2TiDBConn) handleErrorPacket(data, resultBuf []byte) ([]byte, error) {
	resultBuf = append(resultBuf, data[1:]...)
	return resultBuf, nil
}

func (c *proxy2TiDBConn) readResultset(data, resultBuf []byte) ([]byte, error) {
	resultBuf = append(resultBuf, data...)

	var err error
	if resultBuf, err = c.readUntilEOF(resultBuf); err != nil { // result column info
		return nil, err
	}
	if resultBuf, err = c.readUntilEOF(resultBuf); err != nil { // row data
		return nil, err
	}
	return resultBuf, nil
}

func (c *proxy2TiDBConn) readUntilEOF(resultBuf []byte) ([]byte, error) {
	for {
		data, err := c.readPacket()
		if err != nil {
			return nil, err
		}
		resultBuf = append(resultBuf, data...)
		// EOF Packet
		if c.isEOFPacket(data) {
			return resultBuf, nil
		}
	}
}

func (c *proxy2TiDBConn) isEOFPacket(data []byte) bool {
	return data[0] == EOF_HEADER && len(data) <= 5
}

func (c *proxy2TiDBConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *proxy2TiDBConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

const (
	defaultReaderSize = 8 * 1024

	MaxPayloadLen int    = 1<<24 - 1
	ServerVersion string = "tidb"
)

const (
	OK_HEADER          byte = 0x00
	ERR_HEADER         byte = 0xff
	EOF_HEADER         byte = 0xfe
	LocalInFile_HEADER byte = 0xfb
)

var ErrBadConn = errors.New("connection was bad")

type PacketIO struct {
	rb *bufio.Reader
	wb io.Writer

	Sequence uint8
}

func NewPacketIO(conn net.Conn) *PacketIO {
	p := new(PacketIO)

	p.rb = bufio.NewReaderSize(conn, defaultReaderSize)
	p.wb = conn

	p.Sequence = 0

	return p
}

func (p *PacketIO) ReadPacket() ([]byte, error) {
	header := []byte{0, 0, 0, 0}

	if _, err := io.ReadFull(p.rb, header); err != nil {
		return nil, ErrBadConn
	}

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)
	if length < 1 {
		return nil, fmt.Errorf("invalid payload length %d", length)
	}

	sequence := uint8(header[3])

	if sequence != p.Sequence {
		return nil, fmt.Errorf("invalid sequence %d != %d", sequence, p.Sequence)
	}

	p.Sequence++

	data := make([]byte, length)
	if _, err := io.ReadFull(p.rb, data); err != nil {
		return nil, ErrBadConn
	} else {
		if length < MaxPayloadLen {
			return data, nil
		}

		var buf []byte
		buf, err = p.ReadPacket()
		if err != nil {
			return nil, ErrBadConn
		} else {
			return append(data, buf...), nil
		}
	}
}

// data already have header
func (p *PacketIO) WritePacket(data []byte) error {
	length := len(data) - 4

	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence

		if n, err := p.wb.Write(data[:4+MaxPayloadLen]); err != nil {
			return ErrBadConn
		} else if n != (4 + MaxPayloadLen) {
			return ErrBadConn
		} else {
			p.Sequence++
			length -= MaxPayloadLen
			data = data[MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	if n, err := p.wb.Write(data); err != nil {
		return ErrBadConn
	} else if n != len(data) {
		return ErrBadConn
	} else {
		p.Sequence++
		return nil
	}
}

func (p *PacketIO) WritePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	if data == nil {
		//only flush the buffer
		if direct == true {
			n, err := p.wb.Write(total)
			if err != nil {
				return nil, ErrBadConn
			}
			if n != len(total) {
				return nil, ErrBadConn
			}
		}
		return total, nil
	}

	length := len(data) - 4
	for length >= MaxPayloadLen {

		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.Sequence
		total = append(total, data[:4+MaxPayloadLen]...)

		p.Sequence++
		length -= MaxPayloadLen
		data = data[MaxPayloadLen:]
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.Sequence

	total = append(total, data...)
	p.Sequence++

	if direct {
		if n, err := p.wb.Write(total); err != nil {
			return nil, ErrBadConn
		} else if n != len(total) {
			return nil, ErrBadConn
		}
	}
	return total, nil
}
