package pio

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

const (
	// Value of AIO_LISTIO_MAX, which seems to be a kernel constant.
	aioListioMax = 16
)

func (op *Op) data() *byte {
	if len(op.Data) == 0 {
		return nil
	}
	return &op.Data[0]
}

func (op *Op) size() int64 {
	return int64(len(op.Data))
}

func fileMultiReadAt(f *os.File, ops []Op) {
	var buffer [aioListioMax]aiocb
	var requests [aioListioMax]*aiocb
	var operations [aioListioMax]*Op
	var fd = f.Fd()
	var pending = 0

	submit := func(i int, op *Op) error {
		r := &buffer[i]
		r.filedes = int32(fd)
		r.buf = op.data()
		r.nbytes = op.size()
		r.offset = op.Off
		requests[i] = r
		operations[i] = op

		if errno := aio_read(r); errno != 0 {
			return os.NewSyscallError("aio_read", errno)
		}

		return nil
	}

	complete := func(i int, size int, err error) {
		op := operations[i]

		if err != nil {
			op.Err = err
		} else if size < len(op.Data) {
			op.Err = io.EOF
		}

		op.Data = op.Data[:size]
	}

	for len(ops) > 0 && pending < aioListioMax {
		if err := submit(pending, &ops[0]); err != nil {
			complete(pending, 0, err)
		} else {
			pending++
		}
		ops = ops[1:]
	}

	for pending > 0 && aio_suspend(requests[:]) != syscall.EINTR {
		for i, r := range requests {
			if r == nil {
				continue
			}

			size, errno := aio_return(r)
			if errno == syscall.EINPROGRESS {
				continue
			}

			var err error
			if errno != 0 {
				err = os.NewSyscallError("aio_return", errno)
			}
			complete(i, size, err)
			requests[i] = nil
			pending--

			for len(ops) > 0 {
				op := &ops[0]
				ops = ops[1:]

				if err := submit(i, op); err != nil {
					complete(i, 0, err)
				} else {
					pending++
					break
				}
			}
		}
	}
}

// struct sigevent {
// 	int				sigev_notify;				/* Notification type */
// 	int				sigev_signo;				/* Signal number */
// 	union sigval	sigev_value;				/* Signal value */
// 	void			(*sigev_notify_function)(union sigval);	  /* Notification function */
// 	pthread_attr_t	*sigev_notify_attributes;	/* Notification attributes */
// };
type sigevent struct {
	notify           int32
	signo            int32
	value            uintptr
	notifyFunction   uintptr
	notifyAttributes uintptr
}

// struct aiocb {
// 	int		aio_fildes;		/* File descriptor */
// 	off_t		aio_offset;		/* File offset */
// 	volatile void	*aio_buf;		/* Location of buffer */
// 	size_t		aio_nbytes;		/* Length of transfer */
// 	int		aio_reqprio;		/* Request priority offset */
// 	struct sigevent	aio_sigevent;		/* Signal number and value */
// 	int		aio_lio_opcode;		/* Operation to be performed */
// };
type aiocb struct {
	filedes   int32
	offset    int64
	buf       *byte
	nbytes    int64
	reqprio   int32
	sigevent  sigevent
	lioOpcode int32
}

func aio_cancel(filedes int32, aiocb *aiocb) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_AIO_CANCEL, uintptr(filedes), uintptr(unsafe.Pointer(aiocb)), 0)
	return errno
}

func aio_read(aiocb *aiocb) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_AIO_READ, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return errno
}

func aio_return(aiocb *aiocb) (int, syscall.Errno) {
	ret, _, errno := syscall.Syscall(syscall.SYS_AIO_RETURN, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return int(ret), errno
}

func aio_error(aiocb *aiocb) syscall.Errno {
	_, _, errno := syscall.Syscall(syscall.SYS_AIO_ERROR, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return errno
}

func aio_suspend(list []*aiocb) syscall.Errno {
	if len(list) == 0 {
		return 0
	}
	p := &list[0]
	n := len(list)
	_, _, errno := syscall.Syscall(syscall.SYS_AIO_SUSPEND, uintptr(unsafe.Pointer(p)), uintptr(n), uintptr(0))
	return errno
}
