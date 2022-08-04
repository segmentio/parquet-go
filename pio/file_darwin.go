package pio

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

const (
	aioListioMax = 16
)

func fileMultiReadAt(f *os.File, ops []Op) {
	var buf [aioListioMax]aiocb
	var req [aioListioMax]*aiocb
	var fd = f.Fd()

	for i := 0; i < len(ops); {
		j := len(ops)
		n := len(ops) - i

		if n > aioListioMax {
			j = aioListioMax + i
			n = aioListioMax
		}

		list := ops[i:j]
		wait := 0

		for k, op := range list {
			r := &buf[k]
			r.filedes = int32(fd)
			r.offset = op.Off
			r.buf = nil
			r.nbytes = 0

			if len(op.Data) > 0 {
				r.buf = &op.Data[0]
				r.nbytes = int64(len(op.Data))
			}

			switch err := aio_read(r); err {
			case 0:
				req[k] = r
				wait++
			default:
				list[k].Err = os.NewSyscallError("aio_read", err)
				req[k] = nil
			}
		}

		for wait > 0 && aio_suspend(req[:n]) != syscall.EINTR {
			for k, r := range req[:n] {
				if r != nil {
					if size, errno := aio_return(r); errno != syscall.EINPROGRESS {
						op := &list[k]
						if errno != 0 {
							op.Err = os.NewSyscallError("aio_error", errno)
						} else if size < len(op.Data) {
							op.Err = io.EOF
						}
						op.Data = op.Data[:size]
						req[k] = nil
						wait--
					}
				}
			}
		}

		i += len(list)
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
	_, _, errno := syscall.RawSyscall(syscall.SYS_AIO_CANCEL, uintptr(filedes), uintptr(unsafe.Pointer(aiocb)), 0)
	return syscall.Errno(errno)
}

func aio_read(aiocb *aiocb) syscall.Errno {
	_, _, errno := syscall.RawSyscall(syscall.SYS_AIO_READ, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return syscall.Errno(errno)
}

func aio_return(aiocb *aiocb) (int, syscall.Errno) {
	ret, _, errno := syscall.RawSyscall(syscall.SYS_AIO_RETURN, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return int(ret), syscall.Errno(errno)
}

func aio_error(aiocb *aiocb) syscall.Errno {
	_, _, errno := syscall.RawSyscall(syscall.SYS_AIO_ERROR, uintptr(unsafe.Pointer(aiocb)), 0, 0)
	return syscall.Errno(errno)
}

func aio_suspend(list []*aiocb) syscall.Errno {
	if len(list) == 0 {
		return 0
	}
	p := &list[0]
	n := len(list)
	_, _, errno := syscall.Syscall(syscall.SYS_AIO_SUSPEND, uintptr(unsafe.Pointer(p)), uintptr(n), uintptr(0))
	return syscall.Errno(errno)
}
