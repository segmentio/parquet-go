package pio

import (
	"io"
	"os"
	"syscall"
	"unsafe"
)

func (op *Op) data() *byte {
	if len(op.Data) == 0 {
		return nil
	}
	return &op.Data[0]
}

func (op *Op) size() uint64 {
	return uint64(len(op.Data))
}

func fileMultiReadAt(f *os.File, ops []Op) {
	if len(ops) == 0 {
		return
	}

	abort := func(err error) {
		for i := range ops {
			op := &ops[i]
			op.Data, op.Err = op.Data[:0], err
		}
	}

	ctx, errno := io_setup(len(ops))
	if errno != 0 {
		abort(os.NewSyscallError("io_setup", errno))
		return
	}
	defer io_destroy(ctx)

	const stackBufferSize = 64
	ptr := make([]*iocb, 0, stackBufferSize)
	req := make([]iocb, 0, stackBufferSize)
	res := make([]io_event, 0, stackBufferSize)

	fd := f.Fd()
	for i := range ops {
		op := &ops[i]
		res = append(res, io_event{})
		req = append(req, iocb{
			data:    uint64(i),
			opcode:  pread,
			filedes: uint32(fd),
			buf:     op.data(),
			nbytes:  op.size(),
			offset:  op.Off,
		})
		ptr = append(ptr, &req[i])
	}

	pending := 0
	for {
		if len(ptr) > 0 {
			if errno := io_submit(ctx, ptr); errno != 0 {
				// When resubmitting the operations fail, we can afford to
				// only abort these operations and let the other complete.
				err := os.NewSyscallError("io_submit", errno)

				for _, p := range ptr {
					ops[p.data].Err = err
				}
			} else {
				pending += len(ptr)
				ptr = ptr[:0]
			}
		}

		if pending == 0 {
			return
		}

		n, errno := io_getevents(ctx, res)
		switch errno {
		case 0:
			for i := range res[:n] {
				r := &res[i]
				op := &ops[r.data]
				rn := int(r.res) + byteDist(op.data(), req[r.data].buf)

				// According to https://lkml.iu.edu/hypermail/linux/kernel/0304.3/1296.html
				// the `io_event.res` field will be negative value holding the
				// error code on failure.
				if r.res < 0 {
					op.Err = os.NewSyscallError("pread", syscall.Errno(-r.res))
				} else if rn < len(op.Data) {
					op.Err = io.EOF
					op.Data = op.Data[:rn]
				}

				pending--
			}

		case syscall.EINTR:
			// If the syscall is interrupted by a signal, some operations may
			// have only partially completed. Resubmit.
			for i := range res[:n] {
				r := &res[i]
				p := &req[r.data]
				op := &ops[r.data]
				rn := int(r.res) + byteDist(op.data(), p.buf)

				if r.res < 0 {
					op.Err = os.NewSyscallError("pread", syscall.Errno(-r.res))
				} else if rn < len(op.Data) {
					// We don't know whether we reached the end of the file or
					// the read was interrupted by a signal handler, so we must
					// resubmit the operation until io_getevents returns
					// successfully.
					//
					// We adjust the buffer's base pointer, offset and length
					// to account for the data that may have already been read.
					p.buf = byteAdd(p.buf, int(r.res))
					p.nbytes -= uint64(r.res)
					p.offset += int64(r.res)
					ptr = append(ptr, p)
					continue
				}

				pending--
			}

		default:
			abort(os.NewSyscallError("io_getevents", errno))
			return
		}
	}
}

const (
	pread   = 0
	pwrite  = 1
	fsync   = 2
	fdsync  = 3
	poll    = 5
	noop    = 6
	preadv  = 7
	pwritev = 8
)

// struct iocb {
//     __u64   aio_data;
//     __u32   PADDED(aio_key, aio_rw_flags);
//     __u16   aio_lio_opcode;
//     __s16   aio_reqprio;
//     __u32   aio_fildes;
//     __u64   aio_buf;
//     __u64   aio_nbytes;
//     __s64   aio_offset;
//     __u64   aio_reserved2;
//     __u32   aio_flags;
//     __u32   aio_resfd;
// }
type iocb struct {
	data    uint64
	key     uint32
	rwflags uint32
	opcode  uint16
	reqprio int16
	filedes uint32
	buf     *byte
	nbytes  uint64
	offset  int64
	_       uint64
	flags   uint32
	resfd   uint32
}

// struct io_event {
//         __u64           data;           /* the data field from the iocb */
//         __u64           obj;            /* what iocb this event came from */
//         __s64           res;            /* result code for this event */
//         __s64           res2;           /* secondary result */
// };
type io_event struct {
	data uint64
	obj  uint64
	res  int64
	res2 int64
}

type io_context_t uintptr

func io_setup(nrEvents int) (io_context_t, syscall.Errno) {
	ctx := io_context_t(0)
	_, _, errno := syscall.RawSyscall(syscall.SYS_IO_SETUP, uintptr(nrEvents), uintptr(unsafe.Pointer(&ctx)), 0)
	return ctx, errno
}

func io_destroy(ctx io_context_t) syscall.Errno {
	_, _, errno := syscall.RawSyscall(syscall.SYS_IO_DESTROY, uintptr(ctx), 0, 0)
	return errno
}

func io_submit(ctx io_context_t, reqs []*iocb) syscall.Errno {
	p := unsafe.Pointer(&reqs[0])
	n := len(reqs)
	_, _, errno := syscall.Syscall(syscall.SYS_IO_SUBMIT, uintptr(ctx), uintptr(n), uintptr(p))
	return errno
}

func io_getevents(ctx io_context_t, events []io_event) (int, syscall.Errno) {
	p := unsafe.Pointer(&events[0])
	n := len(events)
	r, _, errno := syscall.Syscall6(syscall.SYS_IO_GETEVENTS, uintptr(ctx), uintptr(n), uintptr(n), uintptr(p), 0, 0)
	return int(r), errno
}

const sizeOfIocb = unsafe.Sizeof(iocb{})

// Compile-time check that iocb is a 64 bytes structure.
var _ = ([sizeOfIocb]byte{}) == ([64]byte{})

func byteAdd(base *byte, nbytes int) *byte {
	return (*byte)(unsafe.Add(unsafe.Pointer(base), nbytes))
}

func byteDist(from, to *byte) int {
	return int(uintptr(unsafe.Pointer(to)) - uintptr(unsafe.Pointer(from)))
}
