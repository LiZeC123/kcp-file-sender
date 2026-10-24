package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

const (
	defaultPort      = 13080
	defaultFECData   = 0 // 由于丢包率过高, 因此不开启FEC
	defaultFECParity = 0
	defaultInterval  = 10
	defaultResend    = 2
	defaultNoDelay   = 1
	defaultNC        = 1
	defaultWindow    = 1024
	defaultMTU       = 1400

	maxNameLen   = 65535
	maxFileCount = 1000000

	// 客户端在收完所有文件并写盘后发送的确认字节
	ackDone = 'D'
	// 服务端等待客户端完成确认的超时
	serverAckTimeout = 120 * time.Second
	// 服务端读取客户端请求 (请求头 + 文件序号 + offset) 的超时
	serverRequestTimeout = 120 * time.Second
	// 客户端在 recvFile 中的单次读超时, 避免服务端异常时永久阻塞
	clientReadTimeout = 120 * time.Second
)

// ---------------- 参数 ----------------

type KCPConfig struct {
	FECData   int
	FECParity int
	NoDelay   int
	Interval  int
	Resend    int
	NC        int
	Window    int
	MTU       int
}

func (c KCPConfig) apply(conn *kcp.UDPSession) {
	conn.SetNoDelay(c.NoDelay, c.Interval, c.Resend, c.NC)
	conn.SetWindowSize(c.Window, c.Window)
	conn.SetMtu(c.MTU)
}

type FileEntry struct {
	RelPath string // 相对路径, 斜杠分隔
	AbsPath string // 服务端本地绝对路径
	Size    int64
}

// fileReq 客户端请求中的单条记录
type fileReq struct {
	Seq    uint32
	Offset int64
}

// pending 客户端待下载文件的本地状态
type pending struct {
	Seq       uint32
	Offset    int64
	LocalPath string
	Size      int64
	RelPath   string
}

func main() {
	mode := flag.String("mode", "", "server 或 client")

	// server
	rootPath := flag.String("path", "", "server: 扫描的根路径")
	minSizeMiB := flag.Int64("minsize", 0, "server: 最小文件大小 (MiB)")
	port := flag.Int("port", defaultPort, "监听/连接端口")

	// client
	serverHost := flag.String("server", "", "client: 服务端地址 (IP 或域名)")
	outDir := flag.String("out", ".", "client: 保存目录")

	// KCP 参数
	fecData := flag.Int("fec-data", defaultFECData, "FEC 数据分片数")
	fecParity := flag.Int("fec-parity", defaultFECParity, "FEC 纠错分片数")
	interval := flag.Int("interval", defaultInterval, "KCP 内部 tick (ms)")
	resend := flag.Int("resend", defaultResend, "快速重传阈值")
	noDelay := flag.Int("nodelay", defaultNoDelay, "nodelay 模式")
	nc := flag.Int("nc", defaultNC, "拥塞控制 (1=关闭)")
	window := flag.Int("window", defaultWindow, "收发窗口大小 (分片数)")
	mtu := flag.Int("mtu", defaultMTU, "MTU")

	flag.Parse()

	cfg := KCPConfig{
		FECData:   *fecData,
		FECParity: *fecParity,
		NoDelay:   *noDelay,
		Interval:  *interval,
		Resend:    *resend,
		NC:        *nc,
		Window:    *window,
		MTU:       *mtu,
	}

	switch *mode {
	case "server":
		if *rootPath == "" {
			log.Fatal("server 模式需要 -path 参数")
		}
		runServer(*rootPath, *minSizeMiB, *port, cfg)
	case "client":
		if *serverHost == "" {
			log.Fatal("client 模式需要 -server 参数")
		}
		runClient(*serverHost, *port, *outDir, cfg)
	default:
		log.Fatal("需要 -mode server 或 -mode client")
	}
}

// ---------------- 服务端 ----------------

func runServer(rootPath string, minSizeMiB int64, port int, cfg KCPConfig) {
	minSize := minSizeMiB * 1024 * 1024
	log.Printf("扫描目录: %s, 最小大小: %d MiB", rootPath, minSizeMiB)

	files, err := scanFiles(rootPath, minSize)
	if err != nil {
		log.Fatal("扫描失败: ", err)
	}
	if len(files) == 0 {
		log.Fatal("未找到符合条件的文件")
	}

	log.Printf("找到 %d 个文件:", len(files))
	for i, f := range files {
		log.Printf("  [%d] %s (%.2f MiB)", i+1, f.RelPath, float64(f.Size)/1024/1024)
	}

	listenAddr := fmt.Sprintf(":%d", port)
	lis, err := kcp.ListenWithOptions(listenAddr, nil, cfg.FECData, cfg.FECParity)
	if err != nil {
		log.Fatal("KCP 监听失败: ", err)
	}
	defer lis.Close()
	log.Printf("KCP 服务端监听 %s (FEC %d/%d, window=%d, mtu=%d)",
		listenAddr, cfg.FECData, cfg.FECParity, cfg.Window, cfg.MTU)

	// for {
	// 服务端只接受一次请求, 传输结束直接退出
		conn, err := lis.AcceptKCP()
		if err != nil {
			log.Printf("Accept 错误: %v", err)
			// continue
		}
		log.Printf("客户端连接: %s", conn.RemoteAddr())
		handleClient(conn, files, cfg)
		log.Printf("客户端断开: %s", conn.RemoteAddr())
	// }
}

func scanFiles(root string, minSize int64) ([]FileEntry, error) {
	var files []FileEntry
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		if info.Size() < minSize {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, FileEntry{
			RelPath: filepath.ToSlash(rel),
			AbsPath: path,
			Size:    info.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].RelPath < files[j].RelPath
	})
	return files, nil
}

func handleClient(conn *kcp.UDPSession, files []FileEntry, cfg KCPConfig) {
	cfg.apply(conn)

	// 1) 读握手字节 (触发 AcceptKCP 返回后必须读掉)
	hs := make([]byte, 1)
	if _, err := io.ReadFull(conn, hs); err != nil {
		log.Printf("握手失败: %v", err)
		conn.Close()
		return
	}
	log.Printf("握手完成, 发送文件列表 (%d 个)", len(files))

	// 2) 发送文件列表
	if err := sendFileList(conn, files); err != nil {
		log.Printf("发送列表失败: %v", err)
		conn.Close()
		return
	}

	// 3) 读取客户端请求 (请求头 + 序号/offset), 整个请求读取阶段设置超时
	conn.SetReadDeadline(time.Now().Add(serverRequestTimeout))

	reqHdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, reqHdr); err != nil {
		log.Printf("读取请求头失败 (或超时): %v", err)
		conn.Close()
		return
	}
	count := binary.BigEndian.Uint32(reqHdr)

	// count == 0 表示客户端主动取消或没有任何文件需要下载
	if count == 0 {
		log.Printf("客户端取消或无文件需要下载, 关闭连接")
		conn.SetReadDeadline(time.Time{})
		conn.Close()
		return
	}
	if count > maxFileCount {
		log.Printf("非法请求数量: %d", count)
		conn.Close()
		return
	}

	reqs := make([]fileReq, count)
	reqBuf := make([]byte, 12) // [seq:4][offset:8]
	for i := range reqs {
		if _, err := io.ReadFull(conn, reqBuf); err != nil {
			log.Printf("读取请求条目失败: %v", err)
			conn.Close()
			return
		}
		reqs[i].Seq = binary.BigEndian.Uint32(reqBuf[0:4])
		reqs[i].Offset = int64(binary.BigEndian.Uint64(reqBuf[4:12]))
	}

	// 请求已读完, 取消读超时
	conn.SetReadDeadline(time.Time{})

	// 校验
	for _, r := range reqs {
		if int(r.Seq) >= len(files) {
			log.Printf("序号 %d 越界, 关闭连接", r.Seq)
			conn.Close()
			return
		}
		f := files[r.Seq]
		if r.Offset < 0 || r.Offset > f.Size {
			log.Printf("序号 %d (%s) 的 offset %d 非法 (size=%d), 关闭连接",
				r.Seq, f.RelPath, r.Offset, f.Size)
			conn.Close()
			return
		}
	}

	log.Printf("客户端请求 %d 个文件", count)

	// 4) 逐个发送 (从各自 offset 开始)
	startAll := time.Now()
	var totalSent int64
	for _, r := range reqs {
		f := files[r.Seq]
		n, err := sendFileFrom(conn, f, r.Offset)
		if err != nil {
			log.Printf("发送 %s 失败: %v", f.RelPath, err)
			conn.Close()
			return
		}
		totalSent += n
	}
	elapsed := time.Since(startAll)
	log.Printf("会话完成: %d 个文件, 本次共 %d 字节, 用时 %v, 平均 %.2f MiB/s",
		len(reqs), totalSent, elapsed,
		float64(totalSent)/elapsed.Seconds()/1024/1024)

	// 5) 阻塞等待客户端确认
	log.Printf("等待客户端确认...")
	ack := make([]byte, 1)
	conn.SetReadDeadline(time.Now().Add(serverAckTimeout))
	if _, err := io.ReadFull(conn, ack); err != nil {
		log.Printf("等待客户端确认失败: %v", err)
	} else if ack[0] == ackDone {
		log.Printf("收到客户端完成确认, 数据已全部交付")
	} else {
		log.Printf("收到未知确认字节: %d", ack[0])
	}
	conn.SetReadDeadline(time.Time{})

	conn.Close()
}

func sendFileList(conn io.Writer, files []FileEntry) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(files)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	for _, f := range files {
		nameBytes := []byte(f.RelPath)
		if len(nameBytes) > maxNameLen {
			return fmt.Errorf("文件名过长: %s", f.RelPath)
		}
		entry := make([]byte, 2+len(nameBytes)+8)
		binary.BigEndian.PutUint16(entry[0:2], uint16(len(nameBytes)))
		copy(entry[2:2+len(nameBytes)], nameBytes)
		binary.BigEndian.PutUint64(entry[2+len(nameBytes):], uint64(f.Size))
		if _, err := conn.Write(entry); err != nil {
			return err
		}
	}
	return nil
}

// sendFileFrom 从 offset 开始发送文件.
// 文件头始终携带文件总大小, 便于客户端校验与进度显示.
func sendFileFrom(conn io.Writer, f FileEntry, offset int64) (int64, error) {
	file, err := os.Open(f.AbsPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if offset > 0 {
		if _, err := file.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
	}

	// 文件头: [nameLen:2][name:N][size:8]  (size 为总大小)
	nameBytes := []byte(f.RelPath)
	hdr := make([]byte, 2+len(nameBytes)+8)
	binary.BigEndian.PutUint16(hdr[0:2], uint16(len(nameBytes)))
	copy(hdr[2:2+len(nameBytes)], nameBytes)
	binary.BigEndian.PutUint64(hdr[2+len(nameBytes):], uint64(f.Size))
	if _, err := conn.Write(hdr); err != nil {
		return 0, err
	}

	remaining := f.Size - offset
	start := time.Now()
	var sent int64
	buf := make([]byte, 64*1024)
	lastLog := start
	for sent < remaining {
		want := int64(len(buf))
		if remaining-sent < want {
			want = remaining - sent
		}
		n, err := file.Read(buf[:want])
		if n > 0 {
			if _, werr := conn.Write(buf[:n]); werr != nil {
				return sent, werr
			}
			sent += int64(n)
			if time.Since(lastLog) >= 2*time.Second {
				elapsed := time.Since(start).Seconds()
				rate := float64(sent) / elapsed / 1024 / 1024
				log.Printf("  ↑ %s: %.1f%% (本次 %.2f MiB/s)",
					f.RelPath,
					float64(offset+sent)*100/float64(f.Size),
					rate)
				lastLog = time.Now()
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return sent, err
		}
	}

	elapsed := time.Since(start)
	log.Printf("  ↑ %s 完成: 本次发送 %d 字节 (offset=%d, 累计 %d/%d), %.2fs, 平均 %.2f MiB/s",
		f.RelPath, sent, offset, offset+sent, f.Size, elapsed.Seconds(),
		float64(sent)/elapsed.Seconds()/1024/1024)
	return sent, nil
}

// ---------------- 客户端 ----------------

func runClient(host string, port int, outDir string, cfg KCPConfig) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := kcp.DialWithOptions(addr, nil, cfg.FECData, cfg.FECParity)
	if err != nil {
		log.Fatal("连接失败: ", err)
	}
	defer conn.Close()
	cfg.apply(conn)

	// 发送握手字节, 触发服务端 AcceptKCP 返回
	if _, err := conn.Write([]byte{'H'}); err != nil {
		log.Fatal("握手失败: ", err)
	}
	log.Printf("已连接 %s (FEC %d/%d, window=%d, mtu=%d)",
		addr, cfg.FECData, cfg.FECParity, cfg.Window, cfg.MTU)

	// 接收文件列表
	files, err := recvFileList(conn)
	if err != nil {
		log.Fatal("接收列表失败: ", err)
	}
	if len(files) == 0 {
		log.Fatal("服务端没有可下载的文件")
	}

	fmt.Println()
	fmt.Printf("服务端文件列表 (共 %d 个):\n", len(files))
	for i, f := range files {
		fmt.Printf("  [%d] %-60s %10.2f MiB\n",
			i+1, f.RelPath, float64(f.Size)/1024/1024)
	}

	// 用户输入
	fmt.Print("\n输入序号 (空格分隔, q 退出): ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)

	if line == "" || line == "q" || line == "quit" || line == "exit" {
		// 发送 count=0 空请求, 通知服务端本次会话取消
		sendCancel(conn)
		log.Println("已退出")
		return
	}

	// 解析序号
	parts := strings.Fields(line)
	var seqs []uint32
	seen := make(map[int]bool)
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			log.Fatalf("非法序号: %s", p)
		}
		if n < 1 || n > len(files) {
			log.Fatalf("序号越界: %d (有效范围 1-%d)", n, len(files))
		}
		if seen[n] {
			continue
		}
		seen[n] = true
		seqs = append(seqs, uint32(n-1))
	}
	if len(seqs) == 0 {
		log.Fatal("未选择任何文件")
	}

	// 本地预检: 计算每个选中文件的 localPath / offset, 跳过已完成或异常的
	fmt.Println()
	var pendingFiles []pending
	var doneCount, warnCount int
	for _, s := range seqs {
		f := files[s]
		baseName := filepath.Base(f.RelPath)
		localPath := filepath.Join(outDir, baseName)

		var localSize int64
		if fi, err := os.Stat(localPath); err == nil && fi.Mode().IsRegular() {
			localSize = fi.Size()
		}

		switch {
		case localSize == f.Size:
			fmt.Printf("  - 跳过 %s (本地已完成, %d 字节)\n", f.RelPath, localSize)
			doneCount++
			continue
		case localSize > f.Size:
			fmt.Printf("  ! 警告: 本地 %s 更大 (%d > %d), 跳过下载, 请手动处理\n",
				localPath, localSize, f.Size)
			warnCount++
			continue
		default:
			pendingFiles = append(pendingFiles, pending{
				Seq:       s,
				Offset:    localSize,
				LocalPath: localPath,
				Size:      f.Size,
				RelPath:   f.RelPath,
			})
		}
	}

	if len(pendingFiles) == 0 {
		// 所有选中文件都已完成或被跳过, 通知服务端取消
		sendCancel(conn)
		fmt.Printf("\n无需下载 (已完成 %d, 跳过 %d)\n", doneCount, warnCount)
		return
	}

	// 发送请求: [count:4] + [seq:4][offset:8]*count
	req := make([]byte, 4+12*len(pendingFiles))
	binary.BigEndian.PutUint32(req[0:4], uint32(len(pendingFiles)))
	for i, p := range pendingFiles {
		binary.BigEndian.PutUint32(req[4+12*i:], p.Seq)
		binary.BigEndian.PutUint64(req[8+12*i:], uint64(p.Offset))
	}
	if _, err := conn.Write(req); err != nil {
		log.Fatal("发送请求失败: ", err)
	}

	fmt.Printf("\n开始下载 %d 个文件 (已完成 %d, 跳过 %d)...\n\n",
		len(pendingFiles), doneCount, warnCount)

	startAll := time.Now()
	var totalBytes int64
	for _, p := range pendingFiles {
		recv, err := recvFileAt(conn, p)
		if err != nil {
			fmt.Println()
			log.Fatalf("接收文件失败: %v", err)
		}
		fmt.Printf("  ↓ 已保存 %s (本次 %d 字节, 本地累计 %d/%d, %.2f MiB)\n",
			p.LocalPath, recv, p.Offset+recv, p.Size, float64(p.Size)/1024/1024)
		totalBytes += recv
	}

	// 所有文件已收完并写盘, 通知服务端可以安全关闭
	writeAckDone(conn)

	elapsed := time.Since(startAll)
	fmt.Printf("\n全部完成: 本次下载 %d 个文件, %d 字节 (%.2f MiB), 用时 %v, 平均 %.2f MiB/s\n",
		len(pendingFiles), totalBytes, float64(totalBytes)/1024/1024,
		elapsed, float64(totalBytes)/elapsed.Seconds()/1024/1024)
}

func recvFileList(conn io.Reader) ([]FileEntry, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	count := binary.BigEndian.Uint32(hdr)
	if count > maxFileCount {
		return nil, fmt.Errorf("文件列表过大: %d", count)
	}
	files := make([]FileEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		nhdr := make([]byte, 2)
		if _, err := io.ReadFull(conn, nhdr); err != nil {
			return nil, err
		}
		nameLen := binary.BigEndian.Uint16(nhdr)
		nameBuf := make([]byte, nameLen)
		if _, err := io.ReadFull(conn, nameBuf); err != nil {
			return nil, err
		}
		sizeBuf := make([]byte, 8)
		if _, err := io.ReadFull(conn, sizeBuf); err != nil {
			return nil, err
		}
		size := binary.BigEndian.Uint64(sizeBuf)
		if size > 1<<62 {
			return nil, fmt.Errorf("文件过大: %d", size)
		}
		files = append(files, FileEntry{
			RelPath: string(nameBuf),
			Size:    int64(size),
		})
	}
	return files, nil
}

// recvFileAt 接收单个文件, 从 p.Offset 处开始追加写入 p.LocalPath.
// 文件头里服务端携带的 size 必须等于 p.Size, 否则报错终止.
func recvFileAt(conn *kcp.UDPSession, p pending) (int64, error) {
	// 文件头: [nameLen:2][name:N][size:8]
	nhdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, nhdr); err != nil {
		return 0, err
	}
	nameLen := binary.BigEndian.Uint16(nhdr)
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(conn, nameBuf); err != nil {
		return 0, err
	}
	sizeBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, sizeBuf); err != nil {
		return 0, err
	}
	remoteName := string(nameBuf)
	size := int64(binary.BigEndian.Uint64(sizeBuf))
	if size < 0 {
		return 0, fmt.Errorf("非法文件大小: %d", size)
	}
	if size != p.Size {
		return 0, fmt.Errorf("文件 %s 大小不一致: 服务端 %d, 预期 %d",
			remoteName, size, p.Size)
	}

	out, err := os.OpenFile(p.LocalPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	if p.Offset > 0 {
		if _, err := out.Seek(p.Offset, io.SeekStart); err != nil {
			return 0, err
		}
	}

	remaining := p.Size - p.Offset
	start := time.Now()
	var received int64
	buf := make([]byte, 64*1024)
	lastUpdate := time.Now()

	for received < remaining {
		want := int64(len(buf))
		if remaining-received < want {
			want = remaining - received
		}
		// 单次读超时, 服务端异常时客户端不会永久阻塞
		conn.SetReadDeadline(time.Now().Add(clientReadTimeout))
		n, err := io.ReadFull(conn, buf[:want])
		if n > 0 {
			if _, werr := out.Write(buf[:n]); werr != nil {
				return received, werr
			}
			received += int64(n)
			if time.Since(lastUpdate) >= 1000*time.Millisecond {
				printProgress(remoteName, p.Offset+received, p.Size, start)
				lastUpdate = time.Now()
			}
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if received < remaining {
					return received, fmt.Errorf("连接提前关闭: 收到 %d/%d 字节",
						received, remaining)
				}
				break
			}
			return received, err
		}
	}
	conn.SetReadDeadline(time.Time{})
	printProgress(remoteName, p.Offset+received, p.Size, start)
	fmt.Println()
	return received, nil
}

func writeAckDone(conn *kcp.UDPSession) {
	if _, err := conn.Write([]byte{ackDone}); err != nil {
		log.Printf("发送完成确认失败: %v", err)
	}
	// 给 ackDone 一点时间通过 KCP 送达服务端, 然后退出
	time.Sleep(1 * time.Second)
}

// sendCancel 发送 count=0 空请求, 通知服务端本次会话取消.
func sendCancel(conn *kcp.UDPSession) {
	req := make([]byte, 4)
	binary.BigEndian.PutUint32(req[0:4], 0)
	if _, err := conn.Write(req); err != nil {
		log.Printf("发送取消请求失败: %v", err)
	}
	// 给取消请求一点时间通过 KCP 送达服务端
	time.Sleep(500 * time.Millisecond)
}

func printProgress(name string, done, total int64, start time.Time) {
	const barWidth = 30
	frac := float64(done) / float64(total)
	if frac > 1 {
		frac = 1
	}
	if frac < 0 {
		frac = 0
	}

	finished := done >= total
	pct := frac * 100
	if finished {
		pct = 100.0
	}

	filled := int(frac * barWidth)
	if finished {
		filled = barWidth
	}
	bar := strings.Repeat("=", filled)
	if filled < barWidth {
		bar += ">" + strings.Repeat(" ", barWidth-filled-1)
	}

	elapsed := time.Since(start).Seconds()
	var rate float64
	if elapsed > 0 {
		rate = float64(done) / elapsed / 1024 / 1024
	}

	var eta string
	if finished {
		eta = "00:00"
	} else if rate > 0 {
		rem := float64(total-done) / (rate * 1024 * 1024)
		eta = formatTime(rem)
	} else {
		eta = "--:--"
	}

	displayName := name
	if len(displayName) > 40 {
		displayName = "..." + displayName[len(displayName)-37:]
	}

	fmt.Printf("\r%-40s [%s] %5.1f%%  %6.2f MiB/s ELP %s ETA %s",
		displayName, bar, pct, rate, formatTime(elapsed), eta)
}

func formatTime(sec float64) string {
	m := int(sec) / 60
	s := int(sec) % 60
	return fmt.Sprintf("%02d:%02d", m, s)
}